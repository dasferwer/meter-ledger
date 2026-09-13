import asyncio
import contextlib
import logging
import sys
from uuid import UUID

import aio_pika

from .billing import process_job
from .config import settings
from .db import engine, execute, one

log = logging.getLogger(__name__)
QUEUE = "meterledger.invoice"
QUARANTINE = "meterledger.quarantine"


async def heartbeat(name):
    while True:
        try:
            async with engine.begin() as conn:
                await execute(
                    conn,
                    "INSERT INTO heartbeats(name) VALUES(:name) ON CONFLICT(name) DO UPDATE SET seen_at=now()",
                    name=name,
                )
        except Exception:
            log.exception("Could not update heartbeat")
        await asyncio.sleep(3)


async def dispatch_once(channel):
    async with engine.begin() as conn:
        row = await one(
            conn,
            """
            SELECT o.* FROM outbox o JOIN jobs j ON j.id=o.job_id
            WHERE (o.published_at IS NULL OR j.status='pending') AND o.next_attempt_at<=now()
            ORDER BY o.next_attempt_at,o.id FOR UPDATE OF o SKIP LOCKED LIMIT 1
        """,
        )
        if row is None:
            return False
        await channel.default_exchange.publish(
            aio_pika.Message(
                str(row["job_id"]).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=str(row["id"]),
            ),
            routing_key=QUEUE,
            mandatory=True,
            timeout=5,
        )
        if settings.dispatcher_after_publish_delay:
            await asyncio.sleep(settings.dispatcher_after_publish_delay)
        # После confirm процесс может упасть. Повторная публикация безопасна: завершённая задача больше не выпускает счёт.
        await execute(
            conn,
            "UPDATE outbox SET published_at=now(),publications=publications+1,next_attempt_at=now()+interval '5 seconds' WHERE id=:id",
            id=row["id"],
        )
        return True


async def fail_job(job_id):
    async with engine.begin() as conn:
        await execute(
            conn,
            """UPDATE jobs SET attempts=attempts+1,
            status=CASE WHEN attempts>=4 THEN 'failed' ELSE 'pending' END,
            error='Calculation failed; inspect worker logs',
            finished_at=CASE WHEN attempts>=4 THEN now() ELSE NULL END
            WHERE id=:id AND status='pending'""",
            id=job_id,
        )


async def handle(message, channel):
    async with message.process(requeue=True):
        try:
            job_id = UUID(message.body.decode())
        except (ValueError, UnicodeError):
            await channel.default_exchange.publish(
                aio_pika.Message(message.body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key=QUARANTINE,
                mandatory=True,
                timeout=5,
            )
            return
        try:
            await process_job(job_id)
        except Exception:
            log.exception("Invoice calculation failed for job %s", job_id)
            await fail_job(job_id)
        # ACK уходит после commit. При потере ACK брокер вернёт тот же job_id.


async def run(role):
    pulse = asyncio.create_task(heartbeat(role))
    try:
        while True:
            try:
                connection = await aio_pika.connect_robust(settings.rabbitmq_url, timeout=5)
                async with connection:
                    channel = await connection.channel(
                        publisher_confirms=True, on_return_raises=True
                    )
                    queue = await channel.declare_queue(QUEUE, durable=True)
                    await channel.declare_queue(QUARANTINE, durable=True)
                    await channel.set_qos(prefetch_count=1)
                    if role == "dispatcher":
                        while True:
                            if not await dispatch_once(channel):
                                await asyncio.sleep(0.5)
                    else:
                        async with queue.iterator() as messages:
                            async for message in messages:
                                await handle(message, channel)
            except Exception:
                log.exception("Messaging connection interrupted; reconnecting")
                await asyncio.sleep(2)
    finally:
        pulse.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pulse
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    role = sys.argv[1]
    if role not in {"worker", "dispatcher"}:
        raise SystemExit("Choose worker or dispatcher")
    asyncio.run(run(role))
