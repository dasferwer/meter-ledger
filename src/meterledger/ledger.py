import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from fastapi import HTTPException

from .db import engine, execute, one


def decimal_text(value: Decimal) -> str:
    rendered = format(value, "f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def request_hash(data, kind):
    body = data.model_dump(mode="json")
    if "quantity" in body:
        body["quantity"] = decimal_text(data.quantity)
    return digest({"kind": kind, **body})


async def lock_tenant(conn, tenant_id):
    row = await one(conn, "SELECT * FROM tenants WHERE id=:id FOR UPDATE", id=tenant_id)
    if row is None:
        raise HTTPException(404, "Tenant not found")
    return row


async def accept_event(tenant_id, data, correction=False):
    hashed = request_hash(data, "correction" if correction else "usage")
    async with engine.begin() as conn:
        tenant = await lock_tenant(conn, tenant_id)
        existing = await one(
            conn,
            "SELECT * FROM events WHERE tenant_id=:tenant AND external_id=:key",
            tenant=tenant_id,
            key=data.event_id,
        )
        if existing:
            if existing["body_hash"] != hashed:
                raise HTTPException(409, "Event ID was already used with a different payload")
            return dict(existing), False
        if correction:
            original = await one(
                conn,
                "SELECT * FROM events WHERE tenant_id=:tenant AND id=:id AND original_id IS NULL",
                tenant=tenant_id,
                id=data.original_id,
            )
            if original is None:
                raise HTTPException(404, "Original usage event not found")
            if (
                original["metric"] == "api_calls"
                and data.quantity != data.quantity.to_integral_value()
            ):
                raise HTTPException(422, "API call correction must be an integer")
            corrected = await one(
                conn,
                "SELECT COALESCE(sum(quantity),0) AS quantity FROM events WHERE tenant_id=:tenant AND original_id=:id",
                tenant=tenant_id,
                id=data.original_id,
            )
            if original["quantity"] + corrected["quantity"] < data.quantity:
                raise HTTPException(409, "Correction exceeds the remaining original quantity")
            metric, occurred_at = original["metric"], original["occurred_at"]
            rate_id, price = original["rate_id"], original["unit_price"]
            quantity, original_id, reason = -data.quantity, data.original_id, data.reason
        else:
            rate = await one(
                conn,
                "SELECT * FROM rates WHERE metric=:metric AND effective_at<=:at ORDER BY effective_at DESC LIMIT 1",
                metric=data.metric,
                at=data.occurred_at,
            )
            if rate is None:
                raise HTTPException(422, "No rate applies to this event time")
            metric, occurred_at = data.metric, data.occurred_at
            rate_id, price = rate["id"], rate["unit_price"]
            quantity, original_id, reason = data.quantity, None, None
        sequence = tenant["sequence"] + 1
        # Счёт берёт границу под той же блокировкой. Запись с меньшим номером не сможет появиться после расчёта.
        await execute(
            conn,
            "UPDATE tenants SET sequence=:seq WHERE id=:tenant",
            seq=sequence,
            tenant=tenant_id,
        )
        row = await one(
            conn,
            """
            INSERT INTO events(id,tenant_id,sequence,external_id,body_hash,metric,quantity,occurred_at,rate_id,unit_price,original_id,reason)
            VALUES(:id,:tenant,:seq,:key,:hash,:metric,:quantity,:at,:rate,:price,:original,:reason) RETURNING *
        """,
            id=uuid4(),
            tenant=tenant_id,
            seq=sequence,
            key=data.event_id,
            hash=hashed,
            metric=metric,
            quantity=quantity,
            at=occurred_at,
            rate=rate_id,
            price=price,
            original=original_id,
            reason=reason,
        )
        return dict(row), True


def period_bounds(period):
    try:
        start = datetime.strptime(period, "%Y-%m").replace(tzinfo=UTC)
    except ValueError:
        raise HTTPException(422, "Period must use YYYY-MM") from None
    if start.year < 2020 or start.year > 2099 or start.strftime("%Y-%m") != period:
        raise HTTPException(422, "Period must be between 2020-01 and 2099-12")
    end = datetime(start.year + (start.month == 12), start.month % 12 + 1, 1, tzinfo=UTC)
    return start, end


async def enqueue_invoice(tenant_id, period, key):
    _, end = period_bounds(period)
    if end > datetime.now(UTC):
        raise HTTPException(409, "Only a closed UTC calendar month can be invoiced")
    async with engine.begin() as conn:
        await lock_tenant(conn, tenant_id)
        existing = await one(
            conn,
            "SELECT * FROM jobs WHERE tenant_id=:tenant AND request_key=:key",
            tenant=tenant_id,
            key=key,
        )
        if existing:
            if existing["period"] != period:
                raise HTTPException(409, "Idempotency key belongs to another period")
            return dict(existing), False
        pending = await one(
            conn,
            "SELECT count(*) AS count FROM jobs WHERE tenant_id=:tenant AND status='pending'",
            tenant=tenant_id,
        )
        if pending["count"] >= 20:
            raise HTTPException(429, "Too many pending invoice requests")
        job = await one(
            conn,
            "INSERT INTO jobs(id,tenant_id,request_key,period) VALUES(:id,:tenant,:key,:period) RETURNING *",
            id=uuid4(),
            tenant=tenant_id,
            key=key,
            period=period,
        )
        await execute(
            conn, "INSERT INTO outbox(id,job_id) VALUES(:id,:job)", id=uuid4(), job=job["id"]
        )
        return dict(job), True
