import asyncio
from uuid import uuid4

from fastapi import HTTPException

from .config import settings
from .db import engine, execute, one
from .ledger import canonical, digest, lock_tenant
from .rating import ENGINE_VERSION, calculate


async def process_job(job_id):
    async with engine.begin() as conn:
        initial = await one(conn, "SELECT tenant_id FROM jobs WHERE id=:id", id=job_id)
        if initial is None:
            return
        tenant = await lock_tenant(conn, initial["tenant_id"])
        job = await one(conn, "SELECT * FROM jobs WHERE id=:id FOR UPDATE", id=job_id)
        if job["status"] != "pending":
            return
        last = await one(
            conn,
            "SELECT * FROM invoices WHERE tenant_id=:tenant AND period=:period ORDER BY revision DESC LIMIT 1",
            tenant=tenant["id"],
            period=job["period"],
        )
        calculated = await calculate(
            conn,
            tenant["id"],
            job["period"],
            tenant["sequence"],
            last["calculation"] if last else None,
        )
        if last and last["calculation"]["source_hash"] == calculated["source_hash"]:
            invoice_id = last["id"]
        else:
            invoice_id = uuid4()
            await execute(
                conn,
                """
                INSERT INTO invoices(id,tenant_id,period,revision,cutoff,previous_id,calculation,document_hash)
                VALUES(:id,:tenant,:period,:revision,:cutoff,:previous,CAST(:calculation AS jsonb),:hash)
            """,
                id=invoice_id,
                tenant=tenant["id"],
                period=job["period"],
                revision=last["revision"] + 1 if last else 1,
                cutoff=tenant["sequence"],
                previous=last["id"] if last else None,
                calculation=canonical(calculated),
                hash=digest(calculated),
            )
        await execute(
            conn,
            "UPDATE jobs SET status='done',invoice_id=:invoice,finished_at=now(),error=NULL WHERE id=:id",
            invoice=invoice_id,
            id=job_id,
        )
        if settings.worker_before_commit_delay:
            await asyncio.sleep(settings.worker_before_commit_delay)


async def verify_invoice(tenant_id, invoice_id):
    async with engine.connect() as conn:
        invoice = await one(
            conn,
            "SELECT * FROM invoices WHERE tenant_id=:tenant AND id=:id",
            tenant=tenant_id,
            id=invoice_id,
        )
        if invoice is None:
            raise HTTPException(404, "Invoice not found")
        if invoice["calculation"]["engine_version"] != ENGINE_VERSION:
            raise HTTPException(409, "Historical calculation engine is not available")
        previous = None
        if invoice["previous_id"]:
            previous = await one(
                conn,
                "SELECT calculation FROM invoices WHERE id=:id AND tenant_id=:tenant",
                id=invoice["previous_id"],
                tenant=tenant_id,
            )
        calculated = await calculate(
            conn,
            tenant_id,
            invoice["period"],
            invoice["cutoff"],
            previous["calculation"] if previous else None,
        )
        return {
            "invoice_id": invoice_id,
            "matches": calculated == invoice["calculation"]
            and digest(calculated) == invoice["document_hash"],
            "document_hash": digest(calculated),
            "event_count": calculated["event_count"],
            "cumulative_cents": calculated["cumulative_cents"],
            "delta_cents": calculated["delta_cents"],
        }
