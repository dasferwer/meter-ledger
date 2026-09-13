import secrets
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from .auth import admin_key, key_hash, tenant_key
from .billing import verify_invoice
from .db import engine, execute, one
from .ledger import accept_event, decimal_text, enqueue_invoice, period_bounds
from .rating import calculate
from .schemas import CorrectionInput, IssueInput, RateInput, TenantInput, UsageInput

REQUESTS = Counter(
    "meterledger_http_requests_total", "HTTP requests", ["method", "route", "status"]
)
LATENCY = Histogram("meterledger_http_seconds", "HTTP request duration", ["route"])


def view(row):
    return {
        key: decimal_text(value) if isinstance(value, Decimal) else value
        for key, value in dict(row).items()
    }


@asynccontextmanager
async def lifespan(app):
    yield
    await engine.dispose()


app = FastAPI(
    title="MeterLedger",
    version="0.1.0",
    lifespan=lifespan,
    description="Usage billing: immutable journal, versioned rates, monthly statements and late-event adjustments. All money is simulated USD; no payment provider is connected.",
)


@app.middleware("http")
async def metrics(request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    route = getattr(request.scope.get("route"), "path", "unmatched")
    REQUESTS.labels(request.method, route, response.status_code).inc()
    LATENCY.labels(route).observe(time.monotonic() - start)
    return response


@app.get("/health", tags=["Operations"])
async def health():
    async with engine.connect() as conn:
        await execute(conn, "SELECT 1")
    return {"status": "ok"}


@app.get("/metrics", tags=["Operations"])
async def metrics_endpoint():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post(
    "/admin/tenants", dependencies=[Depends(admin_key)], status_code=201, tags=["Administration"]
)
async def create_tenant(data: TenantInput):
    api_key = "ml_" + secrets.token_urlsafe(32)
    async with engine.begin() as conn:
        tenant = await one(
            conn,
            "INSERT INTO tenants(id,name,key_hash) VALUES(:id,:name,:hash) RETURNING id,name",
            id=uuid4(),
            name=data.name,
            hash=key_hash(api_key),
        )
    return {**tenant, "api_key": api_key}


@app.get("/me", tags=["Account"])
async def me(tenant: UUID = Depends(tenant_key)):
    async with engine.connect() as conn:
        return await one(
            conn, "SELECT id,name,sequence,created_at FROM tenants WHERE id=:id", id=tenant
        )


@app.get("/rates", tags=["Rates"])
async def rates(tenant: UUID = Depends(tenant_key)):
    async with engine.connect() as conn:
        rows = (await execute(conn, "SELECT * FROM rates ORDER BY metric,effective_at")).mappings()
        return [view(row) for row in rows]


@app.post(
    "/admin/rates", dependencies=[Depends(admin_key)], status_code=201, tags=["Administration"]
)
async def add_rate(data: RateInput):
    async with engine.begin() as conn:
        await execute(
            conn, "SELECT pg_advisory_xact_lock(hashtextextended(:metric,0))", metric=data.metric
        )
        now = datetime.now(UTC)
        if not now + timedelta(minutes=1) <= data.effective_at <= now + timedelta(days=366):
            raise HTTPException(
                422, "Schedule a rate at least one minute and at most one year ahead"
            )
        count = await one(
            conn, "SELECT count(*) AS count FROM rates WHERE metric=:metric", metric=data.metric
        )
        if count["count"] >= 1000:
            raise HTTPException(409, "Rate-version limit reached")
        row = await one(
            conn,
            """INSERT INTO rates(id,metric,effective_at,unit_price)
            VALUES(:id,:metric,:at,:price) ON CONFLICT(metric,effective_at) DO NOTHING RETURNING *""",
            id=uuid4(),
            metric=data.metric,
            at=data.effective_at,
            price=data.unit_price,
        )
        if row is None:
            raise HTTPException(409, "A rate already starts at this time")
        return view(row)


@app.post("/events", status_code=201, tags=["Usage"])
async def usage(data: UsageInput, response: Response, tenant: UUID = Depends(tenant_key)):
    event, created = await accept_event(tenant, data)
    response.status_code = 201 if created else 200
    return view(event)


@app.post("/corrections", status_code=201, tags=["Usage"])
async def correction(data: CorrectionInput, response: Response, tenant: UUID = Depends(tenant_key)):
    event, created = await accept_event(tenant, data, correction=True)
    response.status_code = 201 if created else 200
    return view(event)


@app.get("/events", tags=["Usage"])
async def events(
    tenant: UUID = Depends(tenant_key),
    after: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
):
    async with engine.connect() as conn:
        rows = list(
            (
                await execute(
                    conn,
                    "SELECT * FROM events WHERE tenant_id=:tenant AND sequence>:after ORDER BY sequence LIMIT :limit",
                    tenant=tenant,
                    after=after,
                    limit=limit,
                )
            ).mappings()
        )
        return {
            "items": [view(row) for row in rows],
            "next_after": rows[-1]["sequence"] if rows else after,
        }


@app.get("/billing/{period}/preview", tags=["Billing"])
async def preview(period: str, tenant: UUID = Depends(tenant_key)):
    period_bounds(period)
    async with engine.connect() as conn:
        account = await one(conn, "SELECT sequence FROM tenants WHERE id=:id", id=tenant)
        return await calculate(conn, tenant, period, account["sequence"])


@app.post("/billing/issue", status_code=202, tags=["Billing"])
async def issue(
    data: IssueInput,
    tenant: UUID = Depends(tenant_key),
    idempotency_key: str = Header(min_length=1, max_length=128),
):
    job, _ = await enqueue_invoice(tenant, data.period, idempotency_key)
    return job


@app.get("/jobs/{job_id}", tags=["Billing"])
async def get_job(job_id: UUID, tenant: UUID = Depends(tenant_key)):
    async with engine.connect() as conn:
        job = await one(
            conn, "SELECT * FROM jobs WHERE id=:id AND tenant_id=:tenant", id=job_id, tenant=tenant
        )
        if job is None:
            raise HTTPException(404, "Job not found")
        return job


@app.get("/invoices", tags=["Billing"])
async def invoices(
    tenant: UUID = Depends(tenant_key),
    before_id: UUID | None = None,
    limit: int = Query(50, ge=1, le=100),
):
    async with engine.connect() as conn:
        cursor = None
        if before_id:
            cursor = await one(
                conn,
                "SELECT id,created_at FROM invoices WHERE id=:id AND tenant_id=:tenant",
                id=before_id,
                tenant=tenant,
            )
            if cursor is None:
                raise HTTPException(404, "Invoice cursor not found")
        condition = "AND (created_at,id)<(:created,:id)" if cursor else ""
        rows = list(
            (
                await execute(
                    conn,
                    "SELECT id,period,revision,created_at,document_hash FROM invoices WHERE tenant_id=:tenant "
                    + condition
                    + " ORDER BY created_at DESC,id DESC LIMIT :limit",
                    tenant=tenant,
                    limit=limit,
                    created=cursor["created_at"] if cursor else None,
                    id=before_id,
                )
            ).mappings()
        )
        return {"items": rows, "next_before_id": rows[-1]["id"] if rows else None}


@app.get("/invoices/{invoice_id}", tags=["Billing"])
async def get_invoice(invoice_id: UUID, tenant: UUID = Depends(tenant_key)):
    async with engine.connect() as conn:
        row = await one(
            conn,
            "SELECT * FROM invoices WHERE id=:id AND tenant_id=:tenant",
            id=invoice_id,
            tenant=tenant,
        )
        if row is None:
            raise HTTPException(404, "Invoice not found")
        return row


@app.get("/invoices/{invoice_id}/verify", tags=["Billing"])
async def verify(invoice_id: UUID, tenant: UUID = Depends(tenant_key)):
    return await verify_invoice(tenant, invoice_id)


@app.get("/invoices/{invoice_id}/events", tags=["Billing"])
async def invoice_events(
    invoice_id: UUID,
    tenant: UUID = Depends(tenant_key),
    after: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
):
    async with engine.connect() as conn:
        invoice = await one(
            conn,
            "SELECT period,cutoff FROM invoices WHERE id=:id AND tenant_id=:tenant",
            id=invoice_id,
            tenant=tenant,
        )
        if invoice is None:
            raise HTTPException(404, "Invoice not found")
        start, end = period_bounds(invoice["period"])
        rows = list(
            (
                await execute(
                    conn,
                    """SELECT * FROM events WHERE tenant_id=:tenant AND sequence>:after AND sequence<=:cutoff
            AND occurred_at>=:start AND occurred_at<:end ORDER BY sequence LIMIT :limit""",
                    tenant=tenant,
                    after=after,
                    cutoff=invoice["cutoff"],
                    start=start,
                    end=end,
                    limit=limit,
                )
            ).mappings()
        )
        return {
            "items": [view(row) for row in rows],
            "next_after": rows[-1]["sequence"] if rows else after,
        }


@app.get("/admin/queue", dependencies=[Depends(admin_key)], tags=["Operations"])
async def queue_status():
    async with engine.connect() as conn:
        jobs = list(
            (
                await execute(conn, "SELECT status,count(*) AS count FROM jobs GROUP BY status")
            ).mappings()
        )
        outbox = await one(
            conn,
            "SELECT count(*) FILTER(WHERE published_at IS NULL) AS unpublished, COALESCE(sum(GREATEST(publications-1,0)),0) AS republications FROM outbox",
        )
        heartbeats = list(
            (
                await execute(
                    conn,
                    "SELECT name,seen_at,seen_at>now()-interval '20 seconds' AS alive FROM heartbeats",
                )
            ).mappings()
        )
        return {"jobs": jobs, "outbox": outbox, "heartbeats": heartbeats}


@app.post("/admin/jobs/{job_id}/retry", dependencies=[Depends(admin_key)], tags=["Administration"])
async def retry_job(job_id: UUID):
    async with engine.begin() as conn:
        job = await one(conn, "SELECT tenant_id FROM jobs WHERE id=:id", id=job_id)
        if job is None:
            raise HTTPException(404, "Job not found")
        from .ledger import lock_tenant

        await lock_tenant(conn, job["tenant_id"])
        row = await one(
            conn,
            "UPDATE jobs SET status='pending',attempts=0,error=NULL,finished_at=NULL WHERE id=:id AND status='failed' RETURNING *",
            id=job_id,
        )
        if row is None:
            raise HTTPException(409, "Only a failed job can be retried")
        await execute(conn, "UPDATE outbox SET next_attempt_at=now() WHERE job_id=:id", id=job_id)
        return row
