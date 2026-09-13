import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import DBAPIError

from meterledger import billing, ledger
from meterledger.billing import process_job
from meterledger.config import settings
from meterledger.db import engine, execute, one
from meterledger.messaging import dispatch_once, fail_job
from meterledger.rating import rate_groups
from meterledger.seed import seed

PERIOD = "2024-01"
ADMIN = {"X-Admin-Token": settings.admin_token}


def usage(key="first", quantity="500", at="2024-01-15T00:00:00Z", metric="api_calls"):
    return {"event_id": key, "metric": metric, "quantity": quantity, "occurred_at": at}


async def issue(client, key=None, period=PERIOD):
    response = await client.post(
        "/billing/issue", json={"period": period}, headers={"Idempotency-Key": key or str(uuid4())}
    )
    assert response.status_code == 202, response.text
    job = response.json()
    await process_job(UUID(job["id"]))
    completed = (await client.get("/jobs/" + job["id"])).json()
    assert completed["status"] == "done"
    return (await client.get("/invoices/" + completed["invoice_id"])).json()


async def test_concurrent_event_replays_create_one_record(client):
    responses = await asyncio.gather(*(client.post("/events", json=usage()) for _ in range(30)))
    assert [r.status_code for r in responses].count(201) == 1
    assert [r.status_code for r in responses].count(200) == 29
    assert len({r.json()["id"] for r in responses}) == 1
    assert (await client.get("/me")).json()["sequence"] == 1
    assert len((await client.get("/events")).json()["items"]) == 1


async def test_conflicting_payload_and_normalized_decimal(client):
    assert (await client.post("/events", json=usage(quantity="500.0"))).status_code == 201
    assert (await client.post("/events", json=usage(quantity="500.000000"))).status_code == 200
    assert (await client.post("/events", json=usage(quantity="501"))).status_code == 409


@pytest.mark.parametrize(
    "quantity", [1.5, 3, "NaN", "Infinity", "-1", "0", "0.0000001", "1000000001"]
)
async def test_reject_inexact_or_invalid_quantities(client, quantity):
    assert (await client.post("/events", json=usage(quantity=quantity))).status_code == 422


@pytest.mark.parametrize(
    "at", ["2035-01-01T00:00:00Z", "2019-12-31T23:59:59Z", "2024-01-01T00:00:00"]
)
async def test_event_time_validation(client, at):
    assert (await client.post("/events", json=usage(at=at))).status_code == 422


async def test_late_usage_and_credit_preserve_original_invoice(client):
    original = (await client.post("/events", json=usage())).json()
    first = await issue(client)
    assert first["calculation"]["delta_cents"] == 1
    await client.post("/events", json=usage("late", "100"))
    second = await issue(client)
    assert second["revision"] == 2
    assert second["calculation"]["delta_cents"] == 0
    await client.post("/events", json=usage("later", "900"))
    third = await issue(client)
    assert third["calculation"]["cumulative_cents"] == 2
    assert third["calculation"]["delta_cents"] == 1
    correction = {
        "event_id": "credit",
        "original_id": original["id"],
        "quantity": "500",
        "reason": "Duplicate upstream measurement",
    }
    assert (await client.post("/corrections", json=correction)).status_code == 201
    assert (await client.post("/corrections", json=correction)).status_code == 200
    fourth = await issue(client)
    assert fourth["calculation"]["delta_cents"] == -1
    assert fourth["calculation"]["cumulative_cents"] == 1
    assert (await client.get("/invoices/" + first["id"])).json() == first
    for invoice in [first, second, third, fourth]:
        assert (await client.get(f"/invoices/{invoice['id']}/verify")).json()["matches"]
    sources = (await client.get(f"/invoices/{first['id']}/events")).json()["items"]
    assert len(sources) == 1 and sources[0]["id"] == original["id"]
    assert sum(x["calculation"]["delta_cents"] for x in [first, second, third, fourth]) == 1


async def test_corrections_cannot_overcredit_under_concurrency(client):
    original = (await client.post("/events", json=usage(quantity="4"))).json()
    responses = await asyncio.gather(
        *(
            client.post(
                "/corrections",
                json={
                    "event_id": f"credit-{i}",
                    "original_id": original["id"],
                    "quantity": "1",
                    "reason": "Incorrect measurement",
                },
            )
            for i in range(12)
        )
    )
    assert [r.status_code for r in responses].count(201) == 4
    assert [r.status_code for r in responses].count(409) == 8
    correction_id = next(r.json()["id"] for r in responses if r.status_code == 201)
    assert (
        await client.post(
            "/corrections",
            json={
                "event_id": "recursive",
                "original_id": correction_id,
                "quantity": "1",
                "reason": "Cannot correct a correction",
            },
        )
    ).status_code == 404
    assert (await issue(client))["calculation"]["cumulative_cents"] == 0


async def test_rate_at_event_time_and_correction_uses_original_rate(client, database):
    async with database.begin() as conn:
        await execute(
            conn,
            "INSERT INTO rates(id,metric,effective_at,unit_price) VALUES(:id,'api_calls','2024-01-16T00:00:00Z',0.01)",
            id=uuid4(),
        )
    before = (
        await client.post("/events", json=usage("before", "100", "2024-01-15T23:59:59.999999Z"))
    ).json()
    after = (
        await client.post("/events", json=usage("after", "100", "2024-01-16T00:00:00Z"))
    ).json()
    assert before["unit_price"] == "0.00001" and after["unit_price"] == "0.01"
    credit = (
        await client.post(
            "/corrections",
            json={
                "event_id": "credit",
                "original_id": before["id"],
                "quantity": "100",
                "reason": "Original usage was incorrect",
            },
        )
    ).json()
    assert credit["rate_id"] == before["rate_id"] and credit["occurred_at"] == before["occurred_at"]
    invoice = await issue(client)
    assert invoice["calculation"]["cumulative_cents"] == 100


async def test_utc_month_boundaries(client):
    for key, at in [
        ("before", "2024-01-01T00:30:00+01:00"),
        ("inside", "2024-02-01T00:30:00+01:00"),
        ("after", "2024-02-01T00:00:00Z"),
    ]:
        assert (await client.post("/events", json=usage(key, "1000", at))).status_code == 201
    invoice = await issue(client)
    sources = (await client.get(f"/invoices/{invoice['id']}/events")).json()["items"]
    assert [row["external_id"] for row in sources] == ["inside"]


async def test_empty_and_unchanged_period_does_not_add_revisions(client):
    first = await issue(client)
    await client.post("/events", json=usage(at="2024-02-01T00:00:00Z"))
    second = await issue(client)
    assert first["id"] == second["id"] and first["calculation"]["event_count"] == 0


async def test_parallel_jobs_and_duplicate_delivery(client):
    await client.post("/events", json=usage())
    responses = await asyncio.gather(
        *(
            client.post(
                "/billing/issue", json={"period": PERIOD}, headers={"Idempotency-Key": str(i)}
            )
            for i in range(15)
        )
    )
    ids = [UUID(r.json()["id"]) for r in responses]
    await asyncio.gather(*(process_job(job) for job in ids + ids))
    assert len((await client.get("/invoices")).json()["items"]) == 1
    states = await asyncio.gather(*(client.get("/jobs/" + str(job)) for job in ids))
    assert all(response.json()["status"] == "done" for response in states)


async def test_request_key_conflict_and_pending_limit(client):
    first = await client.post(
        "/billing/issue", json={"period": PERIOD}, headers={"Idempotency-Key": "same"}
    )
    repeat = await client.post(
        "/billing/issue", json={"period": PERIOD}, headers={"Idempotency-Key": "same"}
    )
    assert first.json()["id"] == repeat.json()["id"]
    assert (
        await client.post(
            "/billing/issue", json={"period": "2024-02"}, headers={"Idempotency-Key": "same"}
        )
    ).status_code == 409
    for i in range(19):
        assert (
            await client.post(
                "/billing/issue", json={"period": PERIOD}, headers={"Idempotency-Key": str(i)}
            )
        ).status_code == 202
    assert (
        await client.post(
            "/billing/issue", json={"period": PERIOD}, headers={"Idempotency-Key": "overflow"}
        )
    ).status_code == 429


async def test_open_period_only_has_preview(client):
    period = datetime.now(UTC).strftime("%Y-%m")
    assert (
        await client.post(
            "/billing/issue", json={"period": period}, headers={"Idempotency-Key": "now"}
        )
    ).status_code == 409
    assert (await client.get(f"/billing/{period}/preview")).status_code == 200
    assert (await client.get("/billing/2024-99/preview")).status_code == 422


async def test_tenant_isolation_and_auth(client, other_tenant):
    _, other = other_tenant
    event = (await client.post("/events", json=usage())).json()
    invoice = await issue(client, key="job")
    job = (
        await client.post(
            "/billing/issue", json={"period": PERIOD}, headers={"Idempotency-Key": "job"}
        )
    ).json()
    for path in [
        f"/invoices/{invoice['id']}",
        f"/invoices/{invoice['id']}/events",
        f"/invoices/{invoice['id']}/verify",
        f"/jobs/{job['id']}",
        f"/invoices?before_id={invoice['id']}",
    ]:
        assert (await client.get(path, headers=other)).status_code == 404
    assert (await client.get("/events", headers=other)).json()["items"] == []
    assert (
        await client.post(
            "/corrections",
            headers=other,
            json={
                "event_id": "theft",
                "original_id": event["id"],
                "quantity": "1",
                "reason": "Cross tenant attempt",
            },
        )
    ).status_code == 404
    assert (await client.get("/events", headers={"X-API-Key": "bad"})).status_code == 401
    assert (await client.get("/admin/queue")).status_code == 403
    assert (await client.post("/events", json=usage(), headers=other)).status_code == 201


async def test_schedule_rate_rejects_retroactive_changes(client):
    data = {"metric": "api_calls", "unit_price": "0.001", "effective_at": "2024-01-01T00:00:00Z"}
    assert (await client.post("/admin/rates", json=data, headers=ADMIN)).status_code == 422
    data["effective_at"] = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    assert (await client.post("/admin/rates", json=data, headers=ADMIN)).status_code == 201
    assert (await client.post("/admin/rates", json=data, headers=ADMIN)).status_code == 409
    assert (await client.post("/admin/rates", json=data)).status_code == 403


@pytest.mark.parametrize("table", ["events", "rates", "invoices"])
@pytest.mark.parametrize("command", ["UPDATE", "DELETE"])
async def test_append_only_history_blocks_owner_mutation(client, database, table, command):
    await client.post("/events", json=usage())
    await issue(client)
    statement = f"UPDATE {table} SET id=id" if command == "UPDATE" else f"DELETE FROM {table}"
    with pytest.raises(DBAPIError, match="append-only"):
        async with database.begin() as conn:
            await execute(conn, statement)


async def test_runtime_role_cannot_truncate_history():
    with pytest.raises(DBAPIError, match="permission denied"):
        async with engine.begin() as conn:
            await execute(conn, "TRUNCATE events")


async def test_job_and_outbox_commit_together(client, monkeypatch):
    real = ledger.execute

    async def broken(conn, sql, **params):
        if "INSERT INTO outbox" in sql:
            raise RuntimeError("Injected outbox write failure")
        return await real(conn, sql, **params)

    monkeypatch.setattr(ledger, "execute", broken)
    with pytest.raises(RuntimeError, match="Injected"):
        await client.post(
            "/billing/issue", json={"period": PERIOD}, headers={"Idempotency-Key": "atomic"}
        )
    async with engine.connect() as conn:
        assert (await one(conn, "SELECT count(*) AS count FROM jobs"))["count"] == 0


async def test_worker_rollback_then_redelivery(client, monkeypatch):
    await client.post("/events", json=usage())
    job = (
        await client.post(
            "/billing/issue", json={"period": PERIOD}, headers={"Idempotency-Key": "crash"}
        )
    ).json()
    real = billing.execute

    async def broken(conn, sql, **params):
        if "UPDATE jobs" in sql:
            raise RuntimeError("Injected crash after invoice insert")
        return await real(conn, sql, **params)

    monkeypatch.setattr(billing, "execute", broken)
    with pytest.raises(RuntimeError):
        await process_job(UUID(job["id"]))
    assert (await client.get("/invoices")).json()["items"] == []
    assert (await client.get("/jobs/" + job["id"])).json()["status"] == "pending"
    monkeypatch.setattr(billing, "execute", real)
    await process_job(UUID(job["id"]))
    assert len((await client.get("/invoices")).json()["items"]) == 1


async def test_invoice_cutoff_excludes_later_commit(client, monkeypatch):
    await client.post("/events", json=usage())
    job = (
        await client.post(
            "/billing/issue", json={"period": PERIOD}, headers={"Idempotency-Key": "boundary"}
        )
    ).json()
    started, release = asyncio.Event(), asyncio.Event()
    real = billing.calculate

    async def paused(*args, **kwargs):
        started.set()
        await release.wait()
        return await real(*args, **kwargs)

    monkeypatch.setattr(billing, "calculate", paused)
    worker = asyncio.create_task(process_job(UUID(job["id"])))
    await asyncio.wait_for(started.wait(), 3)
    ingestion = asyncio.create_task(client.post("/events", json=usage("late", "1000")))
    await asyncio.sleep(0.05)
    assert not ingestion.done()
    release.set()
    await worker
    assert (await ingestion).status_code == 201
    result = (await client.get("/jobs/" + job["id"])).json()
    invoice = (await client.get("/invoices/" + result["invoice_id"])).json()
    assert invoice["cutoff"] == 1 and invoice["calculation"]["event_count"] == 1
    assert (await client.get(f"/invoices/{invoice['id']}/verify")).json()["matches"]


async def test_publish_confirm_gap_leaves_retryable_outbox(client, monkeypatch):
    job = (
        await client.post(
            "/billing/issue", json={"period": PERIOD}, headers={"Idempotency-Key": "publish"}
        )
    ).json()
    published = asyncio.Event()

    class Channel:
        @property
        def default_exchange(self):
            return self

        async def publish(self, *args, **kwargs):
            published.set()

    monkeypatch.setattr(settings, "dispatcher_after_publish_delay", 30)
    task = asyncio.create_task(dispatch_once(Channel()))
    await asyncio.wait_for(published.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with engine.connect() as conn:
        row = await one(conn, "SELECT * FROM outbox WHERE job_id=:id", id=UUID(job["id"]))
        assert row["published_at"] is None and row["publications"] == 0
    monkeypatch.setattr(settings, "dispatcher_after_publish_delay", 0)
    await process_job(UUID(job["id"]))
    assert await dispatch_once(Channel())


async def test_failed_job_retry(client):
    job = (
        await client.post(
            "/billing/issue", json={"period": PERIOD}, headers={"Idempotency-Key": "failed"}
        )
    ).json()
    for _ in range(5):
        await fail_job(UUID(job["id"]))
    assert (await client.get("/jobs/" + job["id"])).json()["status"] == "failed"
    assert (await client.post(f"/admin/jobs/{job['id']}/retry", headers=ADMIN)).status_code == 200
    await process_job(UUID(job["id"]))
    assert (await client.get("/jobs/" + job["id"])).json()["status"] == "done"


async def test_seed_preserves_history_and_cursor_pagination(client):
    await client.post("/events", json=usage())
    first = await issue(client)
    await client.post("/events", json=usage("late", "100"))
    second = await issue(client)
    await seed()
    await seed()
    assert (await client.get("/invoices/" + first["id"])).json() == first
    page = (await client.get("/invoices?limit=1")).json()
    assert page["items"][0]["id"] == second["id"]
    page2 = (
        await client.get("/invoices", params={"before_id": page["next_before_id"], "limit": 1})
    ).json()
    assert page2["items"][0]["id"] == first["id"]
    assert (await client.get("/me")).json()["sequence"] == 2


def test_rounding_uses_accumulated_group_not_each_event():
    result = rate_groups(
        {
            "x": {
                "quantity": Decimal("1000"),
                "price": Decimal("0.00001"),
                "metric": "api_calls",
                "count": 1000,
            }
        }
    )
    assert result["cumulative_cents"] == 1
    result = rate_groups(
        {
            "x": {
                "quantity": Decimal("1500"),
                "price": Decimal("0.00001"),
                "metric": "api_calls",
                "count": 1500,
            }
        },
        result,
    )
    assert result["cumulative_cents"] == 2 and result["delta_cents"] == 1


async def test_streamed_month_matches_independent_decimal_sum(client, tenant, database):
    async with database.begin() as conn:
        await execute(
            conn,
            """INSERT INTO events(id,tenant_id,sequence,external_id,body_hash,metric,quantity,occurred_at,rate_id,unit_price)
            SELECT gen_random_uuid(),:tenant,n,'bulk-'||n,'fixture','api_calls',10,
                '2024-01-15T00:00:00Z',r.id,r.unit_price
            FROM generate_series(1,3001) n CROSS JOIN rates r WHERE r.metric='api_calls'""",
            tenant=tenant,
        )
        await execute(conn, "UPDATE tenants SET sequence=3001 WHERE id=:tenant", tenant=tenant)
    invoice = await issue(client)
    expected = sum(Decimal("10") * Decimal("0.00001") for _ in range(3001))
    assert invoice["calculation"]["cumulative_cents"] == int(
        (expected * 100).quantize(Decimal("1"))
    )
    assert invoice["calculation"]["event_count"] == 3001
    assert (await client.get(f"/invoices/{invoice['id']}/verify")).json()["matches"]
    after, identifiers = 0, set()
    while True:
        page = (
            await client.get(
                f"/invoices/{invoice['id']}/events", params={"after": after, "limit": 500}
            )
        ).json()
        if not page["items"]:
            break
        identifiers.update(event["id"] for event in page["items"])
        after = page["next_after"]
    assert len(identifiers) == 3001


async def test_fractional_measurements_preserve_precision_and_call_counts_are_whole(client):
    assert (await client.post("/events", json=usage(quantity="0.5"))).status_code == 422
    event = (await client.post("/events", json=usage(quantity="1"))).json()
    assert (
        await client.post(
            "/corrections",
            json={
                "event_id": "half-call",
                "original_id": event["id"],
                "quantity": "0.5",
                "reason": "A call cannot be split",
            },
        )
    ).status_code == 422
    fractional = (
        await client.post("/events", json=usage("storage", "0.123456", metric="storage_gb_hours"))
    ).json()
    assert fractional["quantity"] == "0.123456"
    assert fractional["unit_price"] == "0.00014"
