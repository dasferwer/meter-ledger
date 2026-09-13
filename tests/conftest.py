import os

import httpx
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from meterledger.auth import key_hash
from meterledger.db import engine, execute
from meterledger.main import app
from meterledger.seed import DEMO_ID, DEMO_KEY, seed


@pytest.fixture(autouse=True)
async def database():
    url = os.environ.get("MIGRATION_DATABASE_URL", "")
    if os.environ.get("TESTING") != "true" or "@test-db:" not in url:
        pytest.fail("Integration tests require the isolated Compose test-db")
    owner = create_async_engine(url)
    async with owner.begin() as conn:
        await execute(conn, "TRUNCATE tenants,rates,events,invoices,jobs,outbox,heartbeats CASCADE")
    await seed()
    yield owner
    await owner.dispose()


@pytest.fixture
async def client():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": DEMO_KEY},
    ) as client:
        yield client


@pytest.fixture
def tenant():
    return DEMO_ID


@pytest.fixture
async def other_tenant():
    from uuid import uuid4

    identifier, key = uuid4(), "test-other-tenant-key"
    async with engine.begin() as conn:
        await execute(
            conn,
            "INSERT INTO tenants(id,name,key_hash) VALUES(:id,'Other',:hash)",
            id=identifier,
            hash=key_hash(key),
        )
    return identifier, {"X-API-Key": key}
