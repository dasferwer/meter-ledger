import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import NAMESPACE_DNS, uuid5

from .auth import key_hash
from .db import engine, execute

DEMO_KEY = "ml_local_demo_meterledger_customer_change_before_deployment"
DEMO_ID = uuid5(NAMESPACE_DNS, "meterledger.demo")


async def seed():
    async with engine.begin() as conn:
        await execute(
            conn,
            "INSERT INTO tenants(id,name,key_hash) VALUES(:id,'Demo Workspace',:hash) ON CONFLICT DO NOTHING",
            id=DEMO_ID,
            hash=key_hash(DEMO_KEY),
        )
        for metric, price in (
            ("api_calls", "0.00001"),
            ("storage_gb_hours", "0.00014"),
            ("compute_seconds", "0.000025"),
        ):
            await execute(
                conn,
                "INSERT INTO rates(id,metric,effective_at,unit_price) VALUES(:id,:metric,:at,:price) ON CONFLICT DO NOTHING",
                id=uuid5(NAMESPACE_DNS, "meterledger.rate." + metric),
                metric=metric,
                at=datetime(2020, 1, 1, tzinfo=UTC),
                price=Decimal(price),
            )


if __name__ == "__main__":
    asyncio.run(seed())
