import asyncio
import sys

from meterledger.db import engine, one


async def main():
    async with engine.connect() as conn:
        row = await one(
            conn,
            "SELECT seen_at>now()-interval '20 seconds' AS alive FROM heartbeats WHERE name=:name",
            name=sys.argv[1],
        )
    await engine.dispose()
    return bool(row and row["alive"])


raise SystemExit(0 if asyncio.run(main()) else 1)
