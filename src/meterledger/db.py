from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from .config import settings

engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=10,
    connect_args={"server_settings": {"timezone": "UTC", "application_name": "meterledger"}},
)


async def one(conn, sql, **params):
    return (await conn.execute(text(sql), params)).mappings().first()


async def execute(conn, sql, **params):
    return await conn.execute(text(sql), params)
