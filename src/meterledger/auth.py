import hashlib
import secrets
from uuid import UUID

from fastapi import Depends, HTTPException
from fastapi.security import APIKeyHeader

from .config import settings
from .db import engine, one

customer_header = APIKeyHeader(name="X-API-Key", scheme_name="CustomerKey", auto_error=False)
admin_header = APIKeyHeader(name="X-Admin-Token", scheme_name="AdministratorKey", auto_error=False)


def key_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


async def tenant_key(x_api_key: str | None = Depends(customer_header)) -> UUID:
    async with engine.connect() as conn:
        row = await one(
            conn, "SELECT id FROM tenants WHERE key_hash=:hash", hash=key_hash(x_api_key or "")
        )
    if row is None:
        raise HTTPException(401, "Invalid API key")
    return row["id"]


async def admin_key(x_admin_token: str | None = Depends(admin_header)):
    if not secrets.compare_digest((x_admin_token or "").encode(), settings.admin_token.encode()):
        raise HTTPException(403, "Administrator token required")
