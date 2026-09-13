import hashlib
from decimal import ROUND_HALF_UP, Decimal, localcontext

from sqlalchemy import text

from .ledger import canonical, decimal_text, period_bounds

ENGINE_VERSION = "flat-monthly-v1"


def rate_groups(groups, previous=None):
    previous_lines = {line["rate_id"]: line for line in (previous or {}).get("lines", [])}
    lines = []
    # Округляем накопленную стоимость группы один раз. Позднее событие меняет разницу счетов, а не правило округления.
    with localcontext() as context:
        context.prec = 50
        for rate_id, group in sorted(groups.items()):
            quantity, price = group["quantity"], group["price"]
            raw = quantity * price
            cents = int((raw * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            before = previous_lines.get(rate_id, {}).get("cumulative_cents", 0)
            lines.append(
                {
                    "rate_id": rate_id,
                    "metric": group["metric"],
                    "quantity": decimal_text(quantity),
                    "unit_price": decimal_text(price),
                    "unrounded_amount": decimal_text(raw),
                    "cumulative_cents": cents,
                    "previous_cents": before,
                    "delta_cents": cents - before,
                    "event_count": group["count"],
                }
            )
    cumulative = sum(line["cumulative_cents"] for line in lines)
    delta = sum(line["delta_cents"] for line in lines)
    assert cumulative >= 0
    return {
        "engine_version": ENGINE_VERSION,
        "currency": "USD",
        "rounding": "HALF_UP_PER_RATE_GROUP",
        "lines": lines,
        "cumulative_cents": cumulative,
        "delta_cents": delta,
    }


async def calculate(conn, tenant_id, period, cutoff, previous=None):
    start, end = period_bounds(period)
    groups, source_hash, count = {}, hashlib.sha256(), 0
    result = await conn.stream(
        text("""
        SELECT id,sequence,external_id,body_hash,metric,quantity,occurred_at,rate_id,unit_price,original_id,reason
        FROM events WHERE tenant_id=:tenant AND occurred_at>=:start AND occurred_at<:end AND sequence<=:cutoff
        ORDER BY sequence
    """),
        {"tenant": tenant_id, "start": start, "end": end, "cutoff": cutoff},
        execution_options={"yield_per": 1000},
    )
    with localcontext() as context:
        context.prec = 50
        async for row in result.mappings():
            rate_id = str(row["rate_id"])
            group = groups.setdefault(
                rate_id,
                {
                    "quantity": Decimal(0),
                    "price": row["unit_price"],
                    "metric": row["metric"],
                    "count": 0,
                },
            )
            group["quantity"] += row["quantity"]
            group["count"] += 1
            source = {key: str(value) if value is not None else None for key, value in row.items()}
            source["occurred_at"] = row["occurred_at"].isoformat()
            source_hash.update((canonical(source) + "\n").encode())
            count += 1
    calculated = rate_groups(groups, previous)
    return {
        **calculated,
        "period": period,
        "cutoff": cutoff,
        "event_count": count,
        "source_hash": source_hash.hexdigest(),
    }
