from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

Metric = Literal["api_calls", "storage_gb_hours", "compute_seconds"]


def decimal_string(value):
    if not isinstance(value, str):
        raise ValueError("Use a decimal string, for example '12.500000'")
    if len(value) > 32:
        raise ValueError("Decimal string is too long")
    return value


Quantity = Annotated[
    Decimal,
    BeforeValidator(decimal_string, json_schema_input_type=str),
    Field(gt=0, le=10**9, max_digits=16, decimal_places=6),
]
Price = Annotated[
    Decimal,
    BeforeValidator(decimal_string, json_schema_input_type=str),
    Field(gt=0, le=10**6, max_digits=15, decimal_places=8),
]


class Input(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TenantInput(Input):
    name: str = Field(min_length=1, max_length=120)


class UsageInput(Input):
    event_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9_.:-]+$")
    metric: Metric
    quantity: Quantity
    occurred_at: AwareDatetime

    @model_validator(mode="after")
    def whole_api_calls(self):
        if self.metric == "api_calls" and self.quantity != self.quantity.to_integral_value():
            raise ValueError("API call count must be an integer")
        return self

    @field_validator("occurred_at")
    @classmethod
    def past_event(cls, value):
        value = value.astimezone(UTC)
        if value < datetime(2020, 1, 1, tzinfo=UTC) or value > datetime.now(UTC):
            raise ValueError("Event time must be between 2020-01-01 and now")
        return value


class CorrectionInput(Input):
    event_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9_.:-]+$")
    original_id: UUID
    quantity: Quantity
    reason: str = Field(min_length=5, max_length=500)


class RateInput(Input):
    metric: Metric
    unit_price: Price
    effective_at: AwareDatetime


class IssueInput(Input):
    period: str = Field(pattern=r"^20\d{2}-(0[1-9]|1[0-2])$")
