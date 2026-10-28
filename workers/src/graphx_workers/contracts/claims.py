"""Atomic claims emitted by source mappers."""

from datetime import datetime
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Claim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_kind: Literal["attribute", "relationship", "event"]
    subject_local_ref: str = Field(min_length=1)
    predicate: str = Field(min_length=1)
    object_local_ref: str | None = None
    value: Any | None = None
    raw_value: Any | None = None
    normalized_value: Any | None = None
    qualifiers: dict[str, Any] = Field(default_factory=dict)
    valid_from: str | None = None
    valid_to: str | None = None
    valid_from_precision: Literal["day", "month", "year", "approximate", "unknown"] = "unknown"
    valid_to_precision: Literal["day", "month", "year", "approximate", "unknown"] = "unknown"
    temporal_basis: Literal["explicit", "event_derived", "snapshot", "computed", "unknown"] = (
        "unknown"
    )
    snapshot_at: datetime | None = None
    evidence_locator: dict[str, Any] = Field(min_length=1)
    evidence_excerpt: str | None = None

    @model_validator(mode="after")
    def require_object_or_value(self) -> Self:
        if not ({"object_local_ref", "value"} & self.model_fields_set):
            raise ValueError("claim requires object_local_ref or value")
        return self
