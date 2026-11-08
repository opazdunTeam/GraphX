"""Atomic claims emitted by source mappers."""

from calendar import monthrange
from datetime import date
from typing import Any, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class Claim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_kind: Literal["attribute", "relationship", "event"]
    subject_local_ref: str = Field(min_length=1)
    predicate: str = Field(min_length=1)
    object_local_ref: str | None = Field(default=None, min_length=1)
    raw_value: Any | None = None
    normalized_value: Any | None = None
    qualifiers: dict[str, Any] = Field(default_factory=dict)
    valid_from: date | None = None
    valid_to: date | None = None
    valid_from_precision: Literal["day", "month", "year", "approximate", "unknown"] = "unknown"
    valid_to_precision: Literal["day", "month", "year", "approximate", "unknown"] = "unknown"
    temporal_basis: Literal["explicit", "event_derived", "snapshot", "computed", "unknown"] = (
        "unknown"
    )
    snapshot_at: AwareDatetime | None = None
    evidence_locator: dict[str, Any] = Field(min_length=1)
    evidence_excerpt: str | None = None

    @model_validator(mode="after")
    def validate_claim(self) -> Self:
        if self.claim_kind == "relationship":
            if not self.object_local_ref:
                raise ValueError("relationship claim requires a non-empty object_local_ref")
        elif self.claim_kind == "attribute" and all(
            value is None for value in (self.raw_value, self.normalized_value)
        ):
            raise ValueError("attribute claim requires a meaningful value")
        elif (
            self.claim_kind == "event"
            and not self.object_local_ref
            and all(value is None for value in (self.raw_value, self.normalized_value))
        ):
            raise ValueError("event claim requires an object reference or meaningful value")

        for name, value, precision in (
            ("valid_from", self.valid_from, self.valid_from_precision),
            ("valid_to", self.valid_to, self.valid_to_precision),
        ):
            if precision == "unknown" and value is not None:
                raise ValueError(f"{name} must be null when precision is unknown")
            if precision != "unknown" and value is None:
                raise ValueError(f"{name} is required when precision is known")
            if value is not None and precision == "month" and value.day != 1:
                raise ValueError(f"{name} with month precision must use the first day as anchor")
            if value is not None and precision == "year" and (value.month, value.day) != (1, 1):
                raise ValueError(f"{name} with year precision must use January 1 as anchor")

        if (
            self.valid_from is not None
            and self.valid_to is not None
            and "approximate" not in (self.valid_from_precision, self.valid_to_precision)
        ):
            latest_valid_to = _latest_possible_date(self.valid_to, self.valid_to_precision)
            if latest_valid_to <= self.valid_from:
                raise ValueError("valid_to cannot be later than valid_from")

        if self.temporal_basis == "unknown" and any(
            value is not None for value in (self.valid_from, self.valid_to, self.snapshot_at)
        ):
            raise ValueError("temporal_basis is required for temporal values")
        if self.temporal_basis == "snapshot" and self.snapshot_at is None:
            raise ValueError("snapshot_at is required when temporal_basis is snapshot")
        if self.temporal_basis == "snapshot" and any(
            value is not None for value in (self.valid_from, self.valid_to)
        ):
            raise ValueError("snapshot claims cannot contain interval boundaries")
        if self.temporal_basis == "snapshot" and any(
            precision != "unknown"
            for precision in (self.valid_from_precision, self.valid_to_precision)
        ):
            raise ValueError("snapshot claims require unknown interval precision")
        if self.temporal_basis != "snapshot" and self.snapshot_at is not None:
            raise ValueError("snapshot_at is only allowed for snapshot claims")
        return self


def _latest_possible_date(
    anchor: date, precision: Literal["day", "month", "year", "approximate", "unknown"]
) -> date:
    """Return the latest date represented by an exact, month, or year boundary."""
    if precision == "year":
        return date(anchor.year, 12, 31)
    if precision == "month":
        return date(anchor.year, anchor.month, monthrange(anchor.year, anchor.month)[1])
    return anchor
