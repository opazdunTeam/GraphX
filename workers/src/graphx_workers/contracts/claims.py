"""Atomic claims emitted by source mappers."""

import re
from calendar import monthrange
from datetime import date
from math import isfinite
from typing import Any, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from graphx_workers.contracts.schema import validate_raw_locator_numbers
from graphx_workers.contracts.values import (
    MAX_SAFE_INTEGER,
    has_meaningful_text,
    validate_jsonb_compatible,
    validate_persisted_text,
)


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

    @classmethod
    def model_validate_json(
        cls,
        json_data: str | bytes | bytearray,
        *,
        strict: bool | None = None,
        extra: Literal["allow", "ignore", "forbid"] | None = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        validate_raw_locator_numbers(json_data, single_claim=True)
        return super().model_validate_json(
            json_data,
            strict=strict,
            extra=extra,
            context=context,
            by_alias=by_alias,
            by_name=by_name,
        )

    @field_validator("subject_local_ref", "predicate")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        return validate_persisted_text(value, "claim reference and predicate fields")

    @field_validator("object_local_ref")
    @classmethod
    def validate_object_local_ref(cls, value: str | None) -> str | None:
        return None if value is None else validate_persisted_text(value, "object_local_ref")

    @field_validator(
        "raw_value",
        "normalized_value",
        "qualifiers",
        "evidence_locator",
        "evidence_excerpt",
        mode="before",
    )
    @classmethod
    def validate_persisted_json_values(cls, value: Any, info: ValidationInfo) -> Any:
        return validate_jsonb_compatible(value, info.field_name or "claim value")

    @model_validator(mode="after")
    def validate_claim(self) -> Self:
        _validate_evidence_locator(self.evidence_locator)
        if self.claim_kind == "relationship":
            if not self.object_local_ref:
                raise ValueError("relationship claim requires a non-empty object_local_ref")
        elif self.claim_kind == "attribute" and self.object_local_ref is not None:
            raise ValueError("attribute claims cannot contain object_local_ref")
        elif self.claim_kind == "attribute" and not any(
            meaningful_claim_value(value) for value in (self.raw_value, self.normalized_value)
        ):
            raise ValueError("attribute claim requires a meaningful value")
        elif (
            self.claim_kind == "event"
            and not self.object_local_ref
            and not any(
                meaningful_claim_value(value) for value in (self.raw_value, self.normalized_value)
            )
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


def _validate_evidence_locator(locator: dict[str, Any]) -> None:
    keys = set(locator)
    has_json = "json_pointer" in keys
    has_csv = "csv_row" in keys
    has_document = bool(keys & {"page_number", "fragment"})
    if sum((has_json, has_csv, has_document)) != 1:
        raise ValueError(
            "evidence_locator must identify a JSON pointer, CSV row, or document page/fragment"
        )
    if has_json:
        pointer = locator["json_pointer"]
        if not isinstance(pointer, str) or re.fullmatch(r"(?:/(?:[^~/]|~[01])*)*", pointer) is None:
            raise ValueError("evidence json_pointer must be a valid RFC 6901 pointer")
        return
    if has_csv:
        row = locator["csv_row"]
        if not _positive_integer(row):
            raise ValueError("evidence csv_row must be a positive integer")
        return
    if has_document:
        page = locator.get("page_number")
        fragment = locator.get("fragment")
        if "page_number" in locator and not _positive_integer(page):
            raise ValueError("evidence page_number must be a positive integer")
        if "fragment" in locator and (
            not isinstance(fragment, str) or not has_meaningful_text(fragment) or "\x00" in fragment
        ):
            raise ValueError("evidence fragment must be a non-empty string")
        return


def _positive_integer(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return False
    if isinstance(value, float) and (not isfinite(value) or not value.is_integer()):
        return False
    return 0 < value <= MAX_SAFE_INTEGER


def meaningful_claim_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return has_meaningful_text(value)
    if isinstance(value, (list, dict)):
        return bool(value)
    return True


def _latest_possible_date(
    anchor: date, precision: Literal["day", "month", "year", "approximate", "unknown"]
) -> date:
    """Return the latest date represented by an exact, month, or year boundary."""
    if precision == "year":
        return date(anchor.year, 12, 31)
    if precision == "month":
        return date(anchor.year, anchor.month, monthrange(anchor.year, anchor.month)[1])
    return anchor
