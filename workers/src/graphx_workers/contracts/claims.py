"""Atomic claims emitted by source mappers."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Claim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_kind: str = Field(min_length=1)
    subject_local_ref: str = Field(min_length=1)
    predicate: str = Field(min_length=1)
    object_local_ref: str | None = None
    value: Any | None = None
    evidence_locator: dict[str, Any]
