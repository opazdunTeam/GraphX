"""Source-native records produced by parsers."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SourceRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    local_ref: str = Field(min_length=1)
    artifact_local_ref: str = Field(min_length=1)
    record_type: str = Field(min_length=1)
    external_id: str | None = Field(default=None, min_length=1)
    schema_version: str = Field(min_length=1)
    raw_data: dict[str, Any]
