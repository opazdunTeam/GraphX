"""Source-native records produced by parsers."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SourceRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    local_ref: str = Field(min_length=1)
    record_type: str = Field(min_length=1)
    raw_data: dict[str, Any]
