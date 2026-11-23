"""Source-native records produced by parsers."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from graphx_workers.contracts.values import validate_jsonb_compatible, validate_persisted_text


class SourceRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    local_ref: str = Field(min_length=1)
    artifact_local_ref: str = Field(min_length=1)
    record_type: str = Field(min_length=1)
    external_id: str | None = Field(default=None, min_length=1)
    schema_version: str = Field(min_length=1)
    raw_data: dict[str, Any]

    @field_validator("local_ref", "artifact_local_ref", "record_type", "schema_version")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        return validate_persisted_text(value, "source record text fields")

    @field_validator("external_id")
    @classmethod
    def validate_external_id(cls, value: str | None) -> str | None:
        return None if value is None else validate_persisted_text(value, "external_id")

    @field_validator("raw_data", mode="before")
    @classmethod
    def validate_raw_data(cls, value: dict[str, Any]) -> dict[str, Any]:
        return validate_jsonb_compatible(value, "raw_data")
