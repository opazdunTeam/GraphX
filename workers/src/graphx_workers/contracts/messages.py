"""Typed source collection commands and events."""

from typing import Any, Literal, Self
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.errors import ERROR_CLASSIFICATION, ErrorClassification, ErrorCode
from graphx_workers.contracts.results import SourceResult, validate_artifact_refs


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MessageEnvelope[PayloadT](ContractModel):
    message_id: UUID
    type: str = Field(min_length=1)
    schema_version: Literal[1]
    occurred_at: AwareDatetime
    correlation_id: UUID
    causation_id: UUID | None = None
    producer: str = Field(min_length=1)
    root_command_id: UUID
    payload: PayloadT


class SourceQuery(ContractModel):
    type: Literal["person", "organization"]
    name: str | None = Field(default=None, min_length=1, max_length=500)
    identifiers: dict[str, str] | None = None

    @model_validator(mode="after")
    def require_search_key(self) -> Self:
        if self.name is None and "name" in self.model_fields_set:
            raise ValueError("name must be omitted rather than null")
        if self.identifiers is None and "identifiers" in self.model_fields_set:
            raise ValueError("identifiers must be omitted rather than null")
        if self.name is None and not self.identifiers:
            raise ValueError("source query requires name or identifiers")
        if self.identifiers is not None:
            if not self.identifiers:
                raise ValueError("identifiers must not be empty when present")
            if any(not key or not value for key, value in self.identifiers.items()):
                raise ValueError("identifier names and values must be non-empty")
        return self

    @model_serializer(mode="wrap")
    def serialize_without_absent_keys(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, Any]:
        serialized = handler(self)
        if not isinstance(serialized, dict):
            raise TypeError("source query serializer expected an object")
        if self.name is None:
            serialized.pop("name", None)
        if self.identifiers is None:
            serialized.pop("identifiers", None)
        return serialized


class CollectLimits(ContractModel):
    max_records: int = Field(ge=1)
    max_bytes: int = Field(ge=1)
    timeout_seconds: int = Field(ge=1)


class SourceCollectRequestedPayload(ContractModel):
    job_id: UUID
    attempt_no: int = Field(ge=1)
    investigation_id: UUID
    source: str = Field(min_length=1)
    operation: str = Field(min_length=1)
    purpose: Literal["subject_discovery", "enrichment", "branch_expansion"]
    query: SourceQuery
    limits: CollectLimits
    deadline: AwareDatetime


class SourceCollectRequested(MessageEnvelope[SourceCollectRequestedPayload]):
    type: Literal["source.collect.requested.v1"]


class SourceCollectCompleted(MessageEnvelope[SourceResult]):
    type: Literal["source.collect.completed.v1"]


class SourceCollectError(ContractModel):
    code: ErrorCode
    classification: ErrorClassification
    message: str = Field(min_length=1, max_length=2000)
    retry_after_seconds: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_classification(self) -> Self:
        if ERROR_CLASSIFICATION[self.code] != self.classification:
            raise ValueError("error classification does not match error code")
        return self


class SourceCollectFailedPayload(ContractModel):
    job_id: UUID
    attempt_no: int = Field(ge=1)
    adapter_version: str = Field(min_length=1)
    artifacts: list[ArtifactReference]
    error: SourceCollectError

    @model_validator(mode="after")
    def validate_artifacts(self) -> Self:
        validate_artifact_refs(self.artifacts)
        return self


class SourceCollectFailed(MessageEnvelope[SourceCollectFailedPayload]):
    type: Literal["source.collect.failed.v1"]


SourceMessage = SourceCollectRequested | SourceCollectCompleted | SourceCollectFailed


def parse_source_message(value: dict[str, Any]) -> SourceMessage:
    """Select a strict message model by its discriminator."""
    message_type = value.get("type")
    if value.get("schema_version") != 1:
        raise ValueError(f"unsupported source message contract: {message_type!r}")
    if message_type == "source.collect.requested.v1":
        return SourceCollectRequested.model_validate(value)
    if message_type == "source.collect.completed.v1":
        return SourceCollectCompleted.model_validate(value)
    if message_type == "source.collect.failed.v1":
        return SourceCollectFailed.model_validate(value)
    raise ValueError(f"unsupported source message contract: {message_type!r}")
