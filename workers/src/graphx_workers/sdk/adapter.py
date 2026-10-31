"""Minimal adapter protocol; transport and storage stay in the runtime."""

from datetime import datetime
from typing import Any, Literal, Protocol, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.claims import Claim
from graphx_workers.contracts.records import SourceRecord


class CollectLimits(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_records: int | None = Field(default=None, ge=1)
    max_bytes: int | None = Field(default=None, ge=1)
    timeout_seconds: int | None = Field(default=None, ge=1)


class CollectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation: str = Field(min_length=1)
    purpose: Literal["subject_discovery", "enrichment", "branch_expansion", "reprocessing"]
    query: dict[str, Any]
    limits: CollectLimits = Field(default_factory=CollectLimits)
    deadline: datetime


class CollectResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    outcome: Literal["succeeded", "not_found"]
    record_count: int = Field(ge=0)
    adapter_version: str = Field(min_length=1)
    parser_version: str = Field(min_length=1)
    mapping_version: str = Field(min_length=1)
    normalization_version: str = Field(min_length=1)
    artifacts: list[ArtifactReference] = Field(default_factory=list)
    manifest: ArtifactReference | None = None
    records: list[SourceRecord] = Field(default_factory=list)
    claims: list[Claim] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_result_mode(self) -> Self:
        if not self.artifacts and self.manifest is None:
            raise ValueError("result requires at least one source artifact or manifest")
        if self.manifest is not None:
            if self.manifest.kind != "manifest":
                raise ValueError("manifest reference must have kind=manifest")
            if self.records or self.claims:
                raise ValueError("manifest mode cannot contain inline records or claims")
        elif self.record_count != len(self.records):
            raise ValueError("inline record_count must equal the number of records")
        else:
            refs = [record.local_ref for record in self.records]
            if len(refs) != len(set(refs)):
                raise ValueError("source record local_ref values must be unique")
            known_refs = set(refs)
            for claim in self.claims:
                if claim.subject_local_ref not in known_refs:
                    raise ValueError("claim subject_local_ref must reference an inline record")
                if claim.object_local_ref is not None and claim.object_local_ref not in known_refs:
                    raise ValueError("claim object_local_ref must reference an inline record")
        if self.outcome == "not_found" and (self.record_count != 0 or self.records or self.claims):
            raise ValueError("not_found result cannot contain records or claims")
        return self


class AdapterContext(Protocol):
    async def save_artifact(
        self,
        content: bytes,
        media_type: str,
        *,
        kind: Literal["raw_response", "document", "manifest", "screenshot", "other"],
        source_uri: str,
        retrieved_at: datetime,
        published_at: datetime | None = None,
        http_metadata: dict[str, str] | None = None,
    ) -> ArtifactReference: ...


class SourceAdapter(Protocol):
    async def collect(
        self,
        request: CollectRequest,
        context: AdapterContext,
    ) -> CollectResult: ...
