"""Minimal adapter protocol; transport and storage stay in the runtime."""

from datetime import datetime
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.claims import Claim
from graphx_workers.contracts.records import SourceRecord


class CollectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation: str = Field(min_length=1)
    purpose: Literal["subject_discovery", "enrichment", "branch_expansion", "reprocessing"]
    query: dict[str, Any]
    limits: dict[str, int] = Field(default_factory=dict)
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
