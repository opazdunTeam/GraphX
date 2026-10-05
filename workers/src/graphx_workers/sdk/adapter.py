"""Minimal adapter protocol; transport and storage stay in the runtime."""

from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.claims import Claim
from graphx_workers.contracts.records import SourceRecord


class CollectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation: str = Field(min_length=1)
    query: dict[str, Any]


class CollectResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    outcome: str
    adapter_version: str
    parser_version: str
    mapping_version: str
    normalization_version: str
    artifacts: list[ArtifactReference] = Field(default_factory=list)
    records: list[SourceRecord] = Field(default_factory=list)
    claims: list[Claim] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class AdapterContext(Protocol):
    async def save_artifact(self, content: bytes, media_type: str) -> ArtifactReference: ...


class SourceAdapter(Protocol):
    async def collect(
        self,
        request: CollectRequest,
        context: AdapterContext,
    ) -> CollectResult: ...
