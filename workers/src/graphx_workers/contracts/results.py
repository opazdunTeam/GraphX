from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.claims import Claim
from graphx_workers.contracts.records import SourceRecord


class SourceResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: UUID
    attempt_no: int = Field(ge=1)
    outcome: Literal["succeeded", "not_found"]
    record_count: int = Field(ge=0)
    adapter_version: str = Field(min_length=1)
    parser_version: str = Field(min_length=1)
    mapping_version: str = Field(min_length=1)
    normalization_version: str = Field(min_length=1)
    digest: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    artifacts: list[ArtifactReference]
    manifest: ArtifactReference | None = None
    records: list[SourceRecord]
    claims: list[Claim]
    warnings: list[str]
