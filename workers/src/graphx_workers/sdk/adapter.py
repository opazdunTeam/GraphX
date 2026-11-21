"""Stable adapter boundary; transport and infrastructure stay in the runtime."""

import logging
from collections.abc import AsyncIterable, AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.claims import Claim
from graphx_workers.contracts.messages import CollectLimits, SourceQuery
from graphx_workers.contracts.records import SourceRecord
from graphx_workers.contracts.results import validate_result_content


@dataclass(frozen=True, slots=True)
class AdapterMetadata:
    """Immutable source identity and capabilities declared by an adapter."""

    source: str
    version: str
    supported_operations: frozenset[str]

    def __post_init__(self) -> None:
        if not isinstance(self.source, str) or not isinstance(self.version, str):
            raise TypeError("adapter source and version must be strings")
        if not self.source or not self.version:
            raise ValueError("adapter source and version must not be empty")
        if not isinstance(self.supported_operations, frozenset):
            raise TypeError("adapter supported_operations must be a frozenset")
        if any(not isinstance(operation, str) for operation in self.supported_operations):
            raise TypeError("adapter supported operations must be strings")
        if not self.supported_operations or any(
            not operation for operation in self.supported_operations
        ):
            raise ValueError("adapter must declare non-empty supported operations")


class CollectRequest(BaseModel):
    """Source-facing subset of a collection command."""

    model_config = ConfigDict(extra="forbid")

    operation: str = Field(min_length=1)
    purpose: Literal["subject_discovery", "enrichment", "branch_expansion"]
    query: SourceQuery
    limits: CollectLimits
    deadline: AwareDatetime


class CollectResult(BaseModel):
    """Content returned by an adapter before runtime transport packaging."""

    model_config = ConfigDict(extra="forbid")

    outcome: Literal["succeeded", "not_found"]
    adapter_version: str = Field(min_length=1)
    parser_version: str = Field(min_length=1)
    mapping_version: str = Field(min_length=1)
    normalization_version: str = Field(min_length=1)
    artifacts: list[ArtifactReference] = Field(min_length=1)
    records: list[SourceRecord] = Field(default_factory=list)
    claims: list[Claim] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_content(self) -> Self:
        validate_result_content(self.records, self.claims, self.artifacts)
        if self.outcome == "not_found" and (self.records or self.claims):
            raise ValueError("not_found result cannot contain records or claims")
        return self


class HTTPResponse(Protocol):
    status_code: int
    headers: Mapping[str, str]
    content: bytes


class HTTPClient(Protocol):
    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, str] | None = None,
        content: bytes | None = None,
        timeout_seconds: float | None = None,
    ) -> HTTPResponse: ...


class RateLimiter(Protocol):
    async def acquire(self, key: str) -> None: ...


class SecretProvider(Protocol):
    def get(self, name: str) -> str | None: ...


class ProgressReporter(Protocol):
    async def report(
        self, completed: int, total: int | None, message: str | None = None
    ) -> None: ...


class AdapterContext(Protocol):
    """Runtime capabilities exposed to adapters.

    Logger context is for allowlisted scalar operational metadata only. Adapters must
    persist raw responses via ``save_artifact`` and must never log payloads, headers,
    credentials, URLs, full documents, or unnecessary personal data.
    """

    http: HTTPClient
    rate_limiter: RateLimiter
    logger: logging.Logger
    trace_id: str
    job_id: UUID
    attempt_no: int
    correlation_id: UUID
    deadline: datetime
    secrets: SecretProvider
    progress: ProgressReporter

    def is_cancelled(self) -> bool: ...

    async def save_artifact(
        self,
        content: bytes | AsyncIterable[bytes],
        media_type: str,
        *,
        local_ref: str,
        kind: Literal["raw_response", "document", "screenshot", "other"],
        source_uri: str,
        retrieved_at: AwareDatetime,
        published_at: AwareDatetime | None = None,
        http_metadata: dict[str, str] | None = None,
        original_filename: str | None = None,
        parent_local_ref: str | None = None,
    ) -> ArtifactReference: ...

    def download_artifact(self, reference: ArtifactReference) -> AsyncIterator[bytes]: ...


class SourceAdapter(Protocol):
    metadata: AdapterMetadata

    async def collect(
        self,
        request: CollectRequest,
        context: AdapterContext,
    ) -> CollectResult: ...
