"""Concrete adapter context backed by immutable artifact storage."""

import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator, Mapping, Sequence
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime

from graphx_workers.config import WorkerSettings
from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.runtime.artifacts import ArtifactSession
from graphx_workers.runtime.processor import RuntimeAdapterContext
from graphx_workers.runtime.s3 import ImmutableS3Store
from graphx_workers.sdk import (
    HTTPClient,
    HTTPResponse,
    ProgressReporter,
    RateLimiter,
    SecretProvider,
)


class UnavailableHTTPClient:
    """Explicit boundary until a networked source supplies the shared HTTP capability."""

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, str] | None = None,
        content: bytes | None = None,
        timeout_seconds: float | None = None,
    ) -> HTTPResponse:
        del method, url, headers, params, content, timeout_seconds
        raise RuntimeError("HTTP capability is not configured for this source runtime")


class NoopRateLimiter:
    async def acquire(self, key: str) -> None:
        del key


class EmptySecretProvider:
    def get(self, name: str) -> str | None:
        del name
        return None


class NoopProgressReporter:
    async def report(
        self,
        completed: int,
        total: int | None,
        message: str | None = None,
    ) -> None:
        del completed, total, message


class ArtifactRuntimeContext:
    def __init__(
        self,
        session: ArtifactSession,
        *,
        job_id: UUID,
        attempt_no: int,
        correlation_id: UUID,
        deadline: datetime,
        logger: logging.Logger,
    ) -> None:
        self._session = session
        self.job_id = job_id
        self.attempt_no = attempt_no
        self.correlation_id = correlation_id
        self.deadline = deadline
        self.trace_id = str(correlation_id)
        self.logger = logger
        self.http: HTTPClient = UnavailableHTTPClient()
        self.rate_limiter: RateLimiter = NoopRateLimiter()
        self.secrets: SecretProvider = EmptySecretProvider()
        self.progress: ProgressReporter = NoopProgressReporter()

    @property
    def saved_artifacts(self) -> Sequence[ArtifactReference]:
        return self._session.saved_artifacts

    def is_cancelled(self) -> bool:
        task = asyncio.current_task()
        return task is not None and task.cancelling() > 0

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
    ) -> ArtifactReference:
        return await self._session.save(
            content,
            media_type,
            local_ref=local_ref,
            kind=kind,
            source_uri=source_uri,
            retrieved_at=retrieved_at,
            published_at=published_at,
            http_metadata=http_metadata,
            original_filename=original_filename,
            parent_local_ref=parent_local_ref,
        )

    async def save_manifest(
        self,
        content: AsyncIterable[bytes],
        *,
        local_ref: str,
        source_uri: str,
        retrieved_at: datetime,
        max_bytes: int,
    ) -> ArtifactReference:
        return await self._session.save(
            content,
            "application/json",
            local_ref=local_ref,
            kind="manifest",
            source_uri=source_uri,
            retrieved_at=retrieved_at,
            original_filename="source-result-manifest.v1.json",
            max_bytes=max_bytes,
        )

    def download_artifact(self, reference: ArtifactReference) -> AsyncIterator[bytes]:
        return self._session.download(reference)


class ArtifactContextFactory:
    def __init__(
        self,
        store: ImmutableS3Store,
        settings: WorkerSettings,
        logger: logging.Logger,
    ) -> None:
        self._store = store
        self._settings = settings
        self._logger = logger

    async def create(
        self,
        *,
        job_id: UUID,
        attempt_no: int,
        correlation_id: UUID,
        deadline: datetime,
    ) -> RuntimeAdapterContext:
        session = ArtifactSession(
            self._store,
            job_id=job_id,
            attempt_no=attempt_no,
            max_bytes=self._settings.artifact_max_bytes,
            spool_max_memory_bytes=self._settings.artifact_spool_max_memory_bytes,
            allowed_media_types=self._settings.allowed_artifact_media_types(),
            spool_directory=self._settings.artifact_spool_directory,
        )
        await session.recover()
        return ArtifactRuntimeContext(
            session,
            job_id=job_id,
            attempt_no=attempt_no,
            correlation_id=correlation_id,
            deadline=deadline,
            logger=self._logger,
        )
