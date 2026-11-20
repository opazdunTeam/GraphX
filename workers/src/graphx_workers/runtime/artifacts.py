"""Immutable artifact sessions independent from source-specific adapters."""

import asyncio
import re
import tempfile
import unicodedata
from collections.abc import AsyncIterable, AsyncIterator, Mapping, Sequence
from datetime import datetime
from hashlib import sha256
from pathlib import Path, PurePath
from typing import Any, BinaryIO, Literal, cast
from urllib.parse import quote, unquote
from uuid import UUID

from pydantic import AnyUrl, AwareDatetime, ValidationError

from graphx_workers.contracts.artifacts import (
    SAFE_HTTP_METADATA,
    ArtifactReference,
    artifact_object_key,
)
from graphx_workers.contracts.results import validate_artifact_refs
from graphx_workers.runtime.errors import RuntimeInfrastructureUncertainError
from graphx_workers.runtime.s3 import (
    ImmutableS3Store,
    ObjectIntegrityError,
)

ArtifactKind = Literal["raw_response", "document", "manifest", "screenshot", "other"]
_MEDIA_TYPE = re.compile(
    r"^[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*(?:\s*;[^\r\n]+)?$"
)


class ArtifactPolicyError(ValueError):
    """Artifact content or metadata violates the configured storage policy."""


class ArtifactPersistenceDrainError(RuntimeInfrastructureUncertainError):
    """Artifact persistence did not settle inside the cancellation drain window."""


class ArtifactSession:
    def __init__(
        self,
        store: ImmutableS3Store,
        *,
        job_id: UUID,
        attempt_no: int,
        max_bytes: int,
        spool_max_memory_bytes: int,
        allowed_media_types: frozenset[str],
        spool_directory: Path | None = None,
        persistence_drain_timeout: float = 1.0,
    ) -> None:
        if attempt_no < 1:
            raise ValueError("attempt_no must be positive")
        if persistence_drain_timeout <= 0:
            raise ValueError("persistence_drain_timeout must be positive")
        self._store = store
        self._job_id = job_id
        self._attempt_no = attempt_no
        self._max_bytes = max_bytes
        self._spool_max_memory_bytes = spool_max_memory_bytes
        self._allowed_media_types = allowed_media_types
        self._spool_directory = spool_directory
        self._persistence_drain_timeout = persistence_drain_timeout
        self._artifacts: dict[str, ArtifactReference] = {}
        self._persistence_tasks: set[asyncio.Task[ArtifactReference]] = set()

    @property
    def saved_artifacts(self) -> Sequence[ArtifactReference]:
        return tuple(self._artifacts.values())

    @property
    def prefix(self) -> str:
        return f"artifacts/v1/{self._job_id}/{self._attempt_no}/"

    async def recover(self) -> None:
        recovered: dict[str, ArtifactReference] = {}
        for key in await self._store.list_keys(self.prefix):
            reference = self._reference_from_head(key, await self._store.head(key))
            if key != self._object_key(reference.local_ref):
                raise ObjectIntegrityError("artifact object key does not match its local reference")
            if reference.byte_size > self._max_bytes:
                raise ObjectIntegrityError("recovered artifact exceeds the configured byte limit")
            await self._store.verify_object(key, sha256=reference.sha256, size=reference.byte_size)
            if reference.local_ref in recovered:
                raise ObjectIntegrityError("artifact prefix contains duplicate local references")
            recovered[reference.local_ref] = reference
        try:
            validate_artifact_refs(list(recovered.values()))
        except ValueError as error:
            raise ObjectIntegrityError("recovered artifact lineage is invalid") from error
        self._artifacts = dict(sorted(recovered.items()))

    async def save(
        self,
        content: bytes | AsyncIterable[bytes],
        media_type: str,
        *,
        local_ref: str,
        kind: ArtifactKind,
        source_uri: str,
        retrieved_at: AwareDatetime,
        published_at: AwareDatetime | None = None,
        http_metadata: Mapping[str, str] | None = None,
        original_filename: str | None = None,
        parent_local_ref: str | None = None,
        max_bytes: int | None = None,
    ) -> ArtifactReference:
        self._validate_local_ref(local_ref)
        normalized_media_type = self._validate_media_type(media_type)
        safe_filename = sanitize_original_filename(original_filename)
        safe_http_metadata = sanitize_http_metadata(http_metadata or {})
        if parent_local_ref is not None and parent_local_ref not in self._artifacts:
            raise ArtifactPolicyError("parent_local_ref must refer to an already saved artifact")
        effective_limit = min(self._max_bytes, max_bytes or self._max_bytes)
        if effective_limit < 1:
            raise ArtifactPolicyError("artifact byte limit must be positive")

        directory = str(self._spool_directory) if self._spool_directory is not None else None
        spool = tempfile.SpooledTemporaryFile(  # noqa: SIM115 - ownership transfers to PUT
            max_size=self._spool_max_memory_bytes,
            mode="w+b",
            dir=directory,
        )
        persistence_started = False
        try:
            digest = sha256()
            size = 0
            async for chunk in _content_chunks(content):
                size += len(chunk)
                if size > effective_limit:
                    raise ArtifactPolicyError("artifact exceeds the configured byte limit")
                digest.update(chunk)
                spool.write(chunk)
            checksum = digest.hexdigest()
            key = self._object_key(local_ref)
            try:
                candidate = ArtifactReference(
                    local_ref=local_ref,
                    kind=kind,
                    object_key=key,
                    sha256=checksum,
                    media_type=normalized_media_type,
                    byte_size=size,
                    source_uri=AnyUrl(source_uri),
                    retrieved_at=retrieved_at,
                    published_at=published_at,
                    http_metadata=safe_http_metadata,
                    original_filename=safe_filename,
                    parent_local_ref=parent_local_ref,
                )
            except (TypeError, ValueError, ValidationError) as error:
                raise ArtifactPolicyError(
                    "artifact reference violates the transport contract"
                ) from error
            metadata = _object_metadata(
                local_ref=candidate.local_ref,
                kind=candidate.kind,
                sha256_value=checksum,
                source_uri=str(candidate.source_uri),
                retrieved_at=candidate.retrieved_at,
                published_at=candidate.published_at,
                original_filename=candidate.original_filename,
                parent_local_ref=candidate.parent_local_ref,
                http_metadata=candidate.http_metadata,
            )
            persistence = asyncio.create_task(
                self._persist_spooled_artifact(
                    candidate=candidate,
                    body=cast(BinaryIO, spool),
                    metadata=metadata,
                )
            )
            persistence_started = True
            self._persistence_tasks.add(persistence)
            persistence.add_done_callback(self._persistence_finished)
            try:
                return await asyncio.shield(persistence)
            except asyncio.CancelledError as cancellation:
                # Give a likely committed write a bounded chance to register its
                # authoritative reference. A second cancellation aborts the drain.
                try:
                    async with asyncio.timeout(self._persistence_drain_timeout):
                        await asyncio.shield(persistence)
                except asyncio.CancelledError as repeated:
                    raise ArtifactPersistenceDrainError(
                        "artifact persistence was interrupted during cancellation drain"
                    ) from repeated
                except TimeoutError as timeout:
                    raise ArtifactPersistenceDrainError(
                        "artifact persistence did not settle before drain timeout"
                    ) from timeout
                except RuntimeInfrastructureUncertainError:
                    raise
                except Exception as error:
                    cancellation.add_note(f"artifact persistence ended with {type(error).__name__}")
                raise cancellation
        finally:
            if not persistence_started:
                spool.close()

    def _persistence_finished(self, task: asyncio.Task[ArtifactReference]) -> None:
        self._persistence_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    async def _persist_spooled_artifact(
        self,
        *,
        candidate: ArtifactReference,
        body: BinaryIO,
        metadata: Mapping[str, str],
    ) -> ArtifactReference:
        created = await self._store.put_file_if_absent(
            key=candidate.object_key,
            body=body,
            size=candidate.byte_size,
            sha256=candidate.sha256,
            media_type=candidate.media_type,
            metadata=metadata,
        )
        reference = (
            candidate
            if created
            else self._reference_from_head(
                candidate.object_key,
                await self._store.head_reconciled(candidate.object_key),
            )
        )
        local_ref = candidate.local_ref
        existing = self._artifacts.get(local_ref)
        if existing is not None and existing != reference:
            raise ObjectIntegrityError(
                "saved artifact metadata changed for the same local reference"
            )
        self._artifacts[local_ref] = reference
        return reference

    async def download(self, reference: ArtifactReference) -> AsyncIterator[bytes]:
        registered = self._artifacts.get(reference.local_ref)
        if registered != reference or reference.object_key != self._object_key(reference.local_ref):
            raise ArtifactPolicyError("artifact is not registered in this job attempt session")
        directory = str(self._spool_directory) if self._spool_directory is not None else None
        with tempfile.SpooledTemporaryFile(
            max_size=self._spool_max_memory_bytes,
            mode="w+b",
            dir=directory,
        ) as spool:
            digest = sha256()
            size = 0
            async for chunk in self._store.iter_bytes(reference.object_key):
                digest.update(chunk)
                size += len(chunk)
                if size > reference.byte_size or size > self._max_bytes:
                    raise ObjectIntegrityError("stored artifact is larger than its reference")
                spool.write(chunk)
            if size != reference.byte_size or digest.hexdigest() != reference.sha256:
                raise ObjectIntegrityError("stored artifact does not match its immutable reference")
            spool.seek(0)
            while chunk := spool.read(64 * 1024):
                yield chunk

    def _object_key(self, local_ref: str) -> str:
        return artifact_object_key(self._job_id, self._attempt_no, local_ref)

    def _reference_from_head(self, key: str, head: Mapping[str, Any]) -> ArtifactReference:
        metadata_value = head.get("Metadata")
        if not isinstance(metadata_value, Mapping):
            raise ObjectIntegrityError("artifact object does not contain metadata")
        metadata = {str(name).lower(): str(value) for name, value in metadata_value.items()}
        try:
            http_metadata = {
                name.removeprefix("gx-http-"): unquote(value)
                for name, value in metadata.items()
                if name.startswith("gx-http-")
            }
            return ArtifactReference(
                local_ref=unquote(metadata["gx-local-ref"]),
                kind=cast(ArtifactKind, metadata["gx-kind"]),
                object_key=key,
                sha256=metadata["gx-sha256"],
                media_type=str(head["ContentType"]),
                byte_size=int(head["ContentLength"]),
                source_uri=AnyUrl(unquote(metadata["gx-source-uri"])),
                retrieved_at=datetime.fromisoformat(metadata["gx-retrieved-at"]),
                published_at=(
                    datetime.fromisoformat(metadata["gx-published-at"])
                    if "gx-published-at" in metadata
                    else None
                ),
                http_metadata=http_metadata,
                original_filename=(
                    unquote(metadata["gx-original-filename"])
                    if "gx-original-filename" in metadata
                    else None
                ),
                parent_local_ref=(
                    unquote(metadata["gx-parent-local-ref"])
                    if "gx-parent-local-ref" in metadata
                    else None
                ),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise ObjectIntegrityError("artifact object metadata is invalid") from error

    def _validate_media_type(self, media_type: str) -> str:
        normalized = media_type.strip().lower()
        if len(normalized) > 200 or _MEDIA_TYPE.fullmatch(normalized) is None:
            raise ArtifactPolicyError("artifact media type is invalid")
        base_type = normalized.partition(";")[0].strip()
        if base_type not in self._allowed_media_types:
            raise ArtifactPolicyError("artifact media type is not allowed")
        return normalized

    def _validate_local_ref(self, local_ref: str) -> None:
        if (
            not local_ref
            or len(local_ref) > 200
            or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in local_ref)
            or len(self._object_key(local_ref).encode()) > 1024
        ):
            raise ArtifactPolicyError("artifact local_ref is invalid")


async def _content_chunks(content: bytes | AsyncIterable[bytes]) -> AsyncIterator[bytes]:
    if isinstance(content, bytes):
        yield content
        return
    async for chunk in content:
        if not isinstance(chunk, bytes):
            raise TypeError("artifact stream must yield bytes")
        if chunk:
            yield chunk


def sanitize_original_filename(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = unicodedata.normalize("NFKC", value).replace("\\", "/")
    basename = PurePath(normalized).name
    safe = "".join(character for character in basename if ord(character) >= 32).strip(" .")
    if not safe or safe in {".", ".."}:
        raise ArtifactPolicyError("original filename is empty or unsafe")
    return safe[:255]


def sanitize_http_metadata(values: Mapping[str, str]) -> dict[str, str]:
    sanitized: dict[str, str] = {}
    for raw_name, raw_value in values.items():
        name = raw_name.strip().lower()
        if name not in SAFE_HTTP_METADATA:
            continue
        value = raw_value.strip()
        if not value or len(value) > 512 or any(character in value for character in "\r\n"):
            raise ArtifactPolicyError(f"unsafe HTTP metadata value for {name}")
        sanitized[name] = value
    return sanitized


def _object_metadata(
    *,
    local_ref: str,
    kind: ArtifactKind,
    sha256_value: str,
    source_uri: str,
    retrieved_at: datetime,
    published_at: datetime | None,
    original_filename: str | None,
    parent_local_ref: str | None,
    http_metadata: Mapping[str, str],
) -> dict[str, str]:
    metadata = {
        "gx-local-ref": quote(local_ref, safe=""),
        "gx-kind": kind,
        "gx-sha256": sha256_value,
        "gx-source-uri": quote(source_uri, safe=""),
        "gx-retrieved-at": retrieved_at.isoformat(),
    }
    if published_at is not None:
        metadata["gx-published-at"] = published_at.isoformat()
    if original_filename is not None:
        metadata["gx-original-filename"] = quote(original_filename, safe="")
    if parent_local_ref is not None:
        metadata["gx-parent-local-ref"] = quote(parent_local_ref, safe="")
    for name, value in http_metadata.items():
        metadata[f"gx-http-{name}"] = quote(value, safe="")
    if sum(len(name) + len(value) for name, value in metadata.items()) > 1800:
        raise ArtifactPolicyError("artifact metadata exceeds the safe S3 header budget")
    return metadata
