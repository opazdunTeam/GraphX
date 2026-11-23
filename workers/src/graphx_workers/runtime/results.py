"""Runtime-owned packaging of adapter content into transport results."""

import asyncio
import json
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.results import (
    SourceResult,
    SourceResultManifest,
    compute_source_result_digest,
    validate_manifest_reference_semantics,
    validate_source_result_semantics,
)
from graphx_workers.sdk.adapter import CollectResult


class ResultSerializationLimitError(ValueError):
    """A result exceeds a bounded serialization policy."""


class ManifestTooLargeError(ResultSerializationLimitError):
    """A result manifest exceeds its dedicated bounded-storage policy."""


class ResultTooComplexError(ResultSerializationLimitError):
    """A result contains too many nodes or an oversized scalar."""


class TerminalResultTooLargeError(ResultSerializationLimitError):
    """A terminal SourceResult cannot fit its bounded in-memory payload policy."""


@dataclass(slots=True)
class PreparedManifest:
    document: SourceResultManifest
    _payload: Mapping[str, Any]
    _max_bytes: int
    _byte_size: int | None = None
    _sha256: str | None = None
    _consumed: bool = False

    @property
    def schema_payload(self) -> Mapping[str, Any]:
        if self._consumed:
            raise RuntimeError("manifest payload is no longer available")
        return self._payload

    @property
    def byte_size(self) -> int:
        if self._byte_size is None:
            raise RuntimeError("manifest serialization has not completed")
        return self._byte_size

    @property
    def sha256(self) -> str:
        if self._sha256 is None:
            raise RuntimeError("manifest serialization has not completed")
        return self._sha256

    async def iter_bytes(self) -> AsyncIterator[bytes]:
        if self._consumed:
            raise RuntimeError("manifest serialization is one-shot")
        self._consumed = True
        digest = sha256()
        size = 0
        buffer = bytearray()
        encoder = json.JSONEncoder(
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        try:
            for token in encoder.iterencode(self._payload):
                encoded = token.encode("utf-8")
                if size + len(buffer) + len(encoded) > self._max_bytes:
                    raise ManifestTooLargeError("result manifest exceeds its configured byte limit")
                buffer.extend(encoded)
                while len(buffer) >= 64 * 1024:
                    chunk = bytes(buffer[: 64 * 1024])
                    del buffer[: 64 * 1024]
                    size += len(chunk)
                    digest.update(chunk)
                    yield chunk
                    await asyncio.sleep(0)
            if buffer:
                chunk = bytes(buffer)
                size += len(chunk)
                digest.update(chunk)
                yield chunk
            self._byte_size = size
            self._sha256 = digest.hexdigest()
        finally:
            self._payload = {}


def prepare_manifest(
    job_id: UUID,
    attempt_no: int,
    collected: CollectResult,
    *,
    max_bytes: int,
) -> PreparedManifest:
    """Prepare a schema-compatible, one-shot streamed manifest."""
    if max_bytes < 1:
        raise ValueError("manifest byte limit must be positive")
    collected = revalidate_collect_result(collected)
    document = SourceResultManifest(
        schema_version=1,
        job_id=job_id,
        attempt_no=attempt_no,
        record_count=len(collected.records),
        records=collected.records,
        claims=collected.claims,
    )
    return PreparedManifest(
        document=document,
        _payload=document.model_dump(mode="json"),
        _max_bytes=max_bytes,
    )


def validate_result_serialization_limits(
    value: Any,
    *,
    max_scalar_chars: int,
    max_total_scalar_chars: int,
    max_json_nodes: int,
    max_depth: int = 64,
) -> None:
    """Reject pathological JSON shapes before model_dump or token encoding allocates copies."""
    nodes = 0
    scalar_chars = 0

    def visit(item: Any, depth: int, active: set[int]) -> None:
        nonlocal nodes, scalar_chars
        nodes += 1
        if nodes > max_json_nodes:
            raise ResultTooComplexError("source result exceeds its JSON node limit")
        if depth > max_depth:
            raise ResultTooComplexError("source result exceeds its JSON nesting limit")
        if isinstance(item, str):
            if len(item) > max_scalar_chars:
                raise ResultTooComplexError("source result contains an oversized string scalar")
            scalar_chars += len(item)
            if scalar_chars > max_total_scalar_chars:
                raise ResultTooComplexError(
                    "source result exceeds its total scalar character limit"
                )
            return
        if isinstance(item, (bytes, bytearray)):
            if len(item) > max_scalar_chars:
                raise ResultTooComplexError("source result contains an oversized byte scalar")
            scalar_chars += len(item)
            if scalar_chars > max_total_scalar_chars:
                raise ResultTooComplexError(
                    "source result exceeds its total scalar character limit"
                )
            return

        children: Iterable[Any] | None = None
        if isinstance(item, BaseModel):
            children = (
                child for name in type(item).model_fields for child in (name, getattr(item, name))
            )
        elif isinstance(item, Mapping):
            children = (value for key, child in item.items() for value in (str(key), child))
        elif isinstance(item, (list, tuple, set, frozenset)):
            children = iter(item)
        if children is None:
            return

        identity = id(item)
        if identity in active:
            raise ResultTooComplexError("source result JSON graph contains a cycle")
        active.add(identity)
        try:
            for child in children:
                visit(child, depth + 1, active)
        finally:
            active.remove(identity)

    visit(value, 0, set())


def compact_json_fits(document: Mapping[str, Any], max_bytes: int) -> bool:
    size = 0
    encoder = json.JSONEncoder(
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    for token in encoder.iterencode(document):
        size += len(token.encode("utf-8"))
        if size > max_bytes:
            return False
    return True


def build_inline_source_result(
    job_id: UUID, attempt_no: int, collected: CollectResult
) -> SourceResult:
    return finalize_inline_source_result(
        prepare_inline_source_result(job_id, attempt_no, collected)
    )


def prepare_inline_source_result(
    job_id: UUID,
    attempt_no: int,
    collected: CollectResult,
) -> SourceResult:
    """Build a size-stable inline candidate without materializing its JCS digest input."""
    collected = revalidate_collect_result(collected)
    return _build_source_result(job_id, attempt_no, collected, manifest=None)


def revalidate_collect_result(collected: CollectResult) -> CollectResult:
    # Read fields directly: model_dump warns with raw invalid values and may coerce them
    # before validators can reject non-JSON objects or colliding object keys.
    payload = _model_field_values(collected)
    for field in ("artifacts", "records", "claims"):
        values = payload.get(field)
        if isinstance(values, list):
            payload[field] = [
                _model_field_values(value) if isinstance(value, BaseModel) else value
                for value in values
            ]
    python_validated = CollectResult.model_validate(payload, strict=True)
    return CollectResult.model_validate(python_validated.model_dump(mode="json"))


def reconcile_source_artifacts(
    collected: CollectResult,
    saved_artifacts: Sequence[ArtifactReference],
) -> CollectResult:
    """Replace adapter-owned references with exact authoritative runtime snapshots."""
    authoritative = [
        ArtifactReference.model_validate(_model_field_values(reference), strict=True)
        for reference in saved_artifacts
    ]
    source_artifacts = sorted(
        (reference for reference in authoritative if reference.kind != "manifest"),
        key=lambda reference: reference.local_ref,
    )
    saved_by_ref = {reference.local_ref: reference for reference in source_artifacts}
    if len(saved_by_ref) != len(source_artifacts):
        raise ValueError("artifact session contains duplicate source local_ref values")
    for artifact in collected.artifacts:
        validated = ArtifactReference.model_validate(_model_field_values(artifact), strict=True)
        expected = saved_by_ref.get(validated.local_ref)
        if expected is None or expected.model_dump(mode="json") != validated.model_dump(
            mode="json"
        ):
            raise ValueError("adapter returned an artifact not registered by its runtime context")
    return collected.model_copy(
        update={"artifacts": [reference.model_copy(deep=True) for reference in source_artifacts]}
    )


def _model_field_values(model: BaseModel) -> dict[str, Any]:
    return {name: getattr(model, name) for name in type(model).model_fields}


def finalize_inline_source_result(
    result: SourceResult,
    *,
    max_bytes: int | None = None,
) -> SourceResult:
    if result.manifest is not None:
        raise ValueError("inline finalization cannot accept a manifest result")
    if max_bytes is not None and not compact_json_fits(result.model_dump(mode="json"), max_bytes):
        raise TerminalResultTooLargeError("inline source result exceeds its payload limit")
    result = result.model_copy(update={"digest": compute_source_result_digest(result)})
    validate_source_result_semantics(result)
    return result


def build_manifest_source_result(
    job_id: UUID,
    attempt_no: int,
    collected: CollectResult,
    prepared: PreparedManifest,
    reference: ArtifactReference,
    *,
    terminal_max_bytes: int | None = None,
) -> SourceResult:
    """Finalize a result after the prepared manifest has been stored."""
    document = prepared.document
    if (
        document.job_id != job_id
        or document.attempt_no != attempt_no
        or document.record_count != len(collected.records)
        or document.records != collected.records
        or document.claims != collected.claims
    ):
        raise ValueError("prepared manifest does not belong to this collection result")
    if reference.kind != "manifest" or reference.media_type != "application/json":
        raise ValueError("manifest reference must describe a JSON manifest")
    if reference.byte_size != prepared.byte_size:
        raise ValueError("manifest reference byte_size does not match prepared content")
    if reference.sha256 != prepared.sha256:
        raise ValueError("manifest reference sha256 does not match prepared content")
    result = _build_source_result(job_id, attempt_no, collected, manifest=reference)
    if terminal_max_bytes is not None and not compact_json_fits(
        result.model_dump(mode="json"), terminal_max_bytes
    ):
        raise TerminalResultTooLargeError("manifest source result exceeds its payload limit")
    result = result.model_copy(update={"digest": compute_source_result_digest(result)})
    validate_source_result_semantics(result)
    validate_manifest_reference_semantics(
        result,
        prepared.document,
        byte_size=prepared.byte_size,
        sha256_value=prepared.sha256,
    )
    return result


def _build_source_result(
    job_id: UUID,
    attempt_no: int,
    collected: CollectResult,
    *,
    manifest: ArtifactReference | None,
) -> SourceResult:
    result = SourceResult(
        job_id=job_id,
        attempt_no=attempt_no,
        outcome=collected.outcome,
        record_count=len(collected.records),
        adapter_version=collected.adapter_version,
        parser_version=collected.parser_version,
        mapping_version=collected.mapping_version,
        normalization_version=collected.normalization_version,
        digest="0" * 64,
        artifacts=collected.artifacts,
        manifest=manifest,
        records=[] if manifest is not None else collected.records,
        claims=[] if manifest is not None else collected.claims,
        warnings=collected.warnings,
    )
    return result
