"""Runtime-owned packaging of adapter content into transport results."""

from dataclasses import dataclass
from hashlib import sha256
from uuid import UUID

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.results import (
    SourceResult,
    SourceResultManifest,
    compute_source_result_digest,
    validate_manifest_semantics,
    validate_source_result_semantics,
)
from graphx_workers.sdk.adapter import CollectResult


@dataclass(frozen=True, slots=True)
class PreparedManifest:
    document: SourceResultManifest
    content: bytes


def prepare_manifest(job_id: UUID, attempt_no: int, collected: CollectResult) -> PreparedManifest:
    """Serialize the exact manifest bytes that the runtime must store."""
    document = SourceResultManifest(
        schema_version=1,
        job_id=job_id,
        attempt_no=attempt_no,
        record_count=len(collected.records),
        records=collected.records,
        claims=collected.claims,
    )
    return PreparedManifest(document=document, content=document.model_dump_json().encode("utf-8"))


def build_inline_source_result(
    job_id: UUID, attempt_no: int, collected: CollectResult
) -> SourceResult:
    result = _build_source_result(job_id, attempt_no, collected, manifest=None)
    validate_source_result_semantics(result)
    return result


def build_manifest_source_result(
    job_id: UUID,
    attempt_no: int,
    collected: CollectResult,
    prepared: PreparedManifest,
    reference: ArtifactReference,
) -> SourceResult:
    """Finalize a result after the prepared manifest has been stored."""
    expected = prepare_manifest(job_id, attempt_no, collected)
    if prepared.document != expected.document or prepared.content != expected.content:
        raise ValueError("prepared manifest does not belong to this collection result")
    if reference.kind != "manifest" or reference.media_type != "application/json":
        raise ValueError("manifest reference must describe a JSON manifest")
    if reference.byte_size != len(prepared.content):
        raise ValueError("manifest reference byte_size does not match prepared content")
    if reference.sha256 != sha256(prepared.content).hexdigest():
        raise ValueError("manifest reference sha256 does not match prepared content")
    result = _build_source_result(job_id, attempt_no, collected, manifest=reference)
    validate_source_result_semantics(result)
    validate_manifest_semantics(result, prepared.document, prepared.content)
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
    return result.model_copy(update={"digest": compute_source_result_digest(result)})
