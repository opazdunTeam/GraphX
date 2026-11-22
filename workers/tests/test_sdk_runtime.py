import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import cast
from uuid import UUID, uuid4

import pytest
import rfc8785
from pydantic import AnyUrl, ValidationError

from graphx_workers.contracts.artifacts import ArtifactReference, artifact_object_key
from graphx_workers.contracts.claims import Claim
from graphx_workers.contracts.errors import ERROR_CLASSIFICATION, ErrorClassification, ErrorCode
from graphx_workers.contracts.messages import (
    CollectLimits,
    SourceCollectError,
    SourceCollectRequestedPayload,
    SourceQuery,
)
from graphx_workers.contracts.records import SourceRecord
from graphx_workers.contracts.results import (
    validate_manifest_semantics,
    validate_source_result_semantics,
)
from graphx_workers.runtime.requests import to_collect_request
from graphx_workers.runtime.results import (
    ManifestTooLargeError,
    ResultTooComplexError,
    build_inline_source_result,
    build_manifest_source_result,
    finalize_inline_source_result,
    prepare_inline_source_result,
    prepare_manifest,
    validate_result_serialization_limits,
)
from graphx_workers.sdk import AdapterMetadata, CollectRequest, CollectResult

JOB_ID = UUID("22222222-2222-4222-8222-222222222222")


def test_adapter_metadata_requires_deeply_immutable_operations() -> None:
    mutable_operations = {"collect"}
    with pytest.raises(TypeError, match="must be a frozenset"):
        AdapterMetadata(
            source="fixture",
            version="fixture/1",
            supported_operations=cast(frozenset[str], mutable_operations),
        )

    metadata = AdapterMetadata(
        source="fixture",
        version="fixture/1",
        supported_operations=frozenset({"collect"}),
    )
    with pytest.raises(AttributeError):
        metadata.supported_operations.add("unexpected")  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "operations",
    [
        cast(frozenset[str], "collect"),
        cast(frozenset[str], frozenset({"collect", 1})),
    ],
)
def test_adapter_metadata_rejects_invalid_operation_collections(
    operations: frozenset[str],
) -> None:
    with pytest.raises(TypeError):
        AdapterMetadata(
            source="fixture",
            version="fixture/1",
            supported_operations=operations,
        )


def source_artifact(
    job_id: UUID = JOB_ID, *, local_ref: str = "artifact:raw:1"
) -> ArtifactReference:
    return ArtifactReference(
        local_ref=local_ref,
        kind="raw_response",
        object_key=artifact_object_key(job_id, 1, local_ref),
        sha256="a" * 64,
        media_type="application/json",
        byte_size=2,
        source_uri=AnyUrl("https://example.test/subject/1"),
        retrieved_at=datetime.now(UTC),
        published_at=None,
        http_metadata={},
    )


def collected_result(job_id: UUID = JOB_ID) -> CollectResult:
    return CollectResult(
        outcome="succeeded",
        adapter_version="fixture-adapter/1",
        parser_version="fixture-parser/1",
        mapping_version="fixture-mapper/1",
        normalization_version="normalization/1",
        artifacts=[source_artifact(job_id)],
        records=[
            SourceRecord(
                local_ref="person:1",
                artifact_local_ref="artifact:raw:1",
                record_type="person",
                external_id="1",
                schema_version="fixture/1",
                raw_data={"name": "Example"},
            )
        ],
        claims=[
            Claim(
                claim_kind="attribute",
                subject_local_ref="person:1",
                predicate="name",
                object_local_ref=None,
                raw_value="Example",
                normalized_value="example",
                qualifiers={},
                valid_from=None,
                valid_to=None,
                valid_from_precision="unknown",
                valid_to_precision="unknown",
                temporal_basis="unknown",
                snapshot_at=None,
                evidence_locator={"json_pointer": "/name"},
                evidence_excerpt="Example",
            )
        ],
        warnings=[],
    )


def test_adapter_request_is_independent_from_transport_metadata() -> None:
    request = CollectRequest(
        operation="search_subject",
        purpose="subject_discovery",
        query=SourceQuery(type="person", name="Example"),
        limits=CollectLimits(max_records=10, max_bytes=1024, timeout_seconds=30),
        deadline=datetime.now(UTC) + timedelta(seconds=30),
    )
    assert "job_id" not in request.model_fields_set

    with pytest.raises(ValidationError):
        CollectRequest(**{**request.model_dump(), "job_id": uuid4()})

    payload = SourceCollectRequestedPayload(
        job_id=uuid4(),
        attempt_no=1,
        investigation_id=uuid4(),
        source="fixture",
        operation=request.operation,
        purpose=request.purpose,
        query=request.query,
        limits=request.limits,
        deadline=request.deadline,
    )
    assert to_collect_request(payload) == request


def test_error_catalog_is_shared_with_transport_model() -> None:
    assert "DOCUMENT_UNSUPPORTED" not in {code.value for code in ErrorCode}
    error = SourceCollectError(
        code=ErrorCode.SOURCE_TIMEOUT,
        classification=ERROR_CLASSIFICATION[ErrorCode.SOURCE_TIMEOUT],
        message="source timed out",
    )
    assert error.classification is ErrorClassification.TRANSIENT


def test_runtime_builds_valid_inline_result_and_digest() -> None:
    collected = collected_result()
    result = build_inline_source_result(JOB_ID, 1, collected)
    validate_source_result_semantics(result)
    assert result.record_count == 1
    assert result.manifest is None

    with pytest.raises(ValidationError):
        CollectResult(**{**collected.model_dump(), "manifest": source_artifact()})


def test_inline_finalization_revalidates_mutated_collection() -> None:
    collected = collected_result()
    collected.artifacts.append(collected.artifacts[0].model_copy(deep=True))

    with pytest.raises(ValueError, match="artifact local_ref values must be unique"):
        build_inline_source_result(JOB_ID, 1, collected)


def test_artifact_parent_references_are_resolved_and_acyclic() -> None:
    child = source_artifact(local_ref="artifact:child").model_copy(
        update={"parent_local_ref": "artifact:raw:1"}
    )
    collected = collected_result().model_copy(update={"artifacts": [source_artifact(), child]})
    build_inline_source_result(JOB_ID, 1, collected)

    cyclic_parent = source_artifact().model_copy(update={"parent_local_ref": "artifact:child"})
    cyclic_child = child.model_copy(update={"parent_local_ref": "artifact:raw:1"})
    with pytest.raises(ValueError, match="cycle"):
        CollectResult(**{**collected.model_dump(), "artifacts": [cyclic_parent, cyclic_child]})


@pytest.mark.asyncio
async def test_runtime_owns_manifest_serialization_and_finalization() -> None:
    job_id = uuid4()
    collected = collected_result(job_id)
    prepared = prepare_manifest(job_id, 1, collected, max_bytes=1024 * 1024)
    content = b"".join([chunk async for chunk in prepared.iter_bytes()])
    reference = ArtifactReference(
        local_ref="artifact:manifest:1",
        kind="manifest",
        object_key=artifact_object_key(job_id, 1, "artifact:manifest:1"),
        sha256=sha256(content).hexdigest(),
        media_type="application/json",
        byte_size=len(content),
        source_uri=AnyUrl("https://example.test/subject/1"),
        retrieved_at=datetime.now(UTC),
        published_at=None,
        http_metadata={},
    )

    result = build_manifest_source_result(job_id, 1, collected, prepared, reference)
    validate_source_result_semantics(result)
    validate_manifest_semantics(result, prepared.document, content)
    assert not result.records
    assert not result.claims


@pytest.mark.asyncio
async def test_manifest_finalization_rejects_a_different_collection_with_same_count() -> None:
    job_id = uuid4()
    original = collected_result(job_id)
    prepared = prepare_manifest(job_id, 1, original, max_bytes=1024 * 1024)
    content = b"".join([chunk async for chunk in prepared.iter_bytes()])
    reference = ArtifactReference(
        local_ref="artifact:manifest:1",
        kind="manifest",
        object_key=artifact_object_key(job_id, 1, "artifact:manifest:1"),
        sha256=sha256(content).hexdigest(),
        media_type="application/json",
        byte_size=len(content),
        source_uri=AnyUrl("https://example.test/subject/1"),
        retrieved_at=datetime.now(UTC),
        published_at=None,
        http_metadata={},
    )
    different = original.model_copy(deep=True)
    different.records[0].raw_data["name"] = "Different"

    with pytest.raises(ValueError, match="does not belong"):
        build_manifest_source_result(job_id, 1, different, prepared, reference)

    tampered_reference = reference.model_copy(update={"sha256": "f" * 64})
    with pytest.raises(ValueError, match="sha256"):
        build_manifest_source_result(job_id, 1, original, prepared, tampered_reference)


@pytest.mark.asyncio
async def test_manifest_stream_enforces_its_dedicated_byte_limit() -> None:
    prepared = prepare_manifest(JOB_ID, 1, collected_result(), max_bytes=16)

    with pytest.raises(ManifestTooLargeError, match="configured byte limit"):
        b"".join([chunk async for chunk in prepared.iter_bytes()])


def test_result_serialization_rejects_excessive_aggregate_scalar_content() -> None:
    collected = collected_result()
    collected.records[0].raw_data = {"first": "a" * 64, "second": "b" * 64}

    with pytest.raises(ResultTooComplexError, match="total scalar character limit"):
        validate_result_serialization_limits(
            collected,
            max_scalar_chars=2048,
            max_total_scalar_chars=100,
            max_json_nodes=1000,
        )


def test_jcs_digest_handles_number_longer_than_compact_python_json() -> None:
    collected = collected_result()
    collected.records[0].raw_data["large_number"] = 1e20
    candidate = prepare_inline_source_result(JOB_ID, 1, collected)
    terminal_payload = candidate.model_dump(mode="json")
    compact = json.dumps(
        terminal_payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode()
    digest_payload = dict(terminal_payload)
    digest_payload.pop("digest")
    canonical = rfc8785.dumps(digest_payload)

    assert b"1e+20" in compact
    assert b"100000000000000000000" in canonical

    result = finalize_inline_source_result(candidate, max_bytes=len(compact))
    validate_source_result_semantics(result)
