from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import uuid4

import pytest
from pydantic import AnyUrl, ValidationError

from graphx_workers.contracts.artifacts import ArtifactReference
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
    build_inline_source_result,
    build_manifest_source_result,
    prepare_manifest,
)
from graphx_workers.sdk import CollectRequest, CollectResult


def source_artifact() -> ArtifactReference:
    return ArtifactReference(
        local_ref="artifact:raw:1",
        kind="raw_response",
        object_key="fixture/response.json",
        sha256="a" * 64,
        media_type="application/json",
        byte_size=2,
        source_uri=AnyUrl("https://example.test/subject/1"),
        retrieved_at=datetime.now(UTC),
        published_at=None,
        http_metadata={},
    )


def collected_result() -> CollectResult:
    return CollectResult(
        outcome="succeeded",
        adapter_version="fixture-adapter/1",
        parser_version="fixture-parser/1",
        mapping_version="fixture-mapper/1",
        normalization_version="normalization/1",
        artifacts=[source_artifact()],
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
    result = build_inline_source_result(uuid4(), 1, collected)
    validate_source_result_semantics(result)
    assert result.record_count == 1
    assert result.manifest is None

    with pytest.raises(ValidationError):
        CollectResult(**{**collected.model_dump(), "manifest": source_artifact()})


def test_inline_finalization_revalidates_mutated_collection() -> None:
    collected = collected_result()
    collected.artifacts.append(collected.artifacts[0].model_copy(deep=True))

    with pytest.raises(ValueError, match="artifact local_ref values must be unique"):
        build_inline_source_result(uuid4(), 1, collected)


def test_runtime_owns_manifest_serialization_and_finalization() -> None:
    job_id = uuid4()
    collected = collected_result()
    prepared = prepare_manifest(job_id, 1, collected)
    reference = ArtifactReference(
        local_ref="artifact:manifest:1",
        kind="manifest",
        object_key="fixture/result-manifest.json",
        sha256=sha256(prepared.content).hexdigest(),
        media_type="application/json",
        byte_size=len(prepared.content),
        source_uri=AnyUrl("https://example.test/subject/1"),
        retrieved_at=datetime.now(UTC),
        published_at=None,
        http_metadata={},
    )

    result = build_manifest_source_result(job_id, 1, collected, prepared, reference)
    validate_source_result_semantics(result)
    validate_manifest_semantics(result, prepared.document, prepared.content)
    assert not result.records
    assert not result.claims


def test_manifest_finalization_rejects_a_different_collection_with_same_count() -> None:
    job_id = uuid4()
    original = collected_result()
    prepared = prepare_manifest(job_id, 1, original)
    reference = ArtifactReference(
        local_ref="artifact:manifest:1",
        kind="manifest",
        object_key="fixture/result-manifest.json",
        sha256=sha256(prepared.content).hexdigest(),
        media_type="application/json",
        byte_size=len(prepared.content),
        source_uri=AnyUrl("https://example.test/subject/1"),
        retrieved_at=datetime.now(UTC),
        published_at=None,
        http_metadata={},
    )
    different = original.model_copy(deep=True)
    different.records[0].raw_data["name"] = "Different"

    with pytest.raises(ValueError, match="does not belong"):
        build_manifest_source_result(job_id, 1, different, prepared, reference)

    tampered = prepared.__class__(document=prepared.document, content=prepared.content + b" ")
    with pytest.raises(ValueError, match="does not belong"):
        build_manifest_source_result(job_id, 1, original, tampered, reference)
