"""Source result transport models and semantic validation."""

import json
from collections.abc import Mapping
from hashlib import sha256
from math import isfinite
from typing import Any, Literal, Self
from uuid import UUID

import rfc8785
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from graphx_workers.contracts.artifacts import ArtifactReference, artifact_object_key
from graphx_workers.contracts.claims import Claim, meaningful_claim_value
from graphx_workers.contracts.records import SourceRecord
from graphx_workers.contracts.schema import validate_raw_locator_numbers, validate_raw_record_count
from graphx_workers.contracts.values import MAX_SAFE_INTEGER, validate_persisted_text


def _validate_record_count(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError("record_count must be an exact non-negative safe integer")
    if isinstance(value, float) and (not isfinite(value) or not value.is_integer()):
        raise ValueError("record_count must be an exact non-negative safe integer")
    if not 0 <= value <= MAX_SAFE_INTEGER:
        raise ValueError("record_count must be an exact non-negative safe integer")
    return int(value)


class SourceResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: UUID
    attempt_no: int = Field(ge=1)
    outcome: Literal["succeeded", "not_found"]
    record_count: int = Field(ge=0, le=MAX_SAFE_INTEGER)
    adapter_version: str = Field(min_length=1)
    parser_version: str = Field(min_length=1)
    mapping_version: str = Field(min_length=1)
    normalization_version: str = Field(min_length=1)
    digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    artifacts: list[ArtifactReference] = Field(min_length=1)
    manifest: ArtifactReference | None
    records: list[SourceRecord]
    claims: list[Claim]
    warnings: list[str]

    @field_validator("record_count", mode="before")
    @classmethod
    def validate_record_count(cls, value: Any) -> int:
        return _validate_record_count(value)

    @classmethod
    def model_validate_json(
        cls,
        json_data: str | bytes | bytearray,
        *,
        strict: bool | None = None,
        extra: Literal["allow", "ignore", "forbid"] | None = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        validate_raw_record_count(json_data)
        validate_raw_locator_numbers(json_data)
        return super().model_validate_json(
            json_data,
            strict=strict,
            extra=extra,
            context=context,
            by_alias=by_alias,
            by_name=by_name,
        )

    @field_validator(
        "adapter_version", "parser_version", "mapping_version", "normalization_version"
    )
    @classmethod
    def validate_versions(cls, value: str) -> str:
        return validate_persisted_text(value, "result versions")

    @model_validator(mode="after")
    def validate_schema_rules(self) -> Self:
        validate_artifact_refs(
            [*self.artifacts, *([self.manifest] if self.manifest is not None else [])],
            job_id=self.job_id,
            attempt_no=self.attempt_no,
        )
        if self.manifest is not None:
            if self.manifest.kind != "manifest" or self.manifest.media_type != "application/json":
                raise ValueError("manifest must be a JSON manifest artifact")
            if self.records or self.claims:
                raise ValueError("manifest mode cannot contain inline records or claims")
        if self.outcome == "not_found" and (
            self.record_count != 0 or self.manifest is not None or self.records or self.claims
        ):
            raise ValueError("not_found result cannot contain result data")
        return self


class SourceResultManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    job_id: UUID
    attempt_no: int = Field(ge=1)
    record_count: int = Field(ge=0, le=MAX_SAFE_INTEGER)
    records: list[SourceRecord]
    claims: list[Claim]

    @field_validator("record_count", mode="before")
    @classmethod
    def validate_record_count(cls, value: Any) -> int:
        return _validate_record_count(value)

    @classmethod
    def model_validate_json(
        cls,
        json_data: str | bytes | bytearray,
        *,
        strict: bool | None = None,
        extra: Literal["allow", "ignore", "forbid"] | None = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        validate_raw_record_count(json_data)
        validate_raw_locator_numbers(json_data)
        return super().model_validate_json(
            json_data,
            strict=strict,
            extra=extra,
            context=context,
            by_alias=by_alias,
            by_name=by_name,
        )


def validate_result_content(
    records: list[SourceRecord], claims: list[Claim], artifacts: list[ArtifactReference]
) -> None:
    _validate_source_artifact_kinds(artifacts)
    known_artifacts = validate_artifact_refs(artifacts)

    record_refs = [record.local_ref for record in records]
    if len(record_refs) != len(set(record_refs)):
        raise ValueError("source record local_ref values must be unique")
    known_records = set(record_refs)

    schema_versions_by_artifact: dict[str, str] = {}

    for record in records:
        if record.artifact_local_ref not in known_artifacts:
            raise ValueError("record artifact_local_ref must resolve to a source artifact")
        existing = schema_versions_by_artifact.setdefault(
            record.artifact_local_ref, record.schema_version
        )
        if existing != record.schema_version:
            raise ValueError("records for one artifact must use one schema_version")
    for claim in claims:
        if claim.subject_local_ref not in known_records:
            raise ValueError("claim subject_local_ref must resolve to a source record")
        if claim.object_local_ref is not None and claim.object_local_ref not in known_records:
            raise ValueError("claim object_local_ref must resolve to a source record")
        if claim.claim_kind == "attribute" and claim.object_local_ref is not None:
            raise ValueError("attribute claims cannot contain object_local_ref")
        requires_value = claim.claim_kind in {"attribute", "event"} and (
            claim.object_local_ref is None
        )
        if requires_value and not any(
            meaningful_claim_value(value) for value in (claim.raw_value, claim.normalized_value)
        ):
            raise ValueError(f"{claim.claim_kind} claim requires a meaningful value")


def validate_artifact_refs(
    artifacts: list[ArtifactReference],
    *,
    job_id: UUID | None = None,
    attempt_no: int | None = None,
) -> set[str]:
    artifact_refs = [artifact.local_ref for artifact in artifacts]
    if len(artifact_refs) != len(set(artifact_refs)):
        raise ValueError("artifact local_ref values must be unique")
    known = set(artifact_refs)
    if job_id is not None and attempt_no is not None:
        for artifact in artifacts:
            if artifact.object_key != artifact_object_key(job_id, attempt_no, artifact.local_ref):
                raise ValueError("artifact object_key does not match job, attempt and local_ref")
    parents = {
        artifact.local_ref: artifact.parent_local_ref
        for artifact in artifacts
        if artifact.parent_local_ref is not None
    }
    for local_ref, parent_local_ref in parents.items():
        if parent_local_ref not in known:
            raise ValueError("artifact parent_local_ref must resolve to an artifact")
        if parent_local_ref == local_ref:
            raise ValueError("artifact cannot be its own parent")
        visited = {local_ref}
        current: str | None = parent_local_ref
        while current is not None:
            if current in visited:
                raise ValueError("artifact parent references cannot contain a cycle")
            visited.add(current)
            current = parents.get(current)
    return known


def compute_source_result_digest(result: SourceResult | Mapping[str, Any]) -> str:
    payload = result.model_dump(mode="json") if isinstance(result, SourceResult) else dict(result)
    payload.pop("digest", None)
    return sha256(rfc8785.dumps(payload)).hexdigest()


def validate_source_result_semantics(
    result: SourceResult, raw_payload: Mapping[str, Any] | bytes | None = None
) -> None:
    digest_payload: SourceResult | Mapping[str, Any]
    if isinstance(raw_payload, bytes):
        validate_raw_record_count(raw_payload)
        validate_raw_locator_numbers(raw_payload)
        decoded = json.loads(raw_payload)
        if not isinstance(decoded, dict):
            raise ValueError("source result payload must be a JSON object")
        digest_payload = decoded
    else:
        digest_payload = result if raw_payload is None else raw_payload
    if compute_source_result_digest(digest_payload) != result.digest:
        raise ValueError("source result digest does not match RFC 8785 canonical payload")
    artifact_refs = validate_artifact_refs(
        result.artifacts, job_id=result.job_id, attempt_no=result.attempt_no
    )
    _validate_source_artifact_kinds(result.artifacts)
    if result.manifest is not None:
        if result.manifest.local_ref in artifact_refs:
            raise ValueError("manifest and source artifact local_ref values must be unique")
        validate_artifact_refs(
            [*result.artifacts, result.manifest],
            job_id=result.job_id,
            attempt_no=result.attempt_no,
        )
    if result.manifest is None:
        if result.record_count != len(result.records):
            raise ValueError("inline record_count must equal the number of records")
        validate_result_content(result.records, result.claims, result.artifacts)


def _validate_source_artifact_kinds(artifacts: list[ArtifactReference]) -> None:
    if any(artifact.kind == "manifest" for artifact in artifacts):
        raise ValueError("source artifacts cannot use manifest kind")


def validate_manifest_semantics(
    result: SourceResult, manifest: SourceResultManifest, stored_bytes: bytes
) -> None:
    validate_raw_record_count(stored_bytes)
    validate_raw_locator_numbers(stored_bytes)
    validate_manifest_reference_semantics(
        result,
        manifest,
        byte_size=len(stored_bytes),
        sha256_value=sha256(stored_bytes).hexdigest(),
    )


def validate_manifest_reference_semantics(
    result: SourceResult,
    manifest: SourceResultManifest,
    *,
    byte_size: int,
    sha256_value: str,
) -> None:
    """Validate manifest metadata when trusted storage computed size and digest."""
    reference = result.manifest
    if reference is None:
        raise ValueError("manifest validation requires a manifest result")
    if reference.byte_size != byte_size or reference.sha256 != sha256_value:
        raise ValueError("manifest hash or byte size does not match stored bytes")
    if (result.job_id, result.attempt_no, result.record_count) != (
        manifest.job_id,
        manifest.attempt_no,
        manifest.record_count,
    ):
        raise ValueError("manifest metadata does not match completed result")
    if manifest.record_count != len(manifest.records):
        raise ValueError("manifest record_count must equal the number of records")
    validate_result_content(manifest.records, manifest.claims, result.artifacts)
