"""Source result transport models and semantic validation."""

import json
from collections.abc import Mapping
from hashlib import sha256
from typing import Any, Literal, Self
from uuid import UUID

import rfc8785
from pydantic import BaseModel, ConfigDict, Field, model_validator

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
    digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    artifacts: list[ArtifactReference] = Field(min_length=1)
    manifest: ArtifactReference | None
    records: list[SourceRecord]
    claims: list[Claim]
    warnings: list[str]

    @model_validator(mode="after")
    def validate_schema_rules(self) -> Self:
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
    record_count: int = Field(ge=0)
    records: list[SourceRecord]
    claims: list[Claim]


def _meaningful(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return bool(value)
    return True


def validate_result_content(
    records: list[SourceRecord], claims: list[Claim], artifacts: list[ArtifactReference]
) -> None:
    known_artifacts = validate_artifact_refs(artifacts)

    record_refs = [record.local_ref for record in records]
    if len(record_refs) != len(set(record_refs)):
        raise ValueError("source record local_ref values must be unique")
    known_records = set(record_refs)

    for record in records:
        if record.artifact_local_ref not in known_artifacts:
            raise ValueError("record artifact_local_ref must resolve to a source artifact")
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
            _meaningful(value) for value in (claim.raw_value, claim.normalized_value)
        ):
            raise ValueError(f"{claim.claim_kind} claim requires a meaningful value")


def validate_artifact_refs(artifacts: list[ArtifactReference]) -> set[str]:
    artifact_refs = [artifact.local_ref for artifact in artifacts]
    if len(artifact_refs) != len(set(artifact_refs)):
        raise ValueError("artifact local_ref values must be unique")
    return set(artifact_refs)


def compute_source_result_digest(result: SourceResult | Mapping[str, Any]) -> str:
    payload = result.model_dump(mode="json") if isinstance(result, SourceResult) else dict(result)
    payload.pop("digest", None)
    return sha256(rfc8785.dumps(payload)).hexdigest()


def validate_source_result_semantics(
    result: SourceResult, raw_payload: Mapping[str, Any] | bytes | None = None
) -> None:
    digest_payload: SourceResult | Mapping[str, Any]
    if isinstance(raw_payload, bytes):
        decoded = json.loads(raw_payload)
        if not isinstance(decoded, dict):
            raise ValueError("source result payload must be a JSON object")
        digest_payload = decoded
    else:
        digest_payload = result if raw_payload is None else raw_payload
    if compute_source_result_digest(digest_payload) != result.digest:
        raise ValueError("source result digest does not match RFC 8785 canonical payload")
    artifact_refs = validate_artifact_refs(result.artifacts)
    if result.manifest is not None and result.manifest.local_ref in artifact_refs:
        raise ValueError("manifest and source artifact local_ref values must be unique")
    if result.manifest is None:
        if result.record_count != len(result.records):
            raise ValueError("inline record_count must equal the number of records")
        validate_result_content(result.records, result.claims, result.artifacts)


def validate_manifest_semantics(
    result: SourceResult, manifest: SourceResultManifest, stored_bytes: bytes
) -> None:
    reference = result.manifest
    if reference is None:
        raise ValueError("manifest validation requires a manifest result")
    hash_mismatch = reference.sha256 != sha256(stored_bytes).hexdigest()
    if reference.byte_size != len(stored_bytes) or hash_mismatch:
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
