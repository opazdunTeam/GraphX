import copy
import json
from hashlib import sha256
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pytest
from jsonschema import ValidationError as SchemaValidationError
from pydantic import ValidationError

from graphx_workers.contracts.claims import Claim
from graphx_workers.contracts.errors import ERROR_CLASSIFICATION, ErrorCode
from graphx_workers.contracts.messages import (
    SourceCollectCompleted,
    SourceQuery,
    parse_source_message,
)
from graphx_workers.contracts.results import (
    SourceResultManifest,
    compute_source_result_digest,
    validate_manifest_semantics,
    validate_result_content,
    validate_source_result_semantics,
)
from graphx_workers.contracts.schema import ContractSchemaRegistry, decode_contract_json

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "contracts" / "examples"


def load_json(relative_path: str) -> dict[str, Any]:
    return cast(
        dict[str, Any], json.loads((EXAMPLES / relative_path).read_text(encoding="utf-8"))
    )


@pytest.fixture(scope="module")
def registry() -> ContractSchemaRegistry:
    return ContractSchemaRegistry(ROOT / "contracts" / "schemas")


@pytest.mark.parametrize(
    "relative_path",
    [
        "messages/source-collect-requested.v1.json",
        "messages/source-collect-completed.v1.json",
        "messages/source-collect-completed-manifest.v1.json",
        "messages/source-collect-failed.v1.json",
    ],
)
def test_shared_message_fixtures_match_schema_and_typed_models(
    registry: ContractSchemaRegistry, relative_path: str
) -> None:
    document = registry.validate_message_json((EXAMPLES / relative_path).read_bytes())

    message = parse_source_message(document)
    serialized = message.model_dump(mode="json", exclude_unset=False)
    registry.validate_message(serialized)


@pytest.mark.parametrize(
    "query",
    [
        {"type": "person", "name": "Иван Иванов"},
        {"type": "person", "identifiers": {"tax_id": "1234567890"}},
    ],
    ids=["name-only", "identifier-only"],
)
def test_source_query_round_trip_omits_absent_optional_keys(
    registry: ContractSchemaRegistry, query: dict[str, Any]
) -> None:
    document = load_json("messages/source-collect-requested.v1.json")
    document["payload"]["query"] = query
    registry.validate_message(document)

    message = parse_source_message(document)
    serialized = message.model_dump(mode="json")

    assert serialized["payload"]["query"] == query
    registry.validate_message(serialized)


def test_completed_fixtures_pass_semantic_validation(registry: ContractSchemaRegistry) -> None:
    inline_document = load_json("messages/source-collect-completed.v1.json")
    inline = SourceCollectCompleted.model_validate(inline_document)
    validate_source_result_semantics(inline.payload, inline_document["payload"])

    manifest_message_document = load_json("messages/source-collect-completed-manifest.v1.json")
    manifest_message = SourceCollectCompleted.model_validate(manifest_message_document)
    stored_bytes = (EXAMPLES / "ingestion/source-result-manifest.v1.json").read_bytes()
    manifest_document = registry.validate_manifest_json(stored_bytes)
    manifest = SourceResultManifest.model_validate(manifest_document)
    validate_source_result_semantics(
        manifest_message.payload, manifest_message_document["payload"]
    )
    validate_manifest_semantics(manifest_message.payload, manifest, stored_bytes)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("type", "source.collect.unknown.v1"),
        ("schema_version", 2),
        ("message_id", "not-a-uuid"),
        ("occurred_at", "not-a-date-time"),
    ],
)
def test_schema_boundary_rejects_unknown_or_malformed_messages(
    registry: ContractSchemaRegistry, field: str, value: Any
) -> None:
    document = load_json("messages/source-collect-requested.v1.json")
    document[field] = value
    with pytest.raises((ValueError, SchemaValidationError)):
        registry.validate_message(document)


def test_models_reject_stale_value_and_bad_error_mapping() -> None:
    completed = load_json("messages/source-collect-completed.v1.json")
    completed["payload"]["claims"][0]["value"] = "legacy"
    with pytest.raises(ValidationError):
        parse_source_message(completed)

    failed = load_json("messages/source-collect-failed.v1.json")
    failed["payload"]["error"]["classification"] = "permanent"
    with pytest.raises(ValidationError):
        parse_source_message(failed)


def test_failed_result_rejects_duplicate_artifact_refs() -> None:
    failed = load_json("messages/source-collect-failed.v1.json")
    artifact = load_json("messages/source-collect-completed.v1.json")["payload"]["artifacts"][0]
    failed["payload"]["artifacts"] = [artifact, copy.deepcopy(artifact)]

    with pytest.raises(ValidationError, match="artifact local_ref values must be unique"):
        parse_source_message(failed)


def test_sdk_error_catalog_matches_failed_message_schema() -> None:
    schema = json.loads(
        (ROOT / "contracts/schemas/messages/source-collect-failed.v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    schema_codes = schema["$defs"]["payload"]["properties"]["error"]["properties"]["code"]["enum"]
    assert {code.value for code in ErrorCode} == set(schema_codes)
    assert set(ERROR_CLASSIFICATION) == set(ErrorCode)


def test_models_reject_empty_identifiers_and_invalid_temporal_forms() -> None:
    with pytest.raises(ValidationError):
        SourceQuery(type="person", name="Example", identifiers={})

    claim = load_json("messages/source-collect-completed.v1.json")["payload"]["claims"][0]
    claim["valid_from"] = "2020-05-15"
    claim["valid_from_precision"] = "month"
    claim["temporal_basis"] = "explicit"
    with pytest.raises(ValidationError, match="first day"):
        Claim.model_validate(claim)

    claim["valid_from"] = None
    claim["valid_from_precision"] = "unknown"
    claim["temporal_basis"] = "snapshot"
    claim["snapshot_at"] = None
    with pytest.raises(ValidationError, match="snapshot_at"):
        Claim.model_validate(claim)

    with pytest.raises(ValidationError, match="omitted rather than null"):
        SourceQuery(type="person", name="Example", identifiers=None)


@pytest.mark.parametrize(
    ("valid_from", "from_precision", "valid_to", "to_precision", "valid"),
    [
        ("2020-01-01", "day", "2020-01-02", "day", True),
        ("2020-01-01", "day", "2020-01-01", "day", False),
        ("2020-01-01", "year", "2020-01-01", "year", True),
        ("2020-05-01", "month", "2020-05-01", "month", True),
        ("2021-01-01", "year", "2020-01-01", "year", False),
        ("2021-02-01", "month", "2021-01-01", "month", False),
        ("2021-01-01", "year", "2021-01-01", "day", False),
        ("2021-01-01", "approximate", "1900-01-01", "approximate", True),
        ("2021-01-01", "approximate", "1900-01-01", "day", True),
        ("2021-01-01", "day", "1900-01-01", "approximate", True),
    ],
)
def test_temporal_interval_order_uses_precision_ranges(
    valid_from: str,
    from_precision: str,
    valid_to: str,
    to_precision: str,
    valid: bool,
) -> None:
    claim = load_json("messages/source-collect-completed.v1.json")["payload"]["claims"][0]
    claim.update(
        valid_from=valid_from,
        valid_from_precision=from_precision,
        valid_to=valid_to,
        valid_to_precision=to_precision,
        temporal_basis="explicit",
    )
    if valid:
        Claim.model_validate(claim)
    else:
        with pytest.raises(ValidationError, match="cannot be later"):
            Claim.model_validate(claim)


def test_schema_boundary_checks_uri_format(registry: ContractSchemaRegistry) -> None:
    completed = load_json("messages/source-collect-completed.v1.json")
    completed["payload"]["artifacts"][0]["source_uri"] = "not a URI"
    with pytest.raises(SchemaValidationError):
        registry.validate_message(completed)


@pytest.mark.parametrize(
    ("relative_path", "field", "duplicate", "manifest"),
    [
        (
            "messages/source-collect-completed.v1.json",
            '"schema_version": 1,',
            '"schema_version": 1,',
            False,
        ),
        (
            "messages/source-collect-completed.v1.json",
            '"record_count": 1,',
            '"record_count": 1,',
            False,
        ),
        (
            "messages/source-collect-requested.v1.json",
            '"iin": "900101300000"',
            '"iin": "900101300000",',
            False,
        ),
        (
            "ingestion/source-result-manifest.v1.json",
            '"record_count": 1,',
            '"record_count": 1,',
            True,
        ),
    ],
    ids=["envelope", "payload", "nested-object", "manifest"],
)
def test_raw_schema_boundary_rejects_duplicate_object_keys(
    registry: ContractSchemaRegistry,
    relative_path: str,
    field: str,
    duplicate: str,
    manifest: bool,
) -> None:
    raw = (EXAMPLES / relative_path).read_text(encoding="utf-8")
    assert raw.count(field) == 1
    duplicated = raw.replace(field, f"{duplicate}\n    {field}", 1)

    with pytest.raises(ValueError, match="duplicate JSON object key"):
        if manifest:
            registry.validate_manifest_json(duplicated)
        else:
            registry.validate_message_json(duplicated)


@pytest.mark.parametrize(
    ("value", "valid"),
    [
        (9_007_199_254_740_991, True),
        (9_007_199_254_740_992, False),
        (-9_007_199_254_740_991, True),
        (-9_007_199_254_740_992, False),
    ],
)
def test_raw_contract_boundary_uses_safe_integer_domain(value: int, valid: bool) -> None:
    raw = json.dumps({"value": value})
    if valid:
        assert decode_contract_json(raw) == {"value": value}
    else:
        with pytest.raises(ValueError, match="safe integer domain"):
            decode_contract_json(raw)


@pytest.mark.parametrize(
    ("value", "valid"),
    [
        (9_007_199_254_740_991, True),
        (9_007_199_254_740_992, False),
        (-9_007_199_254_740_991, True),
        (-9_007_199_254_740_992, False),
    ],
)
def test_manifest_uses_shared_safe_integer_domain(
    registry: ContractSchemaRegistry, value: int, valid: bool
) -> None:
    manifest = load_json("ingestion/source-result-manifest.v1.json")
    manifest["records"][0]["raw_data"]["numeric_identifier"] = value
    raw = json.dumps(manifest, ensure_ascii=False)
    if valid:
        registry.validate_manifest_json(raw)
    else:
        with pytest.raises(ValueError, match="safe integer domain"):
            registry.validate_manifest_json(raw)


@pytest.mark.parametrize("mutation", ["digest", "record_ref", "attribute_object", "manifest_hash"])
def test_semantic_validation_rejects_invalid_cross_object_state(mutation: str) -> None:
    if mutation == "manifest_hash":
        document = load_json("messages/source-collect-completed-manifest.v1.json")
        message = SourceCollectCompleted.model_validate(document)
        stored_bytes = (EXAMPLES / "ingestion/source-result-manifest.v1.json").read_bytes()
        manifest = SourceResultManifest.model_validate(json.loads(stored_bytes))
        assert message.payload.manifest is not None
        message.payload.manifest.sha256 = "0" * 64
        with pytest.raises(ValueError, match="hash or byte size"):
            validate_manifest_semantics(message.payload, manifest, stored_bytes)
        return

    document = copy.deepcopy(load_json("messages/source-collect-completed.v1.json"))
    if mutation == "digest":
        document["payload"]["digest"] = "0" * 64
    elif mutation == "record_ref":
        document["payload"]["records"][0]["artifact_local_ref"] = "artifact:missing"
    else:
        document["payload"]["claims"][0]["object_local_ref"] = "person:1"
    message = SourceCollectCompleted.model_validate(document)
    with pytest.raises(ValueError):
        validate_source_result_semantics(message.payload)


@pytest.mark.parametrize(
    "fixture_name",
    ["source-collect-completed.v1.json", "source-collect-completed-manifest.v1.json"],
    ids=["inline", "manifest"],
)
def test_semantic_validation_rejects_duplicate_artifacts_in_every_mode(
    fixture_name: str,
) -> None:
    message = SourceCollectCompleted.model_validate(load_json(f"messages/{fixture_name}"))
    message.payload.artifacts.append(message.payload.artifacts[0].model_copy(deep=True))
    message.payload.digest = compute_source_result_digest(message.payload)

    with pytest.raises(ValueError, match="artifact local_ref values must be unique"):
        validate_source_result_semantics(message.payload)


def test_wire_digest_validation_preserves_original_datetime_representation() -> None:
    document = load_json("messages/source-collect-completed.v1.json")
    payload = document["payload"]
    retrieved_at = payload["artifacts"][0]["retrieved_at"]
    assert retrieved_at.endswith("Z")
    payload["artifacts"][0]["retrieved_at"] = f"{retrieved_at[:-1]}+00:00"
    payload["digest"] = compute_source_result_digest(payload)
    result = SourceCollectCompleted.model_validate(document).payload

    wire_payload = json.dumps(payload, ensure_ascii=False).encode()
    validate_source_result_semantics(result, wire_payload)
    with pytest.raises(ValueError, match="digest"):
        validate_source_result_semantics(result)


def test_jcs_digest_uses_rfc_8785_number_serialization() -> None:
    payload = {
        "digest": "ignored",
        "numbers": [333333333.33333329, 1e30, 4.50, 2e-3, 1e-27],
    }
    canonical = b'{"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27]}'
    assert compute_source_result_digest(payload) == sha256(canonical).hexdigest()


def test_jcs_digest_rejects_integer_outside_safe_domain() -> None:
    with pytest.raises(ValueError, match="safe integer domain"):
        compute_source_result_digest({"raw_data": {"identifier": 9_007_199_254_740_992}})


def test_wire_digest_validation_preserves_original_uri_representation() -> None:
    document = load_json("messages/source-collect-completed.v1.json")
    payload = document["payload"]
    payload["artifacts"][0]["source_uri"] = "https://example.test"
    payload["digest"] = compute_source_result_digest(payload)
    result = SourceCollectCompleted.model_validate(document).payload

    validate_source_result_semantics(result, payload)
    with pytest.raises(ValueError, match="digest"):
        validate_source_result_semantics(result)


def test_semantic_validation_rejects_duplicate_records() -> None:
    message = SourceCollectCompleted.model_validate(
        load_json("messages/source-collect-completed.v1.json")
    )
    result = message.payload
    result.records.append(result.records[0].model_copy(deep=True))
    with pytest.raises(ValueError, match="source record local_ref values must be unique"):
        validate_result_content(result.records, result.claims, result.artifacts)


@pytest.mark.parametrize("field", ["job_id", "attempt_no", "record_count"])
def test_manifest_validation_rejects_every_metadata_mismatch(field: str) -> None:
    message = SourceCollectCompleted.model_validate(
        load_json("messages/source-collect-completed-manifest.v1.json")
    )
    stored_bytes = (EXAMPLES / "ingestion/source-result-manifest.v1.json").read_bytes()
    manifest = SourceResultManifest.model_validate_json(stored_bytes).model_copy(deep=True)
    if field == "job_id":
        manifest.job_id = UUID("00000000-0000-4000-8000-000000000099")
    else:
        setattr(manifest, field, getattr(manifest, field) + 1)

    with pytest.raises(ValueError, match="metadata"):
        validate_manifest_semantics(message.payload, manifest, stored_bytes)


@pytest.mark.parametrize(
    "mutation",
    ["relationship_without_object", "empty_event", "unknown_with_date"],
)
def test_claim_model_rejects_required_negative_semantics(mutation: str) -> None:
    claim = load_json("messages/source-collect-completed.v1.json")["payload"]["claims"][0]
    if mutation == "relationship_without_object":
        claim.update(claim_kind="relationship", object_local_ref=None)
    elif mutation == "empty_event":
        claim.update(
            claim_kind="event", object_local_ref=None, raw_value=None, normalized_value=None
        )
    else:
        claim.update(
            valid_from="2020-01-01",
            valid_from_precision="day",
            temporal_basis="unknown",
        )
    with pytest.raises(ValidationError):
        Claim.model_validate(claim)
