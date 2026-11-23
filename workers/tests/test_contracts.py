import copy
import json
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID

import pytest
from jsonschema import ValidationError as SchemaValidationError
from pydantic import ValidationError

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.claims import Claim
from graphx_workers.contracts.errors import ERROR_CLASSIFICATION, ErrorCode
from graphx_workers.contracts.messages import (
    SourceCollectCompleted,
    SourceQuery,
    parse_source_message,
)
from graphx_workers.contracts.records import SourceRecord
from graphx_workers.contracts.results import (
    SourceResult,
    SourceResultManifest,
    compute_source_result_digest,
    validate_manifest_semantics,
    validate_result_content,
    validate_source_result_semantics,
)
from graphx_workers.contracts.schema import (
    ContractSchemaRegistry,
    decode_contract_json,
    validate_raw_locator_numbers,
)

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "contracts" / "examples"


def load_json(relative_path: str) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads((EXAMPLES / relative_path).read_text(encoding="utf-8")))


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


@pytest.mark.parametrize(
    "locator",
    [
        {"json_pointer": ""},
        {"json_pointer": "/people/0/name~1display"},
        {"json_pointer": "/name", "column": "name"},
        {"csv_row": 2},
        {"csv_row": 2.0},
        {"csv_row": 9_007_199_254_740_991},
        {"page_number": 3},
        {"page_number": 3.0},
        {"fragment": "section-2"},
        {"page_number": 3, "fragment": "table 1"},
        {"page_number": 3, "section": "appendix"},
    ],
)
def test_claim_accepts_portable_evidence_locators(locator: dict[str, Any]) -> None:
    claim = Claim(
        claim_kind="attribute",
        subject_local_ref="record:1",
        predicate="name",
        raw_value="Example",
        evidence_locator=locator,
    )

    assert claim.evidence_locator == locator


@pytest.mark.parametrize("number", ["2", "2.0", "2e0", "9007199254740991"])
def test_claim_accepts_equivalent_safe_integer_json_representations(number: str) -> None:
    claim = Claim.model_validate_json(
        '{"claim_kind":"attribute","subject_local_ref":"record:1",'
        '"predicate":"name","raw_value":"Example","evidence_locator":'
        f'{{"csv_row":{number}}}}}'
    )

    assert claim.evidence_locator["csv_row"] == float(number)


@pytest.mark.parametrize(
    "number",
    ["0", "1.5", "1.00000000000000001", "1e20", "9007199254740992"],
)
def test_claim_rejects_unsafe_integer_json_representations(number: str) -> None:
    with pytest.raises(ValueError, match="exact safe integer"):
        Claim.model_validate_json(
            '{"claim_kind":"attribute","subject_local_ref":"record:1",'
            '"predicate":"name","raw_value":"Example","evidence_locator":'
            f'{{"csv_row":{number}}}}}'
        )


@pytest.mark.parametrize("number", ["2", "2.0", "2e0", "9007199254740991", "2.00e1"])
def test_raw_locator_number_tokens_preserve_valid_decimal_semantics(number: str) -> None:
    validate_raw_locator_numbers(
        f'{{"claims":[{{"evidence_locator":{{"csv_row":{number}}}}}]}}'.encode()
    )


@pytest.mark.parametrize(
    "number",
    [
        "0",
        "1.5",
        "1.00000000000000001",
        "9007199254740991.1",
        "9007199254740992",
        "1e20",
        "-1",
        "1e-1",
    ],
)
def test_raw_locator_number_tokens_reject_fractional_or_unsafe_values(number: str) -> None:
    with pytest.raises(ValueError, match="exact safe integer"):
        validate_raw_locator_numbers(
            f'{{"claims":[{{"evidence_locator":{{"page_number":{number}}}}}]}}'.encode()
        )


@pytest.mark.parametrize(
    "relative_path",
    [
        "messages/source-collect-completed.v1.json",
        "ingestion/source-result-manifest.v1.json",
    ],
)
def test_registry_rejects_inexact_locator_tokens_at_raw_entry_points(
    registry: ContractSchemaRegistry, relative_path: str
) -> None:
    raw = (
        (EXAMPLES / relative_path)
        .read_text()
        .replace('"json_pointer": "/name"', '"csv_row": 1.00000000000000001')
    )

    with pytest.raises(ValueError, match="exact safe integer"):
        if relative_path.startswith("messages/"):
            registry.validate_message_json(raw)
        else:
            registry.validate_manifest_json(raw)


@pytest.mark.parametrize(
    "relative_path",
    [
        "messages/source-collect-completed.v1.json",
        "ingestion/source-result-manifest.v1.json",
    ],
)
@pytest.mark.parametrize("number", ["1.00000000000000001", "9007199254740992.0"])
def test_registry_rejects_inexact_record_count_tokens_at_raw_entry_points(
    registry: ContractSchemaRegistry, relative_path: str, number: str
) -> None:
    raw = (
        (EXAMPLES / relative_path)
        .read_text()
        .replace('"record_count": 1', f'"record_count": {number}', 1)
    )

    with pytest.raises((ValueError, SchemaValidationError), match="record_count"):
        if relative_path.startswith("messages/"):
            registry.validate_message_json(raw)
        else:
            registry.validate_manifest_json(raw)


def test_source_result_json_rejects_inexact_nested_locator_token() -> None:
    result = SourceCollectCompleted.model_validate(
        load_json("messages/source-collect-completed.v1.json")
    ).payload
    raw = result.model_dump_json().replace(
        '"json_pointer":"/name"', '"csv_row":1.00000000000000001'
    )

    with pytest.raises(ValueError, match="exact safe integer"):
        SourceResult.model_validate_json(raw)


def test_manifest_json_rejects_inexact_nested_locator_token() -> None:
    manifest = SourceResultManifest.model_validate(
        load_json("ingestion/source-result-manifest.v1.json")
    )
    raw = manifest.model_dump_json().replace(
        '"json_pointer":"/name"', '"page_number":1.00000000000000001'
    )

    with pytest.raises(ValueError, match="exact safe integer"):
        SourceResultManifest.model_validate_json(raw)


@pytest.mark.parametrize("number", ["1", "1.0", "1e0"])
def test_result_models_accept_exact_record_count_tokens(number: str) -> None:
    result = SourceCollectCompleted.model_validate(
        load_json("messages/source-collect-completed.v1.json")
    ).payload
    result_raw = result.model_dump_json().replace('"record_count":1', f'"record_count":{number}')
    assert SourceResult.model_validate_json(result_raw).record_count == 1

    manifest = SourceResultManifest.model_validate(
        load_json("ingestion/source-result-manifest.v1.json")
    )
    manifest_raw = manifest.model_dump_json().replace(
        '"record_count":1', f'"record_count":{number}'
    )
    assert SourceResultManifest.model_validate_json(manifest_raw).record_count == 1


@pytest.mark.parametrize(
    "number",
    ["-1", "1.5", "1.00000000000000001", "9007199254740992", '"1"', "true", "null"],
)
def test_result_models_reject_inexact_record_count_tokens(number: str) -> None:
    document = SourceCollectCompleted.model_validate(
        load_json("messages/source-collect-completed.v1.json")
    )
    result_raw = document.payload.model_dump_json().replace(
        '"record_count":1', f'"record_count":{number}'
    )
    envelope_raw = document.model_dump_json().replace(
        '"record_count":1', f'"record_count":{number}'
    )

    with pytest.raises(ValueError, match="exact non-negative safe integer"):
        SourceResult.model_validate_json(result_raw)
    with pytest.raises(ValueError, match="exact non-negative safe integer"):
        SourceCollectCompleted.model_validate_json(envelope_raw)

    manifest = SourceResultManifest.model_validate(
        load_json("ingestion/source-result-manifest.v1.json")
    )
    manifest_raw = manifest.model_dump_json().replace(
        '"record_count":1', f'"record_count":{number}'
    )
    with pytest.raises(ValueError, match="exact non-negative safe integer"):
        SourceResultManifest.model_validate_json(manifest_raw)


@pytest.mark.parametrize("claim_kind", ["attribute", "event"])
@pytest.mark.parametrize("value", [[], {}, "", "\u0007", "\u001c", "\u001f", "\u007f", "\u0080"])
def test_claim_rejects_values_without_meaning(
    claim_kind: Literal["attribute", "event"], value: Any
) -> None:
    with pytest.raises(ValidationError, match="meaningful value"):
        Claim(
            claim_kind=claim_kind,
            subject_local_ref="record:1",
            predicate="name",
            raw_value=value,
            normalized_value=None,
            evidence_locator={"json_pointer": "/name"},
        )


@pytest.mark.parametrize("value", [False, 0])
def test_claim_accepts_meaningful_falsey_scalars(value: Any) -> None:
    claim = Claim(
        claim_kind="attribute",
        subject_local_ref="record:1",
        predicate="active",
        raw_value=value,
        normalized_value=None,
        evidence_locator={"json_pointer": "/active"},
    )

    assert claim.raw_value == value


def test_claim_accepts_text_mixed_with_control_characters() -> None:
    claim = Claim(
        claim_kind="attribute",
        subject_local_ref="record:1",
        predicate="name",
        raw_value="Example\u0007",
        normalized_value=None,
        evidence_locator={"json_pointer": "/name"},
    )

    assert claim.raw_value == "Example\u0007"


def test_direct_attribute_claim_rejects_object_reference() -> None:
    with pytest.raises(ValidationError, match="cannot contain object_local_ref"):
        Claim(
            claim_kind="attribute",
            subject_local_ref="record:1",
            predicate="name",
            object_local_ref="record:2",
            raw_value="Example",
            evidence_locator={"json_pointer": "/name"},
        )


@pytest.mark.parametrize("record_count", [True, "1", 9_007_199_254_740_992])
def test_result_mapping_models_reject_invalid_record_count(record_count: Any) -> None:
    document = load_json("messages/source-collect-completed.v1.json")
    document["payload"]["record_count"] = record_count
    payload = document["payload"]
    manifest = load_json("ingestion/source-result-manifest.v1.json")
    manifest["record_count"] = record_count

    with pytest.raises(ValidationError, match="exact non-negative safe integer"):
        SourceResult.model_validate(payload)
    with pytest.raises(ValidationError, match="exact non-negative safe integer"):
        SourceCollectCompleted.model_validate(document)
    with pytest.raises(ValidationError, match="exact non-negative safe integer"):
        SourceResultManifest.model_validate(manifest)


def test_result_mapping_models_accept_integral_float_record_count() -> None:
    document = load_json("messages/source-collect-completed.v1.json")
    document["payload"]["record_count"] = 1.0
    manifest = load_json("ingestion/source-result-manifest.v1.json")
    manifest["record_count"] = 1.0

    assert SourceCollectCompleted.model_validate(document).payload.record_count == 1
    assert SourceResultManifest.model_validate(manifest).record_count == 1


@pytest.mark.parametrize(
    "relative_path",
    [
        "messages/source-collect-completed.v1.json",
        "ingestion/source-result-manifest.v1.json",
    ],
)
def test_raw_locator_scan_does_not_narrow_numbers_outside_locator(
    registry: ContractSchemaRegistry, relative_path: str
) -> None:
    raw = (
        (EXAMPLES / relative_path)
        .read_text()
        .replace('"raw_data": {', '"raw_data": {"extreme_zero": 0e9223372036854775807,', 1)
    )

    if relative_path.startswith("messages/"):
        registry.validate_message_json(raw)
    else:
        registry.validate_manifest_json(raw)


@pytest.mark.parametrize(
    "locator",
    [
        {},
        {"json_pointer": "people/0"},
        {"json_pointer": "/bad~2escape"},
        {"csv_row": 0},
        {"csv_row": 1.5},
        {"csv_row": float(9_007_199_254_740_992)},
        {"page_number": -1},
        {"page_number": None, "fragment": "section-2"},
        {"fragment": ""},
        {"fragment": " \t"},
        {"fragment": "\u0007"},
        {"fragment": "\u001c"},
        {"fragment": "\u007f"},
        {"page_number": 1, "fragment": None},
        {"json_pointer": "/name", "csv_row": 1},
        {"offset": 42},
    ],
)
def test_claim_rejects_non_portable_evidence_locators(locator: dict[str, Any]) -> None:
    with pytest.raises(ValidationError, match="evidence"):
        Claim(
            claim_kind="attribute",
            subject_local_ref="record:1",
            predicate="name",
            raw_value="Example",
            evidence_locator=locator,
        )


@pytest.mark.parametrize("field", ["subject_local_ref", "predicate", "object_local_ref"])
@pytest.mark.parametrize("value", [" \t", "\u0007", "\u007f"])
def test_claim_rejects_blank_persisted_reference_text(field: str, value: str) -> None:
    claim = load_json("messages/source-collect-completed.v1.json")["payload"]["claims"][0]
    claim[field] = value

    with pytest.raises(ValidationError, match=r"blank|U\+0000"):
        Claim.model_validate(claim)


def test_completed_fixtures_pass_semantic_validation(registry: ContractSchemaRegistry) -> None:
    inline_document = load_json("messages/source-collect-completed.v1.json")
    inline = SourceCollectCompleted.model_validate(inline_document)
    validate_source_result_semantics(inline.payload, inline_document["payload"])

    manifest_message_document = load_json("messages/source-collect-completed-manifest.v1.json")
    manifest_message = SourceCollectCompleted.model_validate(manifest_message_document)
    stored_bytes = (EXAMPLES / "ingestion/source-result-manifest.v1.json").read_bytes()
    manifest_document = registry.validate_manifest_json(stored_bytes)
    manifest = SourceResultManifest.model_validate(manifest_document)
    validate_source_result_semantics(manifest_message.payload, manifest_message_document["payload"])
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
    ("field", "value"),
    [
        ("source_uri", "https://user:password@example.test/source"),
        ("source_uri", "https://example.test/source?token=secret"),
        ("source_uri", "file:///etc/passwd"),
        ("source_uri", "javascript:alert(1)"),
        ("source_uri", "https://example.test:99999/source"),
        ("source_uri", "https://example.test:00080/source"),
        ("original_filename", " report.json "),
        ("original_filename", "..."),
        ("original_filename", "bad\u0085name.json"),
        ("local_ref", "artifact:bad\u0085ref"),
    ],
)
def test_artifact_security_boundary_rejects_unsafe_transport_values(
    registry: ContractSchemaRegistry, field: str, value: str
) -> None:
    document = load_json("messages/source-collect-completed.v1.json")
    document["payload"]["artifacts"][0][field] = value
    with pytest.raises((SchemaValidationError, ValueError)):
        registry.validate_message(document)
    with pytest.raises(ValidationError):
        parse_source_message(document)


@pytest.mark.parametrize(
    "metadata",
    [
        {"authorization": "Bearer secret"},
        {"cookie": "session=secret"},
        {"etag": "bad\r\nheader"},
        {"etag": "x" * 513},
    ],
)
def test_artifact_security_boundary_rejects_unsafe_http_metadata(
    registry: ContractSchemaRegistry, metadata: dict[str, str]
) -> None:
    document = load_json("messages/source-collect-completed.v1.json")
    document["payload"]["artifacts"][0]["http_metadata"] = metadata
    with pytest.raises(SchemaValidationError):
        registry.validate_message(document)
    with pytest.raises(ValidationError):
        parse_source_message(document)


def test_python_artifact_model_rejects_transport_values_outside_runtime_policy() -> None:
    document = load_json("messages/source-collect-completed.v1.json")
    artifact = document["payload"]["artifacts"][0]
    artifact["source_uri"] = "https://example.test/source?token=secret"
    with pytest.raises(ValidationError, match="query parameters"):
        ArtifactReference.model_validate(artifact)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("local_ref", " "),
        ("local_ref", "\u0007"),
        ("media_type", " "),
        ("media_type", "\u007f"),
        ("http_metadata", {"etag": "bad\x00value"}),
        ("http_metadata", {b"etag": "first", "etag": "second"}),
    ],
)
def test_python_artifact_model_rejects_postgres_incompatible_values(field: str, value: Any) -> None:
    artifact = load_json("messages/source-collect-completed.v1.json")["payload"]["artifacts"][0]
    artifact[field] = value

    with pytest.raises(ValidationError, match=r"blank|U\+0000|object keys|pattern"):
        ArtifactReference.model_validate(artifact)


def test_artifact_contract_accepts_bracketed_ipv6_source_uri(
    registry: ContractSchemaRegistry,
) -> None:
    document = load_json("messages/source-collect-completed.v1.json")
    payload = document["payload"]
    payload["artifacts"][0]["source_uri"] = "https://[::1]/source/1"
    payload["digest"] = compute_source_result_digest(payload)

    registry.validate_message(document)
    message = SourceCollectCompleted.model_validate(document)
    validate_source_result_semantics(message.payload, payload)


def test_artifact_contract_accepts_maximum_source_uri_port(
    registry: ContractSchemaRegistry,
) -> None:
    document = load_json("messages/source-collect-completed.v1.json")
    payload = document["payload"]
    payload["artifacts"][0]["source_uri"] = "https://example.test:65535/source/1"
    payload["digest"] = compute_source_result_digest(payload)

    registry.validate_message(document)
    message = SourceCollectCompleted.model_validate(document)
    validate_source_result_semantics(message.payload, payload)


def test_typed_boundary_binds_object_key_to_result_attempt() -> None:
    document = load_json("messages/source-collect-completed.v1.json")
    payload = document["payload"]
    payload["artifacts"][0]["object_key"] = (
        "artifacts/v1/00000000-0000-4000-8000-000000000099/1/YXJ0aWZhY3Q6cmF3OjE"
    )
    payload["digest"] = compute_source_result_digest(payload)
    with pytest.raises(ValidationError, match="object_key"):
        SourceCollectCompleted.model_validate(document)


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
        with pytest.raises(ValidationError, match="cannot contain object_local_ref"):
            SourceCollectCompleted.model_validate(document)
        return
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


@pytest.mark.parametrize("mode", ["inline", "manifest"])
def test_semantic_validation_rejects_mixed_schema_versions_per_artifact(mode: str) -> None:
    if mode == "inline":
        message = SourceCollectCompleted.model_validate(
            load_json("messages/source-collect-completed.v1.json")
        )
        records = message.payload.records
        claims = message.payload.claims
        artifacts = message.payload.artifacts
    else:
        message = SourceCollectCompleted.model_validate(
            load_json("messages/source-collect-completed-manifest.v1.json")
        )
        manifest = SourceResultManifest.model_validate(
            load_json("ingestion/source-result-manifest.v1.json")
        )
        records = manifest.records
        claims = manifest.claims
        artifacts = message.payload.artifacts
    conflicting = records[0].model_copy(
        update={"local_ref": "record:conflicting", "schema_version": "schema/other"}
    )

    with pytest.raises(ValueError, match="one schema_version"):
        validate_result_content([*records, conflicting], claims, artifacts)


def test_semantic_validation_allows_different_schema_versions_between_artifacts() -> None:
    message = SourceCollectCompleted.model_validate(
        load_json("messages/source-collect-completed.v1.json")
    )
    result = message.payload
    second_artifact = result.artifacts[0].model_copy(update={"local_ref": "artifact:raw:2"})
    second_record = result.records[0].model_copy(
        update={
            "local_ref": "organization:2",
            "artifact_local_ref": second_artifact.local_ref,
            "schema_version": "fixture-organization/2",
        }
    )

    validate_result_content(
        [result.records[0], second_record],
        result.claims,
        [result.artifacts[0], second_artifact],
    )


@pytest.mark.parametrize(
    "fixture",
    ["source-collect-completed.v1.json", "source-collect-completed-manifest.v1.json"],
)
def test_source_result_rejects_manifest_kind_in_source_artifacts(fixture: str) -> None:
    document = load_json(f"messages/{fixture}")
    payload = document["payload"]
    payload["artifacts"][0]["kind"] = "manifest"
    payload["digest"] = compute_source_result_digest(payload)
    message = SourceCollectCompleted.model_validate(document)

    with pytest.raises(ValueError, match="cannot use manifest kind"):
        validate_source_result_semantics(message.payload, payload)


@pytest.mark.parametrize(
    "field", ["local_ref", "artifact_local_ref", "record_type", "external_id", "schema_version"]
)
@pytest.mark.parametrize("value", [" \t", "\u0007", "\u001c", "\u007f", "\u00a0", "v\x00x"])
def test_source_record_rejects_invalid_persisted_text(field: str, value: str) -> None:
    record = load_json("messages/source-collect-completed.v1.json")["payload"]["records"][0]
    record[field] = value

    with pytest.raises(ValidationError, match=r"blank|U\+0000"):
        SourceRecord.model_validate(record)


@pytest.mark.parametrize(
    "field",
    ["adapter_version", "parser_version", "mapping_version", "normalization_version"],
)
@pytest.mark.parametrize("value", [" \t", "\u0007", "\u001c", "\u007f", "\u00a0", "v\x00x"])
def test_source_result_rejects_invalid_versions(field: str, value: str) -> None:
    document = load_json("messages/source-collect-completed.v1.json")
    document["payload"][field] = value

    with pytest.raises(ValidationError, match="result versions"):
        SourceCollectCompleted.model_validate(document)


def test_persisted_text_and_fragment_accept_text_mixed_with_control() -> None:
    document = load_json("messages/source-collect-completed.v1.json")
    document["payload"]["parser_version"] = "parser\u0007v1"
    document["payload"]["records"][0]["local_ref"] = "person\u0007:1"
    document["payload"]["claims"][0]["subject_local_ref"] = "person\u0007:1"
    document["payload"]["claims"][0]["evidence_locator"] = {"fragment": "section\u0007-1"}

    message = SourceCollectCompleted.model_validate(document)

    assert message.payload.parser_version == "parser\u0007v1"
    assert message.payload.records[0].local_ref == "person\u0007:1"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("raw_data", {"nested": ["safe", "bad\x00value"]}),
        ("raw_data", {"bad\x00key": "value"}),
    ],
)
def test_source_record_rejects_postgres_incompatible_jsonb(field: str, value: Any) -> None:
    record = load_json("messages/source-collect-completed.v1.json")["payload"]["records"][0]
    record[field] = value

    with pytest.raises(ValidationError, match="U\\+0000"):
        SourceRecord.model_validate(record)


def test_source_record_rejects_non_string_json_object_keys_before_coercion() -> None:
    record = load_json("messages/source-collect-completed.v1.json")["payload"]["records"][0]
    record["raw_data"] = {b"sample": "first", "sample": "second"}

    with pytest.raises(ValidationError, match="object keys must be strings"):
        SourceRecord.model_validate(record)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("raw_value", {"nested": "bad\x00value"}),
        ("qualifiers", {"bad\x00key": True}),
        ("evidence_locator", {"json_pointer": "/bad\x00value"}),
        ("evidence_excerpt", "bad\x00value"),
    ],
)
def test_claim_rejects_postgres_incompatible_values(field: str, value: Any) -> None:
    claim = load_json("messages/source-collect-completed.v1.json")["payload"]["claims"][0]
    claim[field] = value

    with pytest.raises(ValidationError, match="U\\+0000"):
        Claim.model_validate(claim)


def test_claim_rejects_non_string_json_object_keys_before_coercion() -> None:
    claim = load_json("messages/source-collect-completed.v1.json")["payload"]["claims"][0]
    claim["qualifiers"] = {b"sample": "first", "sample": "second"}

    with pytest.raises(ValidationError, match="object keys must be strings"):
        Claim.model_validate(claim)


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
