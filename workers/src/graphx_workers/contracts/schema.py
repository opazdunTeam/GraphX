"""Portable JSON Schema validation at the messaging boundary."""

import json
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from math import isfinite
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from graphx_workers.contracts.values import MAX_SAFE_INTEGER

MESSAGE_SCHEMAS = {
    "source.collect.requested.v1": "messages/source-collect-requested.v1.schema.json",
    "source.collect.completed.v1": "messages/source-collect-completed.v1.schema.json",
    "source.collect.failed.v1": "messages/source-collect-failed.v1.schema.json",
}


def decode_contract_json(raw: bytes | str) -> dict[str, Any]:
    """Decode an unambiguous I-JSON object before schema or typed-model validation."""

    def object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"duplicate JSON object key: {key!r}")
            value[key] = item
        return value

    def safe_integer(text: str) -> int:
        value = int(text)
        if abs(value) > MAX_SAFE_INTEGER:
            raise ValueError(f"JSON integer {text!r} exceeds the JCS safe integer domain")
        return value

    def finite_float(text: str) -> float:
        value = float(text)
        if not isfinite(value):
            raise ValueError(f"JSON number {text!r} is outside the finite JCS domain")
        return value

    def reject_constant(text: str) -> None:
        raise ValueError(f"non-standard JSON constant is forbidden: {text}")

    value = json.loads(
        raw,
        object_pairs_hook=object_without_duplicates,
        parse_int=safe_integer,
        parse_float=finite_float,
        parse_constant=reject_constant,
    )
    if not isinstance(value, dict):
        raise ValueError("contract JSON document must be an object")
    return value


class ContractSchemaRegistry:
    """Load the canonical schemas once and reject unsupported discriminators."""

    def __init__(self, schema_root: Path) -> None:
        resources: list[tuple[str, Resource[Any]]] = []
        documents: dict[str, dict[str, Any]] = {}
        for path in sorted(schema_root.rglob("*.schema.json")):
            document = decode_contract_json(path.read_bytes())
            Draft202012Validator.check_schema(document)
            resources.append((document["$id"], Resource.from_contents(document)))
            documents[path.relative_to(schema_root).as_posix()] = document
        if not documents:
            raise ValueError(f"no contract schemas found under {schema_root}")

        registry = Registry().with_resources(resources)
        self._validators = {
            name: Draft202012Validator(
                documents[relative_path],
                registry=registry,
                format_checker=Draft202012Validator.FORMAT_CHECKER,
            )
            for name, relative_path in MESSAGE_SCHEMAS.items()
        }
        self._manifest_validator = Draft202012Validator(
            documents["ingestion/source-result-manifest.v1.schema.json"],
            registry=registry,
            format_checker=Draft202012Validator.FORMAT_CHECKER,
        )

    def validate_message(self, value: Mapping[str, Any]) -> None:
        message_type = value.get("type")
        if not isinstance(message_type, str) or value.get("schema_version") != 1:
            raise ValueError("unsupported message type or schema version")
        validator = self._validators.get(message_type)
        if validator is None:
            raise ValueError(f"unsupported message type: {message_type}")
        validator.validate(value)

    def validate_message_json(self, raw: bytes | str) -> dict[str, Any]:
        value = decode_contract_json(raw)
        self.validate_message(value)
        if value.get("type") == "source.collect.completed.v1":
            validate_raw_record_count(raw, envelope=True)
            validate_raw_locator_numbers(raw, envelope=True)
        return value

    def validate_manifest(self, value: Mapping[str, Any]) -> None:
        self._manifest_validator.validate(value)

    def validate_manifest_json(self, raw: bytes | str) -> dict[str, Any]:
        value = decode_contract_json(raw)
        self.validate_manifest(value)
        validate_raw_record_count(raw)
        validate_raw_locator_numbers(raw)
        return value


def validate_raw_locator_numbers(
    document: bytes | bytearray | str,
    *,
    envelope: bool = False,
    single_claim: bool = False,
) -> None:
    """Reject locator decimals whose exact token is fractional or not portable."""

    class NumberToken(str):
        pass

    decoded = json.loads(document, parse_int=NumberToken, parse_float=NumberToken)
    if not isinstance(decoded, dict):
        return
    if envelope:
        decoded = decoded.get("payload")
        if not isinstance(decoded, dict):
            return
    claims = [decoded] if single_claim else decoded.get("claims")
    if not isinstance(claims, list):
        return
    for claim in claims:
        if not isinstance(claim, dict):
            continue
        locator = claim.get("evidence_locator")
        if not isinstance(locator, dict):
            continue
        for field in ("csv_row", "page_number"):
            token = locator.get(field)
            if not isinstance(token, NumberToken):
                continue
            try:
                value = Decimal(token)
                integral = value.to_integral_exact()
            except (InvalidOperation, ValueError) as error:
                raise ValueError(f"evidence {field} must be an exact safe integer") from error
            if value != integral or not 1 <= integral <= MAX_SAFE_INTEGER:
                raise ValueError(f"evidence {field} must be an exact safe integer")


def validate_raw_record_count(document: bytes | bytearray | str, *, envelope: bool = False) -> None:
    """Reject record counts that only become integers after float coercion."""

    class NumberToken(str):
        pass

    decoded = json.loads(document, parse_int=NumberToken, parse_float=NumberToken)
    if not isinstance(decoded, dict):
        return
    if envelope:
        decoded = decoded.get("payload")
        if not isinstance(decoded, dict):
            return
    if "record_count" not in decoded:
        return
    token = decoded["record_count"]
    if not isinstance(token, NumberToken):
        raise ValueError("record_count must be an exact non-negative safe integer")
    try:
        value = Decimal(token)
        integral = value.to_integral_exact()
    except (InvalidOperation, ValueError) as error:
        raise ValueError("record_count must be an exact non-negative safe integer") from error
    if value != integral or not 0 <= integral <= MAX_SAFE_INTEGER:
        raise ValueError("record_count must be an exact non-negative safe integer")
