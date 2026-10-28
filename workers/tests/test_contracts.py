import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.claims import Claim
from graphx_workers.contracts.messages import MessageEnvelope
from graphx_workers.contracts.records import SourceRecord
from graphx_workers.contracts.results import SourceResult


def test_message_envelope_uses_transport_type_field() -> None:
    envelope = MessageEnvelope(
        message_id=uuid4(),
        type="source.collect.requested.v1",
        schema_version=1,
        occurred_at=datetime.now(UTC),
        correlation_id=str(uuid4()),
        producer="graphx-api",
        root_command_id=uuid4(),
        payload={},
    )

    dumped = envelope.model_dump(mode="json")
    assert dumped["type"] == "source.collect.requested.v1"
    assert "message_type" not in dumped


def test_shared_json_schemas_are_valid_json() -> None:
    schemas = Path(__file__).resolve().parents[2] / "contracts" / "schemas"
    files = sorted(schemas.rglob("*.schema.json"))

    assert files
    for schema in files:
        document = json.loads(schema.read_text(encoding="utf-8"))
        assert document["$schema"] == "https://json-schema.org/draft/2020-12/schema"


def test_source_result_preserves_provenance_and_processing_versions() -> None:
    retrieved_at = datetime.now(UTC)
    result = SourceResult(
        job_id=uuid4(),
        attempt_no=1,
        outcome="succeeded",
        record_count=1,
        adapter_version="fixture-1",
        parser_version="parser-1",
        mapping_version="mapping-1",
        normalization_version="normalization-1",
        digest="a" * 64,
        artifacts=[
            ArtifactReference(
                kind="raw_response",
                object_key="fixture/result.json",
                sha256="b" * 64,
                media_type="application/json",
                byte_size=2,
                source_uri="fixture://subject",
                retrieved_at=retrieved_at,
                http_metadata={},
            )
        ],
        records=[
            SourceRecord(
                local_ref="person-1",
                external_id="external-1",
                record_type="person",
                schema_version="fixture-v1",
                raw_data={"name": "Example"},
            )
        ],
        claims=[
            Claim(
                claim_kind="attribute",
                subject_local_ref="person-1",
                predicate="name",
                value="Example",
                raw_value=" Example ",
                normalized_value="example",
                qualifiers={},
                temporal_basis="unknown",
                evidence_locator={"json_pointer": "/name"},
            )
        ],
        warnings=[],
    )

    dumped = result.model_dump(mode="json")
    assert dumped["mapping_version"] == "mapping-1"
    assert dumped["normalization_version"] == "normalization-1"
    assert dumped["artifacts"][0]["source_uri"] == "fixture://subject"
    assert dumped["records"][0]["external_id"] == "external-1"
    assert dumped["claims"][0]["normalized_value"] == "example"
