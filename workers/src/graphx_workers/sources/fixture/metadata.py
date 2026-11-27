"""Stable fixture adapter identity and capability metadata."""

from graphx_workers.sdk import AdapterMetadata

SOURCE_CODE = "fixture"
SUPPORTED_OPERATION = "search_subject"
LEGACY_COLLECT_OPERATION = "collect"
SUPPORTED_ENTITY_TYPE = "person"
FIXTURE_CASE_IDENTIFIER = "fixture_case"

ADAPTER_VERSION = "fixture-adapter/1.0.0"
PARSER_VERSION = "fixture-parser/1.0.0"
MAPPING_VERSION = "fixture-mapper/1.0.0"
NORMALIZATION_VERSION = "graphx-normalization/1.0.0"
SCHEMA_VERSION = "fixture-person/1"

RAW_ARTIFACT_LOCAL_REF = "artifact:raw:1"
PERSON_LOCAL_REF = "person:1"

ADAPTER_METADATA = AdapterMetadata(
    source=SOURCE_CODE,
    version=ADAPTER_VERSION,
    supported_operations=frozenset({SUPPORTED_OPERATION, LEGACY_COLLECT_OPERATION}),
)
