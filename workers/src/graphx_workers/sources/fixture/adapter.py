"""Fixture adapter implementing the public source SDK boundary."""

from typing import Literal, cast

from graphx_workers.sdk import (
    AdapterContext,
    ArtifactReference,
    Claim,
    CollectRequest,
    CollectResult,
    ErrorCode,
    PermanentAdapterError,
    SourceAdapter,
    SourceRecord,
    TransientAdapterError,
)
from graphx_workers.sources.fixture.client import (
    FixtureCase,
    FixtureClient,
    FixtureUnavailableError,
)
from graphx_workers.sources.fixture.mapper import map_person
from graphx_workers.sources.fixture.metadata import (
    ADAPTER_METADATA,
    ADAPTER_VERSION,
    FIXTURE_CASE_IDENTIFIER,
    MAPPING_VERSION,
    NORMALIZATION_VERSION,
    PARSER_VERSION,
    RAW_ARTIFACT_LOCAL_REF,
    SUPPORTED_ENTITY_TYPE,
    SUPPORTED_OPERATION,
)
from graphx_workers.sources.fixture.parser import FixtureFormatError, parse_fixture_response


class FixtureAdapter(SourceAdapter):
    metadata = ADAPTER_METADATA

    def __init__(self, client: FixtureClient | None = None) -> None:
        self._client = client or FixtureClient()

    async def collect(self, request: CollectRequest, context: AdapterContext) -> CollectResult:
        self._validate_request(request)
        fixture_case = self._fixture_case(request)
        try:
            response = self._client.fetch(fixture_case)
        except FixtureUnavailableError as error:
            raise TransientAdapterError(
                ErrorCode.SOURCE_UNAVAILABLE,
                "fixture source is temporarily unavailable",
            ) from error

        artifact = await context.save_artifact(
            response.content,
            "application/json",
            local_ref=RAW_ARTIFACT_LOCAL_REF,
            kind="raw_response",
            source_uri=response.source_uri,
            retrieved_at=response.retrieved_at,
            http_metadata={"etag": response.etag},
            original_filename="fixture-response.json",
        )
        try:
            parsed = parse_fixture_response(response.content)
        except FixtureFormatError as error:
            raise PermanentAdapterError(
                ErrorCode.SOURCE_INVALID_RESPONSE,
                "fixture response does not match its declared format",
            ) from error

        if parsed.outcome == "not_found":
            return self._result(outcome="not_found", artifact=artifact)
        if parsed.person is None:
            raise PermanentAdapterError(
                ErrorCode.SOURCE_INVALID_RESPONSE,
                "fixture success response does not contain a person",
            )
        record, claim = map_person(parsed.person, artifact)
        return self._result(
            outcome="succeeded",
            artifact=artifact,
            records=[record],
            claims=[claim],
        )

    @staticmethod
    def _validate_request(request: CollectRequest) -> None:
        if request.operation != SUPPORTED_OPERATION or request.query.type != SUPPORTED_ENTITY_TYPE:
            raise PermanentAdapterError(
                ErrorCode.SOURCE_UNSUPPORTED_OPERATION,
                "fixture adapter supports only person search_subject requests",
            )

    @staticmethod
    def _fixture_case(request: CollectRequest) -> FixtureCase:
        identifiers = request.query.identifiers or {}
        value = identifiers.get(FIXTURE_CASE_IDENTIFIER, "success")
        if value not in {"success", "not_found", "transient", "permanent"}:
            raise PermanentAdapterError(
                ErrorCode.SOURCE_UNSUPPORTED_OPERATION,
                "fixture scenario is not supported",
            )
        return cast(FixtureCase, value)

    @staticmethod
    def _result(
        *,
        outcome: Literal["succeeded", "not_found"],
        artifact: ArtifactReference,
        records: list[SourceRecord] | None = None,
        claims: list[Claim] | None = None,
    ) -> CollectResult:
        return CollectResult(
            outcome=outcome,
            adapter_version=ADAPTER_VERSION,
            parser_version=PARSER_VERSION,
            mapping_version=MAPPING_VERSION,
            normalization_version=NORMALIZATION_VERSION,
            artifacts=[artifact],
            records=records or [],
            claims=claims or [],
            warnings=[],
        )
