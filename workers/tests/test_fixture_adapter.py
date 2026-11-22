import asyncio
from collections.abc import AsyncIterable, AsyncIterator
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

import pytest
from pydantic import AnyUrl, AwareDatetime

from graphx_workers.contracts.artifacts import ArtifactReference, artifact_object_key
from graphx_workers.contracts.messages import (
    CollectLimits,
    SourceCollectCompleted,
    SourceCollectFailed,
    SourceCollectRequested,
    SourceCollectRequestedPayload,
    SourceMessage,
    SourceQuery,
    parse_source_message,
)
from graphx_workers.contracts.results import (
    SourceResultManifest,
    validate_manifest_semantics,
)
from graphx_workers.contracts.schema import ContractSchemaRegistry, decode_contract_json
from graphx_workers.runtime.finalization import MANIFEST_LOCAL_REF, ManifestResultFinalizer
from graphx_workers.runtime.processor import (
    AdapterContextFactory,
    AdapterRegistration,
    RuntimeAdapterContext,
    SourceCommandProcessor,
)
from graphx_workers.runtime.replay import FileTerminalReplayStore, VolatileTerminalReplayStore
from graphx_workers.runtime.results import ResultTooComplexError
from graphx_workers.runtime.terminal import TerminalMessageFactory
from graphx_workers.sdk import (
    AdapterContext,
    AdapterMetadata,
    CollectRequest,
    CollectResult,
    ErrorCode,
    PermanentAdapterError,
    TransientAdapterError,
)
from graphx_workers.sources.fixture import (
    ADAPTER_VERSION,
    FIXTURE_CASE_IDENTIFIER,
    SOURCE_CODE,
    SUPPORTED_OPERATION,
    FixtureAdapter,
)
from graphx_workers.sources.fixture.metadata import RAW_ARTIFACT_LOCAL_REF

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_ROOT = ROOT / "contracts" / "schemas"
NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
JOB_ID = UUID("22222222-2222-4222-8222-222222222222")
COMMAND_ID = UUID("33333333-3333-4333-8333-333333333333")
CORRELATION_ID = UUID("11111111-1111-4111-8111-111111111111")


async def byte_chunks(content: bytes) -> AsyncIterator[bytes]:
    yield content


class RecordingContext:
    def __init__(self, job_id: UUID = JOB_ID, attempt_no: int = 1) -> None:
        self.job_id = job_id
        self.attempt_no = attempt_no
        self.saved_artifacts: list[ArtifactReference] = []
        self.saved_content: list[bytes] = []

    async def save_artifact(
        self,
        content: bytes | AsyncIterable[bytes],
        media_type: str,
        *,
        local_ref: str,
        kind: Literal["raw_response", "document", "screenshot", "other"],
        source_uri: str,
        retrieved_at: AwareDatetime,
        published_at: AwareDatetime | None = None,
        http_metadata: dict[str, str] | None = None,
        original_filename: str | None = None,
        parent_local_ref: str | None = None,
    ) -> ArtifactReference:
        if isinstance(content, bytes):
            stored = content
        else:
            stored = b"".join([chunk async for chunk in content])
        reference = ArtifactReference(
            local_ref=local_ref,
            kind=kind,
            object_key=artifact_object_key(self.job_id, self.attempt_no, local_ref),
            sha256=sha256(stored).hexdigest(),
            media_type=media_type,
            byte_size=len(stored),
            source_uri=AnyUrl(source_uri),
            retrieved_at=retrieved_at,
            published_at=published_at,
            http_metadata=http_metadata or {},
            original_filename=original_filename,
            parent_local_ref=parent_local_ref,
        )
        self.saved_content.append(stored)
        self.saved_artifacts.append(reference)
        return reference

    async def save_manifest(
        self,
        content: AsyncIterable[bytes],
        *,
        local_ref: str,
        source_uri: str,
        retrieved_at: datetime,
        max_bytes: int,
    ) -> ArtifactReference:
        stored = b"".join([chunk async for chunk in content])
        if len(stored) > max_bytes:
            raise ValueError("manifest exceeds test context limit")
        reference = ArtifactReference(
            local_ref=local_ref,
            kind="manifest",
            object_key=artifact_object_key(self.job_id, self.attempt_no, local_ref),
            sha256=sha256(stored).hexdigest(),
            media_type="application/json",
            byte_size=len(stored),
            source_uri=AnyUrl(source_uri),
            retrieved_at=retrieved_at,
            original_filename="source-result-manifest.v1.json",
        )
        self.saved_content.append(stored)
        self.saved_artifacts.append(reference)
        return reference


class RecordingContextFactory(AdapterContextFactory):
    def __init__(self) -> None:
        self.context: RecordingContext | None = None
        self.calls = 0

    async def create(
        self,
        *,
        job_id: UUID,
        attempt_no: int,
        correlation_id: UUID,
        deadline: datetime,
    ) -> RuntimeAdapterContext:
        del correlation_id, deadline
        self.calls += 1
        self.context = RecordingContext(job_id, attempt_no)
        return cast(RuntimeAdapterContext, self.context)


def request(fixture_case: str = "success") -> CollectRequest:
    return CollectRequest(
        operation=SUPPORTED_OPERATION,
        purpose="subject_discovery",
        query=SourceQuery(
            type="person",
            name="Ada Fixture",
            identifiers={FIXTURE_CASE_IDENTIFIER: fixture_case},
        ),
        limits=CollectLimits(max_records=10, max_bytes=1024, timeout_seconds=30),
        deadline=NOW + timedelta(seconds=30),
    )


def command(fixture_case: str = "success") -> SourceCollectRequested:
    return SourceCollectRequested(
        message_id=COMMAND_ID,
        type="source.collect.requested.v1",
        schema_version=1,
        occurred_at=NOW,
        correlation_id=CORRELATION_ID,
        causation_id=None,
        producer="graphx-api",
        root_command_id=COMMAND_ID,
        payload=SourceCollectRequestedPayload(
            job_id=JOB_ID,
            attempt_no=1,
            investigation_id=CORRELATION_ID,
            source=SOURCE_CODE,
            operation=SUPPORTED_OPERATION,
            purpose="subject_discovery",
            query=request(fixture_case).query,
            limits=CollectLimits(max_records=10, max_bytes=1024, timeout_seconds=30),
            deadline=NOW + timedelta(seconds=30),
        ),
    )


def processor(
    registry: ContractSchemaRegistry,
    context_factory: RecordingContextFactory,
) -> SourceCommandProcessor:
    return SourceCommandProcessor(
        registry,
        TerminalMessageFactory(registry, clock=lambda: NOW + timedelta(seconds=5)),
        VolatileTerminalReplayStore(),
        context_factory,
        (
            AdapterRegistration(
                adapter=FixtureAdapter(),
            ),
        ),
        job_timeout=timedelta(minutes=1),
        clock=lambda: NOW,
    )


def test_fixture_registration_uses_immutable_adapter_metadata() -> None:
    adapter = FixtureAdapter()
    registration = AdapterRegistration(adapter=adapter)

    assert registration.source == SOURCE_CODE
    assert registration.version == ADAPTER_VERSION
    assert registration.metadata.supported_operations == frozenset({SUPPORTED_OPERATION})

    adapter.metadata = AdapterMetadata(
        source="changed",
        version="changed/1",
        supported_operations=frozenset({"changed"}),
    )
    assert registration.source == SOURCE_CODE
    assert registration.version == ADAPTER_VERSION


@pytest.mark.asyncio
async def test_success_fixture_saves_raw_response_and_maps_person_claim() -> None:
    context = RecordingContext()

    result = await FixtureAdapter().collect(request(), cast(AdapterContext, context))

    assert result.outcome == "succeeded"
    assert result.artifacts == context.saved_artifacts
    assert context.saved_content == [
        b'{"case":"success","person":{"external_id":"fixture-person-1","name":"Ada Fixture"}}'
    ]
    assert len(result.records) == 1
    assert len(result.claims) == 1
    assert result.records[0].local_ref == "person:1"
    assert result.records[0].artifact_local_ref == RAW_ARTIFACT_LOCAL_REF
    assert result.records[0].external_id == "fixture-person-1"
    assert result.claims[0].subject_local_ref == "person:1"
    assert result.claims[0].raw_value == "Ada Fixture"
    assert result.claims[0].normalized_value == "ada fixture"
    assert result.claims[0].evidence_locator == {"json_pointer": "/person/name"}


@pytest.mark.asyncio
async def test_not_found_fixture_is_successful_and_preserves_raw_response() -> None:
    context = RecordingContext()

    result = await FixtureAdapter().collect(
        request("not_found"),
        cast(AdapterContext, context),
    )

    assert result.outcome == "not_found"
    assert result.records == []
    assert result.claims == []
    assert result.artifacts == context.saved_artifacts
    assert context.saved_content == [b'{"case":"not_found","person":null}']


@pytest.mark.asyncio
async def test_transient_fixture_raises_typed_error_before_response_exists() -> None:
    context = RecordingContext()

    with pytest.raises(TransientAdapterError) as caught:
        await FixtureAdapter().collect(request("transient"), cast(AdapterContext, context))

    assert caught.value.code is ErrorCode.SOURCE_UNAVAILABLE
    assert context.saved_artifacts == []


@pytest.mark.asyncio
async def test_permanent_fixture_saves_raw_response_before_parse_error() -> None:
    context = RecordingContext()

    with pytest.raises(PermanentAdapterError) as caught:
        await FixtureAdapter().collect(request("permanent"), cast(AdapterContext, context))

    assert caught.value.code is ErrorCode.SOURCE_INVALID_RESPONSE
    assert context.saved_content == [b'{"case":"permanent","person":']
    assert len(context.saved_artifacts) == 1


@pytest.mark.asyncio
async def test_fixture_repeat_is_deterministic() -> None:
    first = await FixtureAdapter().collect(request(), cast(AdapterContext, RecordingContext()))
    second = await FixtureAdapter().collect(request(), cast(AdapterContext, RecordingContext()))

    assert first == second


@pytest.mark.asyncio
async def test_manifest_finalizer_keeps_small_result_inline() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context = RecordingContext()
    collected = await FixtureAdapter().collect(request(), cast(AdapterContext, context))

    result = await ManifestResultFinalizer(
        registry, inline_max_bytes=1024 * 1024, manifest_max_bytes=1024 * 1024
    ).finalize(command(), collected, cast(RuntimeAdapterContext, context))

    assert result.manifest is None
    assert len(result.records) == 1
    assert len(context.saved_content) == 1


@pytest.mark.asyncio
async def test_manifest_finalizer_stores_large_result_without_inline_duplication() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context = RecordingContext()
    collected = await FixtureAdapter().collect(request(), cast(AdapterContext, context))

    result = await ManifestResultFinalizer(
        registry, inline_max_bytes=1, manifest_max_bytes=1024 * 1024
    ).finalize(command(), collected, cast(RuntimeAdapterContext, context))

    assert result.manifest is not None
    assert result.manifest.local_ref == MANIFEST_LOCAL_REF
    assert result.records == []
    assert result.claims == []
    assert result.artifacts == collected.artifacts
    manifest_bytes = context.saved_content[-1]
    registry.validate_manifest_json(manifest_bytes)
    manifest = SourceResultManifest.model_validate_json(manifest_bytes)
    validate_manifest_semantics(result, manifest, manifest_bytes)


@pytest.mark.asyncio
async def test_manifest_finalizer_keeps_not_found_inline_below_any_threshold() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context = RecordingContext()
    collected = await FixtureAdapter().collect(request("not_found"), cast(AdapterContext, context))

    result = await ManifestResultFinalizer(
        registry, inline_max_bytes=1, manifest_max_bytes=1
    ).finalize(command("not_found"), collected, cast(RuntimeAdapterContext, context))

    assert result.outcome == "not_found"
    assert result.manifest is None
    assert result.records == []
    assert result.claims == []
    assert len(context.saved_content) == 1


@pytest.mark.asyncio
async def test_manifest_finalizer_includes_recovered_originals_in_terminal_result() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context = RecordingContext()
    collected = await FixtureAdapter().collect(request(), cast(AdapterContext, context))
    recovered = await context.save_artifact(
        b"recovered evidence",
        "text/plain",
        local_ref="artifact:document:recovered",
        kind="document",
        source_uri="https://example.test/fixture/document/1",
        retrieved_at=NOW,
    )

    result = await ManifestResultFinalizer(
        registry, inline_max_bytes=1024 * 1024, manifest_max_bytes=1024 * 1024
    ).finalize(command(), collected, cast(RuntimeAdapterContext, context))

    assert result.manifest is None
    assert result.artifacts == [recovered, collected.artifacts[0]]


@pytest.mark.asyncio
async def test_recovered_artifact_is_bounded_before_reconciliation_copy() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context = RecordingContext()
    collected = await FixtureAdapter().collect(request(), cast(AdapterContext, context))
    await context.save_artifact(
        b"recovered evidence",
        "text/plain",
        local_ref="artifact:document:recovered",
        kind="document",
        source_uri="https://example.test/fixture/document/1",
        retrieved_at=NOW,
        original_filename="x" * 201,
    )

    with pytest.raises(ResultTooComplexError, match="oversized string scalar"):
        await ManifestResultFinalizer(
            registry,
            inline_max_bytes=1024 * 1024,
            manifest_max_bytes=1024 * 1024,
            max_scalar_chars=200,
        ).finalize(command(), collected, cast(RuntimeAdapterContext, context))


@pytest.mark.asyncio
async def test_finalizer_default_matches_total_scalar_configuration_policy() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context = RecordingContext()
    collected = await FixtureAdapter().collect(request(), cast(AdapterContext, context))
    collected = collected.model_copy(update={"warnings": ["w" * 220_000] * 5})

    with pytest.raises(ResultTooComplexError, match="total scalar character limit"):
        await ManifestResultFinalizer(
            registry,
            inline_max_bytes=1,
            manifest_max_bytes=16 * 1024 * 1024,
        ).finalize(command(), collected, cast(RuntimeAdapterContext, context))


@pytest.mark.asyncio
async def test_manifest_finalizer_rejects_unregistered_adapter_artifact() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context = RecordingContext()
    collected = await FixtureAdapter().collect(request(), cast(AdapterContext, context))
    context.saved_artifacts.clear()

    with pytest.raises(ValueError, match="not registered"):
        await ManifestResultFinalizer(
            registry, inline_max_bytes=1, manifest_max_bytes=1024 * 1024
        ).finalize(command(), collected, cast(RuntimeAdapterContext, context))


@pytest.mark.asyncio
async def test_fixture_runtime_replays_exact_bytes_without_reexecution() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context_factory = RecordingContextFactory()
    runtime = processor(registry, context_factory)
    raw = command().model_dump_json().encode()

    first = await runtime.prepare(raw)
    duplicate = await runtime.prepare(raw)

    assert duplicate == first
    assert duplicate.body == first.body
    assert context_factory.calls == 1
    assert context_factory.context is not None
    assert len(context_factory.context.saved_content) == 1


@pytest.mark.asyncio
async def test_terminal_replay_boundary_forces_manifest_before_journal_write(
    tmp_path: Path,
) -> None:
    class LargeFixtureAdapter(FixtureAdapter):
        async def collect(
            self,
            request: CollectRequest,
            context: AdapterContext,
        ) -> CollectResult:
            result = await super().collect(request, context)
            record = result.records[0].model_copy(deep=True)
            record.raw_data["padding"] = "x" * 5000
            return result.model_copy(update={"records": [record]})

    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    replay = FileTerminalReplayStore(
        tmp_path,
        retention=timedelta(days=1),
        cleanup_interval=timedelta(minutes=1),
        max_entries=10,
        max_bytes=1024 * 1024,
        max_entry_bytes=4096,
        clock=lambda: NOW,
    )
    context_factory = RecordingContextFactory()
    runtime = SourceCommandProcessor(
        registry,
        TerminalMessageFactory(registry, clock=lambda: NOW + timedelta(seconds=5)),
        replay,
        context_factory,
        (AdapterRegistration(adapter=LargeFixtureAdapter()),),
        job_timeout=timedelta(minutes=1),
        result_finalizer=ManifestResultFinalizer(
            registry,
            inline_max_bytes=8192,
            manifest_max_bytes=1024 * 1024,
        ),
        clock=lambda: NOW,
    )

    prepared = await runtime.prepare(command().model_dump_json().encode())
    message = parse_source_message(decode_contract_json(prepared.body))

    assert isinstance(message, SourceCollectCompleted)
    assert message.payload.manifest is not None
    assert message.payload.records == []
    assert replay.entry_fits(sha256(command().model_dump_json().encode()).hexdigest(), prepared)


@pytest.mark.asyncio
async def test_manifest_limit_returns_contract_failure_with_complete_provenance() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context_factory = RecordingContextFactory()
    runtime = SourceCommandProcessor(
        registry,
        TerminalMessageFactory(registry, clock=lambda: NOW + timedelta(seconds=5)),
        VolatileTerminalReplayStore(),
        context_factory,
        (AdapterRegistration(adapter=FixtureAdapter()),),
        job_timeout=timedelta(minutes=1),
        result_finalizer=ManifestResultFinalizer(
            registry,
            inline_max_bytes=1,
            manifest_max_bytes=16,
        ),
        clock=lambda: NOW,
    )

    prepared = await runtime.prepare(command().model_dump_json().encode())
    message = parse_source_message(decode_contract_json(prepared.body))

    assert isinstance(message, SourceCollectFailed)
    assert message.payload.error.code is ErrorCode.CONTRACT_VALIDATION_FAILED
    assert message.payload.error.message == (
        "source result exceeds configured serialization limits"
    )
    assert len(message.payload.artifacts) == 1
    assert message.payload.artifacts[0].kind == "raw_response"


@pytest.mark.asyncio
async def test_oversized_single_scalar_is_rejected_before_manifest_encoding() -> None:
    class OversizedScalarAdapter(FixtureAdapter):
        async def collect(
            self,
            request: CollectRequest,
            context: AdapterContext,
        ) -> CollectResult:
            result = await super().collect(request, context)
            record = result.records[0].model_copy(deep=True)
            record.raw_data["oversized"] = "x" * 257
            return result.model_copy(update={"records": [record]})

    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context_factory = RecordingContextFactory()
    runtime = SourceCommandProcessor(
        registry,
        TerminalMessageFactory(registry, clock=lambda: NOW + timedelta(seconds=5)),
        VolatileTerminalReplayStore(),
        context_factory,
        (AdapterRegistration(adapter=OversizedScalarAdapter()),),
        job_timeout=timedelta(minutes=1),
        result_finalizer=ManifestResultFinalizer(
            registry,
            inline_max_bytes=1,
            manifest_max_bytes=1024 * 1024,
            max_scalar_chars=256,
        ),
        clock=lambda: NOW,
    )

    prepared = await runtime.prepare(command().model_dump_json().encode())
    message = parse_source_message(decode_contract_json(prepared.body))

    assert isinstance(message, SourceCollectFailed)
    assert message.payload.error.code is ErrorCode.CONTRACT_VALIDATION_FAILED
    assert message.payload.error.message == (
        "source result exceeds configured serialization limits"
    )
    assert len(message.payload.artifacts) == 1
    assert message.payload.artifacts[0].kind == "raw_response"
    assert context_factory.context is not None
    assert len(context_factory.context.saved_content) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fixture_case", "expected_artifact_kinds"),
    [
        ("not_found", {"raw_response"}),
        ("success", {"raw_response", "manifest"}),
    ],
)
async def test_oversized_terminal_warnings_fail_before_jcs_digest(
    fixture_case: str,
    expected_artifact_kinds: set[str],
) -> None:
    class WarningFixtureAdapter(FixtureAdapter):
        async def collect(
            self,
            request: CollectRequest,
            context: AdapterContext,
        ) -> CollectResult:
            result = await super().collect(request, context)
            return result.model_copy(update={"warnings": ["w" * 400] * 8})

    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context_factory = RecordingContextFactory()
    runtime = SourceCommandProcessor(
        registry,
        TerminalMessageFactory(registry, clock=lambda: NOW + timedelta(seconds=5)),
        VolatileTerminalReplayStore(),
        context_factory,
        (AdapterRegistration(adapter=WarningFixtureAdapter()),),
        job_timeout=timedelta(minutes=1),
        result_finalizer=ManifestResultFinalizer(
            registry,
            inline_max_bytes=512,
            terminal_max_bytes=2048,
            manifest_max_bytes=1024 * 1024,
        ),
        clock=lambda: NOW,
    )

    prepared = await runtime.prepare(command(fixture_case).model_dump_json().encode())
    message = parse_source_message(decode_contract_json(prepared.body))

    assert isinstance(message, SourceCollectFailed)
    assert message.payload.error.code is ErrorCode.CONTRACT_VALIDATION_FAILED
    assert message.payload.error.message == (
        "source result exceeds configured serialization limits"
    )
    assert {artifact.kind for artifact in message.payload.artifacts} == expected_artifact_kinds


@pytest.mark.asyncio
async def test_concurrent_finalization_near_aggregate_scalar_limit_is_isolated() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)

    async def finalize() -> tuple[SourceMessage, bytes]:
        context_factory = RecordingContextFactory()

        class BoundedWarningAdapter(FixtureAdapter):
            async def collect(
                self,
                request: CollectRequest,
                context: AdapterContext,
            ) -> CollectResult:
                result = await super().collect(request, context)
                return result.model_copy(update={"warnings": ["w" * 1000] * 4})

        runtime = SourceCommandProcessor(
            registry,
            TerminalMessageFactory(registry, clock=lambda: NOW + timedelta(seconds=5)),
            VolatileTerminalReplayStore(),
            context_factory,
            (AdapterRegistration(adapter=BoundedWarningAdapter()),),
            job_timeout=timedelta(minutes=1),
            result_finalizer=ManifestResultFinalizer(
                registry,
                inline_max_bytes=1,
                terminal_max_bytes=16 * 1024,
                manifest_max_bytes=1024 * 1024,
                max_total_scalar_chars=8 * 1024,
            ),
            clock=lambda: NOW,
        )
        prepared = await runtime.prepare(command().model_dump_json().encode())
        return parse_source_message(decode_contract_json(prepared.body)), prepared.body

    first, second = await asyncio.gather(finalize(), finalize())

    assert isinstance(first[0], SourceCollectCompleted)
    assert isinstance(second[0], SourceCollectCompleted)
    assert first[0].payload.manifest is not None
    assert second[0].payload.manifest is not None
    assert first[1] == second[1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsupported",
    [
        request().model_copy(update={"operation": "unsupported"}),
        request().model_copy(
            update={"query": SourceQuery(type="organization", name="Fixture Organization")}
        ),
        request("unknown"),
    ],
)
async def test_fixture_rejects_unsupported_request(unsupported: CollectRequest) -> None:
    with pytest.raises(PermanentAdapterError) as caught:
        await FixtureAdapter().collect(unsupported, cast(AdapterContext, RecordingContext()))

    assert caught.value.code is ErrorCode.SOURCE_UNSUPPORTED_OPERATION


@pytest.mark.asyncio
async def test_fixture_success_passes_runtime_and_json_schema_contract() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context_factory = RecordingContextFactory()

    prepared = await processor(registry, context_factory).prepare(
        command().model_dump_json().encode()
    )
    decoded = registry.validate_message_json(prepared.body)
    message = parse_source_message(decoded)

    assert isinstance(message, SourceCollectCompleted)
    assert message.payload.adapter_version == ADAPTER_VERSION
    assert message.payload.record_count == 1
    assert context_factory.context is not None
    assert message.payload.artifacts == context_factory.context.saved_artifacts


@pytest.mark.asyncio
async def test_fixture_not_found_passes_runtime_and_json_schema_contract() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context_factory = RecordingContextFactory()

    prepared = await processor(registry, context_factory).prepare(
        command("not_found").model_dump_json().encode()
    )
    decoded = registry.validate_message_json(prepared.body)
    message = parse_source_message(decoded)

    assert isinstance(message, SourceCollectCompleted)
    assert message.payload.outcome == "not_found"
    assert message.payload.record_count == 0
    assert message.payload.records == []
    assert message.payload.claims == []
    assert context_factory.context is not None
    assert message.payload.artifacts == context_factory.context.saved_artifacts


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fixture_case", "error_code", "artifact_count"),
    [
        ("transient", ErrorCode.SOURCE_UNAVAILABLE, 0),
        ("permanent", ErrorCode.SOURCE_INVALID_RESPONSE, 1),
    ],
)
async def test_fixture_errors_pass_runtime_and_json_schema_contract(
    fixture_case: str,
    error_code: ErrorCode,
    artifact_count: int,
) -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context_factory = RecordingContextFactory()

    prepared = await processor(registry, context_factory).prepare(
        command(fixture_case).model_dump_json().encode()
    )
    decoded = decode_contract_json(prepared.body)
    registry.validate_message(decoded)
    message = parse_source_message(decoded)

    assert isinstance(message, SourceCollectFailed)
    assert message.payload.error.code is error_code
    assert len(message.payload.artifacts) == artifact_count


@pytest.mark.asyncio
async def test_transient_retry_registers_manifest_recovered_before_adapter_call() -> None:
    registry = ContractSchemaRegistry(SCHEMA_ROOT)
    context = RecordingContext()
    await FixtureAdapter().collect(request(), cast(AdapterContext, context))
    await context.save_manifest(
        byte_chunks(b'{"schema_version":1}'),
        local_ref=MANIFEST_LOCAL_REF,
        source_uri="https://example.test/fixture/person/1",
        retrieved_at=NOW,
        max_bytes=1024,
    )

    class RecoveredContextFactory:
        async def create(
            self,
            *,
            job_id: UUID,
            attempt_no: int,
            correlation_id: UUID,
            deadline: datetime,
        ) -> RuntimeAdapterContext:
            del job_id, attempt_no, correlation_id, deadline
            return cast(RuntimeAdapterContext, context)

    runtime = SourceCommandProcessor(
        registry,
        TerminalMessageFactory(registry, clock=lambda: NOW + timedelta(seconds=5)),
        VolatileTerminalReplayStore(),
        RecoveredContextFactory(),
        (AdapterRegistration(adapter=FixtureAdapter()),),
        job_timeout=timedelta(minutes=1),
        clock=lambda: NOW,
    )

    prepared = await runtime.prepare(command("transient").model_dump_json().encode())
    message = parse_source_message(decode_contract_json(prepared.body))

    assert isinstance(message, SourceCollectFailed)
    assert message.payload.error.code is ErrorCode.SOURCE_UNAVAILABLE
    assert {artifact.kind for artifact in message.payload.artifacts} == {
        "raw_response",
        "manifest",
    }
