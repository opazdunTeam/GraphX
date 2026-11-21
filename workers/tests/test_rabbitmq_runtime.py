import asyncio
import logging
import os
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import portalocker
import pytest
from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from pydantic import AnyUrl

from graphx_workers.contracts.artifacts import ArtifactReference, artifact_object_key
from graphx_workers.contracts.errors import ErrorClassification, ErrorCode
from graphx_workers.contracts.messages import (
    CollectLimits,
    SourceCollectFailed,
    SourceCollectRequested,
    SourceCollectRequestedPayload,
    SourceQuery,
    parse_source_message,
)
from graphx_workers.contracts.schema import ContractSchemaRegistry, decode_contract_json
from graphx_workers.runtime.artifacts import ArtifactSession
from graphx_workers.runtime.errors import RuntimeFatalError
from graphx_workers.runtime.processor import (
    AdapterContextFactory,
    AdapterRegistration,
    InvalidCommandError,
    RuntimeAdapterContext,
    SourceCommandProcessor,
)
from graphx_workers.runtime.rabbitmq import ResultPublisher, SourceDeliveryHandler
from graphx_workers.runtime.replay import (
    FileTerminalReplayStore,
    ReplayStoreCapacityError,
    TerminalReplayStore,
    VolatileTerminalReplayStore,
)
from graphx_workers.runtime.s3 import (
    ImmutableS3Store,
    ObjectReconciliationError,
    ObjectStoreCapacityError,
    ObjectStoreOperationError,
)
from graphx_workers.runtime.terminal import PreparedTerminalMessage, TerminalMessageFactory
from graphx_workers.sdk import (
    ActionRequiredError,
    AdapterMetadata,
    CollectRequest,
    CollectResult,
    PermanentAdapterError,
    RateLimitedError,
    SourceAdapter,
    TransientAdapterError,
)
from graphx_workers.sdk.errors import AdapterError

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 11, 20, 0, 0, tzinfo=UTC)
JOB_ID = UUID("22222222-2222-4222-8222-222222222222")


def durable_replay_store(
    root: Path,
    *,
    retention: timedelta = timedelta(days=30),
    max_entries: int = 100,
    max_bytes: int = 10 * 1024 * 1024,
    max_entry_bytes: int = 1024 * 1024,
    clock: Callable[[], datetime] | None = None,
) -> FileTerminalReplayStore:
    return FileTerminalReplayStore(
        root,
        retention=retention,
        cleanup_interval=timedelta(seconds=1),
        max_entries=max_entries,
        max_bytes=max_bytes,
        max_entry_bytes=max_entry_bytes,
        clock=clock or (lambda: NOW),
    )


def schema_registry() -> ContractSchemaRegistry:
    return ContractSchemaRegistry(ROOT / "contracts" / "schemas")


def command_bytes(
    *,
    message_id: UUID | None = None,
    source: str = "fixture",
    operation: str = "collect",
    deadline: datetime | None = None,
) -> bytes:
    command_id = message_id or uuid4()
    command = SourceCollectRequested(
        message_id=command_id,
        type="source.collect.requested.v1",
        schema_version=1,
        occurred_at=NOW,
        correlation_id=uuid4(),
        causation_id=None,
        producer="graphx-api",
        root_command_id=command_id,
        payload=SourceCollectRequestedPayload(
            job_id=JOB_ID,
            attempt_no=1,
            investigation_id=uuid4(),
            source=source,
            operation=operation,
            purpose="subject_discovery",
            query=SourceQuery(type="person", name="Example"),
            limits=CollectLimits(max_records=10, max_bytes=1024, timeout_seconds=30),
            deadline=deadline or NOW + timedelta(minutes=1),
        ),
    )
    return command.model_dump_json().encode()


def source_artifact() -> ArtifactReference:
    return ArtifactReference(
        local_ref="artifact:raw:1",
        kind="raw_response",
        object_key=artifact_object_key(JOB_ID, 1, "artifact:raw:1"),
        sha256="a" * 64,
        media_type="application/json",
        byte_size=2,
        source_uri=AnyUrl("https://example.test/subject/1"),
        retrieved_at=NOW,
        published_at=None,
        http_metadata={},
    )


def not_found_result() -> CollectResult:
    return CollectResult(
        outcome="not_found",
        adapter_version="fixture-adapter/1",
        parser_version="fixture-parser/1",
        mapping_version="fixture-mapper/1",
        normalization_version="normalization/1",
        artifacts=[source_artifact()],
    )


class FakeContext:
    def __init__(self, artifacts: tuple[ArtifactReference, ...] = ()) -> None:
        self.saved_artifacts = artifacts
        self.job_id: UUID | None = None
        self.attempt_no: int | None = None
        self.correlation_id: UUID | None = None
        self.deadline: datetime | None = None


class FakeContextFactory(AdapterContextFactory):
    def __init__(self, context: FakeContext | None = None) -> None:
        self.context = context or FakeContext()
        self.calls = 0

    async def create(
        self,
        *,
        job_id: UUID,
        attempt_no: int,
        correlation_id: UUID,
        deadline: datetime,
    ) -> RuntimeAdapterContext:
        self.calls += 1
        self.context.job_id = job_id
        self.context.attempt_no = attempt_no
        self.context.correlation_id = correlation_id
        self.context.deadline = deadline
        return cast(RuntimeAdapterContext, self.context)


class FakeAdapter:
    metadata = AdapterMetadata(
        source="fixture",
        version="fixture-adapter/1",
        supported_operations=frozenset({"collect"}),
    )

    def __init__(
        self,
        result: CollectResult | None = None,
        error: Exception | None = None,
        gate: asyncio.Event | None = None,
    ) -> None:
        self.result = result or not_found_result()
        self.error = error
        self.gate = gate
        self.calls = 0
        self.started = asyncio.Event()

    async def collect(
        self, request: CollectRequest, context: RuntimeAdapterContext
    ) -> CollectResult:
        del request, context
        self.calls += 1
        self.started.set()
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error
        return self.result


class RegisterArtifactOnCancellationAdapter(FakeAdapter):
    async def collect(
        self, request: CollectRequest, context: RuntimeAdapterContext
    ) -> CollectResult:
        del request
        try:
            await asyncio.Future[None]()
        except asyncio.CancelledError:
            cast(FakeContext, context).saved_artifacts = (source_artifact(),)
            raise
        raise AssertionError("cancellation test adapter unexpectedly resumed")


class CancellationResistantAdapter(FakeAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()
        self.cancellations = 0

    async def collect(
        self, request: CollectRequest, context: RuntimeAdapterContext
    ) -> CollectResult:
        del request, context
        self.started.set()
        while not self.release.is_set():
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancellations += 1
        return self.result


class ServiceErrorS3Client:
    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        kwargs["Body"].read()
        raise ClientError(
            {
                "Error": {"Code": "SlowDown"},
                "ResponseMetadata": {"HTTPStatusCode": 503},
            },
            "PutObject",
        )

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        raise ClientError(
            {
                "Error": {"Code": "NoSuchKey"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            "HeadObject",
        )


class DelayedAccessDeniedS3Client(ServiceErrorS3Client):
    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        kwargs["Body"].read()
        time.sleep(0.03)
        raise ClientError(
            {
                "Error": {"Code": "AccessDenied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            "PutObject",
        )


class StoreWritingAdapter(FakeAdapter):
    def __init__(self, artifact_session: ArtifactSession) -> None:
        super().__init__()
        self.artifact_session = artifact_session

    async def collect(
        self, request: CollectRequest, context: RuntimeAdapterContext
    ) -> CollectResult:
        del request, context
        await self.artifact_session.save(
            b"evidence",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )
        return self.result


def processor(
    adapter: FakeAdapter,
    *,
    context_factory: AdapterContextFactory | None = None,
    job_timeout: timedelta = timedelta(minutes=1),
    replay_store: TerminalReplayStore | None = None,
    clock: Callable[[], datetime] | None = None,
) -> SourceCommandProcessor:
    registry = schema_registry()
    runtime_clock = clock or (lambda: NOW)
    return SourceCommandProcessor(
        registry,
        TerminalMessageFactory(registry, clock=runtime_clock),
        replay_store or VolatileTerminalReplayStore(),
        context_factory or FakeContextFactory(),
        (
            AdapterRegistration(
                adapter=cast(SourceAdapter, adapter),
            ),
        ),
        job_timeout=job_timeout,
        clock=runtime_clock,
    )


@pytest.mark.asyncio
async def test_processor_builds_valid_result_and_replays_exact_bytes() -> None:
    adapter = FakeAdapter()
    context_factory = FakeContextFactory()
    runtime = processor(adapter, context_factory=context_factory)
    raw = command_bytes()

    first, duplicate = await asyncio.gather(runtime.prepare(raw), runtime.prepare(raw))

    assert adapter.calls == 1
    assert first == duplicate
    document = schema_registry().validate_message_json(first.body)
    result = parse_source_message(document)
    assert result.type == "source.collect.completed.v1"
    assert result.message_id == first.message_id
    command = cast(SourceCollectRequested, parse_source_message(decode_contract_json(raw)))
    assert context_factory.context.job_id == command.payload.job_id
    assert context_factory.context.attempt_no == command.payload.attempt_no
    assert context_factory.context.correlation_id == command.correlation_id
    assert context_factory.context.deadline == NOW + timedelta(seconds=30)


@pytest.mark.asyncio
async def test_processor_rejects_operation_outside_adapter_metadata() -> None:
    adapter = FakeAdapter()
    context_factory = FakeContextFactory()

    prepared = await processor(adapter, context_factory=context_factory).prepare(
        command_bytes(operation="unsupported")
    )
    result = parse_source_message(decode_contract_json(prepared.body))

    assert isinstance(result, SourceCollectFailed)
    assert result.payload.error.code is ErrorCode.SOURCE_UNSUPPORTED_OPERATION
    assert result.payload.adapter_version == adapter.metadata.version
    assert adapter.calls == 0
    assert context_factory.calls == 0


@pytest.mark.asyncio
async def test_processor_rejects_result_version_outside_adapter_metadata() -> None:
    mismatched = not_found_result().model_copy(update={"adapter_version": "fixture-adapter/other"})
    adapter = FakeAdapter(result=mismatched)

    prepared = await processor(adapter).prepare(command_bytes())
    result = parse_source_message(decode_contract_json(prepared.body))

    assert isinstance(result, SourceCollectFailed)
    assert result.payload.error.code is ErrorCode.INTERNAL_ADAPTER_ERROR
    assert result.payload.adapter_version == adapter.metadata.version


@pytest.mark.asyncio
async def test_processor_rejects_message_id_reuse_with_different_bytes() -> None:
    adapter = FakeAdapter()
    runtime = processor(adapter)
    message_id = uuid4()
    await runtime.prepare(command_bytes(message_id=message_id))

    with pytest.raises(InvalidCommandError, match="different command bytes"):
        await runtime.prepare(command_bytes(message_id=message_id, source="other"))


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_start_duplicate_adapter_call() -> None:
    gate = asyncio.Event()
    adapter = FakeAdapter(gate=gate)
    runtime = processor(adapter)
    raw = command_bytes()
    first = asyncio.create_task(runtime.prepare(raw))
    await asyncio.wait_for(adapter.started.wait(), timeout=1)

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    duplicate = asyncio.create_task(runtime.prepare(raw))
    gate.set()

    result = await duplicate
    assert result.message_type == "source.collect.completed.v1"
    assert adapter.calls == 1


@pytest.mark.asyncio
async def test_durable_replay_survives_processor_restart(tmp_path: Path) -> None:
    replay_store = durable_replay_store(tmp_path / "replay")
    raw = command_bytes()
    first_adapter = FakeAdapter()
    first = await processor(first_adapter, replay_store=replay_store).prepare(raw)

    restarted_adapter = FakeAdapter()
    replayed = await processor(restarted_adapter, replay_store=replay_store).prepare(raw)

    assert replayed == first
    assert first_adapter.calls == 1
    assert restarted_adapter.calls == 0


@pytest.mark.asyncio
async def test_old_command_reloads_exact_durable_result(tmp_path: Path) -> None:
    replay_store = durable_replay_store(tmp_path / "replay")
    adapter = FakeAdapter()
    runtime = processor(
        adapter,
        replay_store=replay_store,
    )
    first_raw = command_bytes()
    first = await runtime.prepare(first_raw)
    await runtime.prepare(command_bytes())

    replayed = await runtime.prepare(first_raw)

    assert replayed == first
    assert adapter.calls == 2


@pytest.mark.asyncio
async def test_concurrent_processors_publish_single_durable_result(tmp_path: Path) -> None:
    replay_store = durable_replay_store(tmp_path / "replay")
    first_adapter = FakeAdapter()
    second_adapter = FakeAdapter(
        result=not_found_result().model_copy(update={"adapter_version": "fixture-adapter/2"})
    )
    raw = command_bytes()

    first, second = await asyncio.gather(
        processor(first_adapter, replay_store=replay_store).prepare(raw),
        processor(second_adapter, replay_store=replay_store).prepare(raw),
    )

    assert first == second
    assert first_adapter.calls + second_adapter.calls == 1


@pytest.mark.asyncio
async def test_replay_claim_recovers_after_owner_cancellation(tmp_path: Path) -> None:
    replay_store = durable_replay_store(tmp_path / "replay")
    gate = asyncio.Event()
    abandoned_adapter = FakeAdapter(gate=gate)
    abandoned = processor(abandoned_adapter, replay_store=replay_store)
    raw = command_bytes()
    first = asyncio.create_task(abandoned.prepare(raw))
    await asyncio.wait_for(abandoned_adapter.started.wait(), timeout=1)

    await abandoned.cancel_inflight(timeout=0.1)
    with pytest.raises(asyncio.CancelledError):
        await first

    recovered_adapter = FakeAdapter()
    result = await processor(recovered_adapter, replay_store=replay_store).prepare(raw)
    assert result.message_type == "source.collect.completed.v1"
    assert recovered_adapter.calls == 1


@pytest.mark.asyncio
async def test_shutdown_retains_unfinished_single_flight_task() -> None:
    adapter = CancellationResistantAdapter()
    runtime = processor(adapter)
    preparing = asyncio.create_task(runtime.prepare(command_bytes()))
    await asyncio.wait_for(adapter.started.wait(), timeout=1)

    try:
        await runtime.cancel_inflight(timeout=0.01)
        assert len(runtime._inflight) == 1
        assert adapter.cancellations >= 1
    finally:
        adapter.release.set()
    result = await asyncio.wait_for(preparing, timeout=1)
    assert result.message_type == "source.collect.completed.v1"
    assert runtime._inflight == {}


@pytest.mark.asyncio
async def test_stale_claim_repeats_metadata_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replay_store = durable_replay_store(tmp_path / "replay")
    command_id = uuid4()
    reserve_calls = 0
    identity_checks = 0
    original_reserve = replay_store._reserve_claim

    def tracking_reserve(candidate: UUID) -> Path:
        nonlocal reserve_calls
        reserve_calls += 1
        return original_reserve(candidate)

    def stale_once(lock: object, path: Path) -> bool:
        nonlocal identity_checks
        del lock, path
        identity_checks += 1
        return identity_checks > 1

    monkeypatch.setattr(replay_store, "_reserve_claim", tracking_reserve)
    monkeypatch.setattr(replay_store, "_claim_is_current", stale_once)

    async with replay_store.claim(command_id, timeout=1):
        pass

    assert reserve_calls == 2
    assert identity_checks == 2


@pytest.mark.asyncio
async def test_replay_claim_recovers_after_owner_process_dies(tmp_path: Path) -> None:
    replay_store = durable_replay_store(tmp_path / "replay")
    command_id = uuid4()
    claim_path = tmp_path / "replay" / f"{command_id}.lock"
    claim_path.parent.mkdir(parents=True)
    owner = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import portalocker,sys,time; "
                "lock=portalocker.Lock(sys.argv[1],mode='a+b',timeout=0); "
                "lock.acquire(); print('ready',flush=True); time.sleep(60)"
            ),
            str(claim_path),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert owner.stdout is not None
        assert owner.stdout.readline().strip() == "ready"
        owner.kill()
        owner.wait(timeout=5)

        async with replay_store.claim(command_id, timeout=1):
            pass
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.wait(timeout=5)


@pytest.mark.skipif(os.name == "nt", reason="POSIX permits unlinking an open lock inode")
def test_stale_unlinked_claim_inode_is_rejected(tmp_path: Path) -> None:
    replay_store = durable_replay_store(tmp_path / "replay")
    claim_path = tmp_path / "replay" / f"{uuid4()}.lock"
    claim_path.parent.mkdir(parents=True)
    stale_lock = portalocker.Lock(claim_path, mode="a+b", timeout=0)
    stale_lock.acquire()
    claim_path.unlink()
    current_lock = portalocker.Lock(claim_path, mode="a+b", timeout=0)
    current_lock.acquire()
    try:
        assert not replay_store._claim_is_current(stale_lock, claim_path)
        assert replay_store._claim_is_current(current_lock, claim_path)
    finally:
        current_lock.release()
        stale_lock.release()


@pytest.mark.asyncio
async def test_replay_cleanup_does_not_treat_old_command_as_processed(tmp_path: Path) -> None:
    current_time = [NOW]
    replay_store = durable_replay_store(
        tmp_path / "replay",
        retention=timedelta(hours=1),
        clock=lambda: current_time[0],
    )
    raw = command_bytes()
    await processor(FakeAdapter(), replay_store=replay_store).prepare(raw)
    old_timestamp = (NOW - timedelta(hours=2)).timestamp()
    for path in (tmp_path / "replay").glob("*"):
        os.utime(path, (old_timestamp, old_timestamp))
    current_time[0] = NOW + timedelta(hours=2)

    stats = await replay_store.cleanup()

    assert stats.entries == 0
    assert stats.bytes_used == 0
    late_adapter = FakeAdapter()
    terminal = await processor(
        late_adapter,
        replay_store=replay_store,
        clock=lambda: current_time[0],
    ).prepare(raw)
    result = parse_source_message(decode_contract_json(terminal.body))
    assert isinstance(result, SourceCollectFailed)
    assert result.payload.error.code is ErrorCode.SOURCE_TIMEOUT
    assert late_adapter.calls == 0


@pytest.mark.asyncio
async def test_replay_entry_quota_fails_before_adapter_execution(tmp_path: Path) -> None:
    replay_store = durable_replay_store(tmp_path / "replay", max_entries=1)
    await processor(FakeAdapter(), replay_store=replay_store).prepare(command_bytes())
    rejected_adapter = FakeAdapter()

    with pytest.raises(ReplayStoreCapacityError):
        await processor(rejected_adapter, replay_store=replay_store).prepare(command_bytes())

    assert rejected_adapter.calls == 0


@pytest.mark.asyncio
async def test_pending_claim_reserves_byte_quota_before_adapter_execution(
    tmp_path: Path,
) -> None:
    replay_store = durable_replay_store(
        tmp_path / "replay",
        max_entries=10,
        max_bytes=1024 * 1024,
    )
    gate = asyncio.Event()
    active_adapter = FakeAdapter(gate=gate)
    active = asyncio.create_task(
        processor(active_adapter, replay_store=replay_store).prepare(command_bytes())
    )
    await asyncio.wait_for(active_adapter.started.wait(), timeout=1)
    rejected_adapter = FakeAdapter()

    with pytest.raises(ReplayStoreCapacityError):
        await processor(rejected_adapter, replay_store=replay_store).prepare(command_bytes())

    assert rejected_adapter.calls == 0
    gate.set()
    await active


@pytest.mark.asyncio
async def test_oversized_terminal_is_replaced_with_replayable_failure(tmp_path: Path) -> None:
    replay_store = durable_replay_store(
        tmp_path / "replay",
        max_entry_bytes=4096,
    )
    saved_artifacts = (source_artifact(),)
    oversized_result = not_found_result().model_copy(update={"warnings": ["x" * 10_000]})
    context_factory = FakeContextFactory(FakeContext(saved_artifacts))
    raw = command_bytes()
    first_adapter = FakeAdapter(result=oversized_result)

    first = await processor(
        first_adapter,
        replay_store=replay_store,
        context_factory=context_factory,
    ).prepare(raw)
    replay_adapter = FakeAdapter()
    replayed = await processor(replay_adapter, replay_store=replay_store).prepare(raw)

    result = parse_source_message(decode_contract_json(first.body))
    assert isinstance(result, SourceCollectFailed)
    assert result.payload.error.code is ErrorCode.CONTRACT_VALIDATION_FAILED
    assert result.payload.adapter_version == "fixture-adapter/1"
    assert result.payload.artifacts == list(saved_artifacts)
    assert replayed == first
    assert first_adapter.calls == 1
    assert replay_adapter.calls == 0


@pytest.mark.asyncio
async def test_oversized_failure_never_publishes_partial_artifact_list(tmp_path: Path) -> None:
    replay_store = durable_replay_store(
        tmp_path / "replay",
        max_entry_bytes=4096,
    )
    saved_artifacts = tuple(
        source_artifact().model_copy(
            update={
                "local_ref": f"artifact:raw:{index}",
                "object_key": artifact_object_key(JOB_ID, 1, f"artifact:raw:{index}"),
            }
        )
        for index in range(100)
    )
    oversized_result = not_found_result().model_copy(update={"artifacts": list(saved_artifacts)})
    context_factory = FakeContextFactory(FakeContext(saved_artifacts))
    runtime = processor(
        FakeAdapter(result=oversized_result),
        replay_store=replay_store,
        context_factory=context_factory,
    )
    events: list[str] = []
    failures: list[RuntimeFatalError] = []
    publisher = FakePublisher(events)
    handler = SourceDeliveryHandler(
        runtime,
        publisher,
        logging.getLogger("test"),
        failures.append,
    )

    await handler.handle(FakeDelivery(command_bytes(), events))

    assert events == ["nack:True"]
    assert publisher.messages == []
    assert len(failures) == 1


@pytest.mark.asyncio
async def test_uncertain_artifact_persistence_requeues_without_terminal_result() -> None:
    events: list[str] = []
    delivery = FakeDelivery(command_bytes(), events)
    publisher = FakePublisher(events)
    handler = SourceDeliveryHandler(
        processor(
            FakeAdapter(
                error=ObjectReconciliationError(
                    "conditional artifact write outcome is still uncertain"
                )
            )
        ),
        publisher,
        logging.getLogger("test"),
    )

    await handler.handle(delivery)

    assert events == ["nack:True"]
    assert publisher.messages == []


@pytest.mark.asyncio
async def test_s3_service_client_error_requeues_without_terminal_result() -> None:
    artifact_session = ArtifactSession(
        ImmutableS3Store(
            cast(Any, ServiceErrorS3Client()),
            "artifacts",
            require_versioning=False,
            reconciliation_timeout=0.01,
        ),
        job_id=JOB_ID,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )
    events: list[str] = []
    delivery = FakeDelivery(command_bytes(), events)
    publisher = FakePublisher(events)
    handler = SourceDeliveryHandler(
        processor(StoreWritingAdapter(artifact_session)),
        publisher,
        logging.getLogger("test"),
    )

    await handler.handle(delivery)

    assert events == ["nack:True"]
    assert publisher.messages == []


@pytest.mark.asyncio
async def test_deadline_during_s3_fatal_error_requeues_and_stops_consumer() -> None:
    artifact_session = ArtifactSession(
        ImmutableS3Store(
            cast(Any, DelayedAccessDeniedS3Client()),
            "artifacts",
            require_versioning=False,
        ),
        job_id=JOB_ID,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )
    events: list[str] = []
    failures: list[RuntimeFatalError] = []
    publisher = FakePublisher(events)
    handler = SourceDeliveryHandler(
        processor(
            StoreWritingAdapter(artifact_session),
            job_timeout=timedelta(milliseconds=5),
        ),
        publisher,
        logging.getLogger("test"),
        failures.append,
    )

    await handler.handle(FakeDelivery(command_bytes(), events))

    assert events == ["nack:True"]
    assert publisher.messages == []
    assert len(failures) == 1
    assert isinstance(failures[0], ObjectStoreOperationError)


@pytest.mark.asyncio
async def test_object_store_capacity_requeues_and_stops_consumer() -> None:
    events: list[str] = []
    failures: list[RuntimeFatalError] = []
    publisher = FakePublisher(events)
    handler = SourceDeliveryHandler(
        processor(FakeAdapter(error=ObjectStoreCapacityError("S3 capacity exhausted"))),
        publisher,
        logging.getLogger("test"),
        failures.append,
    )

    await handler.handle(FakeDelivery(command_bytes(), events))

    assert events == ["nack:True"]
    assert publisher.messages == []
    assert len(failures) == 1


@pytest.mark.asyncio
async def test_object_store_configuration_error_requeues_and_stops_consumer() -> None:
    events: list[str] = []
    failures: list[RuntimeFatalError] = []
    publisher = FakePublisher(events)
    handler = SourceDeliveryHandler(
        processor(FakeAdapter(error=ObjectStoreOperationError("S3 access denied"))),
        publisher,
        logging.getLogger("test"),
        failures.append,
    )

    await handler.handle(FakeDelivery(command_bytes(), events))

    assert events == ["nack:True"]
    assert publisher.messages == []
    assert len(failures) == 1


@pytest.mark.asyncio
async def test_deadline_failure_contains_artifact_registered_during_cancellation() -> None:
    context = FakeContextFactory()
    prepared = await processor(
        RegisterArtifactOnCancellationAdapter(),
        context_factory=context,
        job_timeout=timedelta(milliseconds=10),
    ).prepare(command_bytes(deadline=NOW + timedelta(minutes=1)))

    result = parse_source_message(decode_contract_json(prepared.body))
    assert isinstance(result, SourceCollectFailed)
    assert result.payload.error.code is ErrorCode.SOURCE_TIMEOUT
    assert result.payload.artifacts == [source_artifact()]


@pytest.mark.asyncio
async def test_replay_hot_path_does_not_scan_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replay_store = durable_replay_store(tmp_path / "replay")
    await replay_store.cleanup()
    runtime = processor(FakeAdapter(), replay_store=replay_store)

    def fail_glob(path: Path, pattern: str) -> list[Path]:
        del path, pattern
        raise AssertionError("journal scan entered command hot path")

    monkeypatch.setattr(Path, "glob", fail_glob)
    terminal = await runtime.prepare(command_bytes())

    assert terminal.message_type == "source.collect.completed.v1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "classification"),
    [
        (
            TransientAdapterError(ErrorCode.SOURCE_NETWORK_ERROR, "network unavailable"),
            ErrorClassification.TRANSIENT,
        ),
        (
            RateLimitedError(
                ErrorCode.SOURCE_RATE_LIMITED,
                "source rate limited",
                retry_after_seconds=30,
            ),
            ErrorClassification.RATE_LIMITED,
        ),
        (
            PermanentAdapterError(ErrorCode.SOURCE_INVALID_RESPONSE, "response is invalid"),
            ErrorClassification.PERMANENT,
        ),
        (
            ActionRequiredError(ErrorCode.SOURCE_ACTION_REQUIRED, "manual action required"),
            ErrorClassification.ACTION_REQUIRED,
        ),
    ],
)
async def test_processor_maps_typed_adapter_errors(
    error: AdapterError,
    classification: ErrorClassification,
) -> None:
    context = FakeContextFactory(FakeContext((source_artifact(),)))
    prepared = await processor(FakeAdapter(error=error), context_factory=context).prepare(
        command_bytes()
    )

    result = parse_source_message(decode_contract_json(prepared.body))
    assert isinstance(result, SourceCollectFailed)
    assert result.payload.error.classification is classification
    assert result.payload.artifacts == [source_artifact()]


@pytest.mark.asyncio
async def test_expired_command_does_not_call_adapter() -> None:
    adapter = FakeAdapter()
    prepared = await processor(adapter).prepare(command_bytes(deadline=NOW - timedelta(seconds=1)))

    result = parse_source_message(decode_contract_json(prepared.body))
    assert isinstance(result, SourceCollectFailed)
    assert result.payload.error.code is ErrorCode.SOURCE_TIMEOUT
    assert adapter.calls == 0


@pytest.mark.asyncio
async def test_context_receives_effective_deadline() -> None:
    context_factory = FakeContextFactory()
    await processor(
        FakeAdapter(),
        context_factory=context_factory,
        job_timeout=timedelta(seconds=5),
    ).prepare(command_bytes(deadline=NOW + timedelta(hours=1)))

    assert context_factory.context.deadline == NOW + timedelta(seconds=5)


class HangingContextFactory(AdapterContextFactory):
    async def create(
        self,
        *,
        job_id: UUID,
        attempt_no: int,
        correlation_id: UUID,
        deadline: datetime,
    ) -> RuntimeAdapterContext:
        del job_id, attempt_no, correlation_id, deadline
        await asyncio.Future[None]()
        raise AssertionError("unreachable")


@pytest.mark.asyncio
async def test_context_creation_uses_job_timeout_budget() -> None:
    adapter = FakeAdapter()
    prepared = await processor(
        adapter,
        context_factory=HangingContextFactory(),
        job_timeout=timedelta(milliseconds=1),
    ).prepare(command_bytes(deadline=NOW + timedelta(hours=1)))

    result = parse_source_message(decode_contract_json(prepared.body))
    assert isinstance(result, SourceCollectFailed)
    assert result.payload.error.code is ErrorCode.SOURCE_TIMEOUT
    assert adapter.calls == 0


class FakeDelivery:
    def __init__(self, body: bytes, events: list[str]) -> None:
        self.body = body
        self.events = events

    async def ack(self) -> None:
        self.events.append("ack")

    async def reject(self, *, requeue: bool = False) -> None:
        self.events.append(f"reject:{requeue}")

    async def nack(self, *, requeue: bool = True) -> None:
        self.events.append(f"nack:{requeue}")


class FakePublisher(ResultPublisher):
    def __init__(self, events: list[str], *, fail: bool = False) -> None:
        self.events = events
        self.fail = fail
        self.messages: list[PreparedTerminalMessage] = []

    async def publish(self, message: PreparedTerminalMessage) -> None:
        self.events.append("publish")
        self.messages.append(message)
        if self.fail:
            raise ConnectionError("confirm was lost")
        self.events.append("confirm")


@pytest.mark.asyncio
async def test_delivery_is_acked_only_after_confirm() -> None:
    events: list[str] = []
    delivery = FakeDelivery(command_bytes(), events)
    handler = SourceDeliveryHandler(
        processor(FakeAdapter()),
        FakePublisher(events),
        logging.getLogger("test"),
    )

    await handler.handle(delivery)

    assert events == ["publish", "confirm", "ack"]


@pytest.mark.asyncio
async def test_lost_confirm_nacks_command_for_redelivery() -> None:
    events: list[str] = []
    delivery = FakeDelivery(command_bytes(), events)
    handler = SourceDeliveryHandler(
        processor(FakeAdapter()),
        FakePublisher(events, fail=True),
        logging.getLogger("test"),
    )

    await handler.handle(delivery)

    assert events == ["publish", "nack:True"]


@pytest.mark.asyncio
async def test_invalid_command_is_rejected_without_requeue() -> None:
    events: list[str] = []
    delivery = FakeDelivery(b"not-json", events)
    handler = SourceDeliveryHandler(
        processor(FakeAdapter()),
        FakePublisher(events),
        logging.getLogger("test"),
    )

    await handler.handle(delivery)

    assert events == ["reject:False"]


@pytest.mark.asyncio
async def test_replay_capacity_requeues_delivery_and_stops_consumer(tmp_path: Path) -> None:
    replay_store = durable_replay_store(tmp_path / "replay", max_entries=1)
    runtime = processor(FakeAdapter(), replay_store=replay_store)
    await runtime.prepare(command_bytes())
    events: list[str] = []
    failures: list[RuntimeFatalError] = []
    publisher = FakePublisher(events)
    handler = SourceDeliveryHandler(
        runtime,
        publisher,
        logging.getLogger("test"),
        failures.append,
    )

    await handler.handle(FakeDelivery(command_bytes(), events))

    assert events == ["nack:True"]
    assert publisher.messages == []
    assert len(failures) == 1


def test_typed_error_rejects_wrong_classification() -> None:
    with pytest.raises(ValueError, match="not a transient"):
        TransientAdapterError(ErrorCode.SOURCE_ACCESS_DENIED, "access denied")


def test_adapter_error_enforces_transport_message_boundary() -> None:
    detail = "x" * 2_000
    assert AdapterError(ErrorCode.INTERNAL_ADAPTER_ERROR, detail).safe_detail == detail

    with pytest.raises(ValueError, match="must not exceed 2000"):
        AdapterError(ErrorCode.INTERNAL_ADAPTER_ERROR, f"{detail}x")
