import asyncio
import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import aio_pika
import pytest
from aio_pika.abc import (
    AbstractConnection,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
)
from pydantic import AnyUrl

from graphx_workers.contracts.artifacts import ArtifactReference, artifact_object_key
from graphx_workers.contracts.messages import (
    CollectLimits,
    SourceCollectRequested,
    SourceCollectRequestedPayload,
    SourceQuery,
)
from graphx_workers.contracts.schema import ContractSchemaRegistry
from graphx_workers.runtime.processor import (
    AdapterContextFactory,
    AdapterRegistration,
    RuntimeAdapterContext,
    SourceCommandProcessor,
)
from graphx_workers.runtime.rabbitmq import (
    COMMANDS_EXCHANGE,
    EVENTS_EXCHANGE,
    REQUESTED_ROUTING_KEY,
    SOURCE_QUEUE,
    SourceRabbitRuntime,
)
from graphx_workers.runtime.replay import (
    FileTerminalReplayStore,
    ReplayStoreCapacityError,
    TerminalReplayStore,
    VolatileTerminalReplayStore,
)
from graphx_workers.runtime.terminal import TerminalMessageFactory
from graphx_workers.sdk import CollectRequest, CollectResult, SourceAdapter

ROOT = Path(__file__).resolve().parents[2]
TEST_URL = os.environ.get("RABBITMQ_TEST_URL")
NOW = datetime(2026, 11, 20, 0, 0, tzinfo=UTC)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(TEST_URL is None, reason="RABBITMQ_TEST_URL is not configured"),
]


class FakeContextFactory(AdapterContextFactory):
    async def create(
        self,
        *,
        job_id: UUID,
        attempt_no: int,
        correlation_id: UUID,
        deadline: datetime,
    ) -> RuntimeAdapterContext:
        context = type(
            "FakeContext",
            (),
            {
                "saved_artifacts": (),
                "job_id": job_id,
                "attempt_no": attempt_no,
                "correlation_id": correlation_id,
                "deadline": deadline,
            },
        )()
        return cast(RuntimeAdapterContext, context)


class FakeAdapter:
    def __init__(
        self,
        gate: asyncio.Event | None = None,
        *,
        ignore_first_cancellation: bool = False,
    ) -> None:
        self.gate = gate
        self.ignore_first_cancellation = ignore_first_cancellation
        self.calls = 0
        self.started = asyncio.Event()
        self.first_cancellation = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def collect(
        self, request: CollectRequest, context: RuntimeAdapterContext
    ) -> CollectResult:
        del request
        self.calls += 1
        self.started.set()
        try:
            if self.gate is not None:
                await self.gate.wait()
        except asyncio.CancelledError:
            self.first_cancellation.set()
            if self.ignore_first_cancellation:
                try:
                    await asyncio.Future[None]()
                except asyncio.CancelledError:
                    self.cancelled.set()
                    raise
            self.cancelled.set()
            raise
        return CollectResult(
            outcome="not_found",
            adapter_version="fixture-adapter/1",
            parser_version="fixture-parser/1",
            mapping_version="fixture-mapper/1",
            normalization_version="normalization/1",
            artifacts=[
                ArtifactReference(
                    local_ref="artifact:raw:1",
                    kind="raw_response",
                    object_key=artifact_object_key(
                        context.job_id, context.attempt_no, "artifact:raw:1"
                    ),
                    sha256="a" * 64,
                    media_type="application/json",
                    byte_size=2,
                    source_uri=AnyUrl("https://example.test/subject/1"),
                    retrieved_at=NOW,
                    published_at=None,
                    http_metadata={},
                )
            ],
        )


class RabbitConnectionProxy:
    def __init__(self) -> None:
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._handlers: set[asyncio.Task[None]] = set()

    @property
    def url(self) -> str:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("proxy is not running")
        port = self._server.sockets[0].getsockname()[1]
        return f"amqp://test_user:test_password@127.0.0.1:{port}/graphx_test"

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", 0)

    async def drop_connections(self) -> None:
        writers = tuple(self._writers)
        for writer in writers:
            writer.close()
        if writers:
            await asyncio.gather(*(writer.wait_closed() for writer in writers))

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        await self.drop_connections()
        if self._handlers:
            await asyncio.gather(*tuple(self._handlers), return_exceptions=True)

    async def _accept(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("proxy handler is not running in a task")
        self._handlers.add(task)
        upstream_writer: asyncio.StreamWriter | None = None
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection("127.0.0.1", 5673)
            self._writers.update((client_writer, upstream_writer))
            pipes = {
                asyncio.create_task(self._pipe(client_reader, upstream_writer)),
                asyncio.create_task(self._pipe(upstream_reader, client_writer)),
            }
            _, pending = await asyncio.wait(pipes, return_when=asyncio.FIRST_COMPLETED)
            for pipe in pending:
                pipe.cancel()
            await asyncio.gather(*pipes, return_exceptions=True)
        except ConnectionError, asyncio.IncompleteReadError:
            pass
        finally:
            client_writer.close()
            self._writers.discard(client_writer)
            if upstream_writer is not None:
                upstream_writer.close()
                self._writers.discard(upstream_writer)
            self._handlers.discard(task)

    @staticmethod
    async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while data := await reader.read(64 * 1024):
            writer.write(data)
            await writer.drain()


def command_bytes(*, message_id: UUID | None = None) -> bytes:
    command_id = message_id or uuid4()
    return (
        SourceCollectRequested(
            message_id=command_id,
            type=REQUESTED_ROUTING_KEY,
            schema_version=1,
            occurred_at=NOW,
            correlation_id=uuid4(),
            causation_id=None,
            producer="graphx-api",
            root_command_id=command_id,
            payload=SourceCollectRequestedPayload(
                job_id=uuid4(),
                attempt_no=1,
                investigation_id=uuid4(),
                source="fixture",
                operation="collect",
                purpose="subject_discovery",
                query=SourceQuery(type="person", name="Example"),
                limits=CollectLimits(max_records=10, max_bytes=1024, timeout_seconds=30),
                deadline=NOW + timedelta(days=1),
            ),
        )
        .model_dump_json()
        .encode()
    )


def build_runtime(
    adapter: FakeAdapter,
    *,
    prefetch: int = 1,
    concurrency: int | None = None,
    url: str | None = None,
    replay_store: TerminalReplayStore | None = None,
) -> SourceRabbitRuntime:
    assert TEST_URL is not None
    registry = ContractSchemaRegistry(ROOT / "contracts" / "schemas")
    processor = SourceCommandProcessor(
        registry,
        TerminalMessageFactory(registry, clock=lambda: NOW),
        replay_store or VolatileTerminalReplayStore(),
        FakeContextFactory(),
        (
            AdapterRegistration(
                source="fixture",
                version="fixture-adapter/1",
                adapter=cast(SourceAdapter, adapter),
            ),
        ),
        job_timeout=timedelta(minutes=1),
        clock=lambda: NOW,
    )
    return SourceRabbitRuntime(
        url=url or TEST_URL,
        processor=processor,
        prefetch=prefetch,
        concurrency=concurrency or prefetch,
        connect_timeout=timedelta(seconds=5),
        publish_timeout=timedelta(seconds=5),
        shutdown_timeout=timedelta(milliseconds=200),
        logger=logging.getLogger("rabbitmq-integration-test"),
    )


async def prepare_broker(
    connection: AbstractConnection,
) -> tuple[AbstractExchange, AbstractQueue]:
    channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
    commands = await channel.declare_exchange(
        COMMANDS_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    events = await channel.declare_exchange(
        EVENTS_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    source_queue = await channel.declare_queue(SOURCE_QUEUE, durable=True)
    await source_queue.bind(commands, routing_key=REQUESTED_ROUTING_KEY)
    await source_queue.purge()
    result_queue = await channel.declare_queue(exclusive=True, auto_delete=True)
    await result_queue.bind(events, routing_key="source.collect.*.v1")
    return commands, result_queue


async def publish_command(exchange: AbstractExchange, body: bytes) -> None:
    await exchange.publish(
        aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
        routing_key=REQUESTED_ROUTING_KEY,
        mandatory=True,
    )


async def wait_for_message(queue: AbstractQueue) -> AbstractIncomingMessage:
    async with asyncio.timeout(10):
        while True:
            message = await queue.get(fail=False, timeout=1)
            if message is not None:
                return message
            await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_runtime_confirms_result_before_ack_and_replays_duplicate() -> None:
    assert TEST_URL is not None
    connection = await aio_pika.connect(TEST_URL)
    stop = asyncio.Event()
    adapter = FakeAdapter()
    runtime = build_runtime(adapter)
    task = asyncio.create_task(runtime.run(stop))
    try:
        await runtime.wait_ready()
        commands, results = await prepare_broker(connection)
        raw = command_bytes()

        await publish_command(commands, raw)
        await publish_command(commands, raw)
        first = await wait_for_message(results)
        second = await wait_for_message(results)
        assert first.body == second.body
        await first.ack()
        await second.ack()
        assert adapter.calls == 1
    finally:
        stop.set()
        await task
        await connection.close()


@pytest.mark.asyncio
async def test_capacity_requeues_command_and_stops_runtime(tmp_path: Path) -> None:
    assert TEST_URL is not None
    connection = await aio_pika.connect(TEST_URL)
    stop = asyncio.Event()
    replay_store = FileTerminalReplayStore(
        tmp_path / "replay",
        retention=timedelta(days=30),
        cleanup_interval=timedelta(minutes=10),
        max_entries=1,
        max_bytes=1024 * 1024,
        max_entry_bytes=512 * 1024,
    )
    runtime = build_runtime(FakeAdapter(), replay_store=replay_store)
    task = asyncio.create_task(runtime.run(stop))
    try:
        await runtime.wait_ready()
        commands, results = await prepare_broker(connection)
        await publish_command(commands, command_bytes())
        first_result = await wait_for_message(results)
        await first_result.ack()
        blocked_command = command_bytes()

        await publish_command(commands, blocked_command)

        with pytest.raises(ReplayStoreCapacityError):
            await asyncio.wait_for(task, timeout=10)
        channel = await connection.channel()
        source_queue = await channel.declare_queue(SOURCE_QUEUE, passive=True)
        redelivered = await wait_for_message(source_queue)
        assert redelivered.body == blocked_command
        await redelivered.ack()
        await channel.close()
    finally:
        if not task.done():
            stop.set()
            await task
        await connection.close()


@pytest.mark.asyncio
async def test_shutdown_cancels_adapter_and_redelivers_unfinished_commands() -> None:
    assert TEST_URL is not None
    connection = await aio_pika.connect(TEST_URL)
    stop = asyncio.Event()
    gate = asyncio.Event()
    adapter = FakeAdapter(gate, ignore_first_cancellation=True)
    runtime = build_runtime(adapter, prefetch=1)
    task = asyncio.create_task(runtime.run(stop))
    try:
        await runtime.wait_ready()
        commands, _ = await prepare_broker(connection)
        await publish_command(commands, command_bytes())
        await publish_command(commands, command_bytes())
        await asyncio.wait_for(adapter.started.wait(), timeout=10)

        stop.set()
        await asyncio.wait_for(task, timeout=10)
        await asyncio.wait_for(adapter.first_cancellation.wait(), timeout=2)
        await asyncio.wait_for(adapter.cancelled.wait(), timeout=2)

        channel = await connection.channel()
        queue = await channel.declare_queue(SOURCE_QUEUE, passive=True)
        first = await wait_for_message(queue)
        second = await wait_for_message(queue)
        await first.ack()
        await second.ack()
        await channel.close()
    finally:
        if not task.done():
            stop.set()
            await task
        await connection.close()


@pytest.mark.asyncio
async def test_runtime_limits_processing_concurrency_below_prefetch() -> None:
    assert TEST_URL is not None
    connection = await aio_pika.connect(TEST_URL)
    stop = asyncio.Event()
    gate = asyncio.Event()
    adapter = FakeAdapter(gate)
    runtime = build_runtime(adapter, prefetch=2, concurrency=1)
    task = asyncio.create_task(runtime.run(stop))
    try:
        await runtime.wait_ready()
        commands, results = await prepare_broker(connection)
        await publish_command(commands, command_bytes())
        await publish_command(commands, command_bytes())
        await asyncio.wait_for(adapter.started.wait(), timeout=10)
        await asyncio.sleep(0.1)
        assert adapter.calls == 1

        gate.set()
        first = await wait_for_message(results)
        second = await wait_for_message(results)
        await first.ack()
        await second.ack()
        assert adapter.calls == 2
    finally:
        stop.set()
        await task
        await connection.close()


@pytest.mark.asyncio
async def test_broker_nack_redelivers_command_with_identical_terminal_bytes() -> None:
    assert TEST_URL is not None
    connection = await aio_pika.connect(TEST_URL)
    channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
    events = await channel.declare_exchange(
        EVENTS_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    blocker = await channel.declare_queue(
        exclusive=True,
        auto_delete=True,
        arguments={"x-max-length": 1, "x-overflow": "reject-publish"},
    )
    await blocker.bind(events, routing_key="source.collect.completed.v1")
    await events.publish(
        aio_pika.Message(b"block confirm"),
        routing_key="source.collect.completed.v1",
        mandatory=True,
    )

    stop = asyncio.Event()
    adapter = FakeAdapter()
    runtime = build_runtime(adapter)
    task = asyncio.create_task(runtime.run(stop))
    try:
        await runtime.wait_ready()
        commands, results = await prepare_broker(connection)
        await publish_command(commands, command_bytes())

        rejected_delivery = await wait_for_message(results)
        filler = await wait_for_message(blocker)
        await filler.ack()
        confirmed_delivery = await wait_for_message(blocker)
        assert rejected_delivery.body == confirmed_delivery.body
        await rejected_delivery.ack()
        await confirmed_delivery.ack()
        assert adapter.calls == 1

        await asyncio.sleep(0.1)
        stop.set()
        await task
        source_queue = await channel.declare_queue(SOURCE_QUEUE, passive=True)
        assert await source_queue.get(fail=False, timeout=1) is None
    finally:
        if not task.done():
            stop.set()
            await task
        await channel.close()
        await connection.close()


@pytest.mark.asyncio
async def test_connection_loss_before_confirm_redelivers_exact_result() -> None:
    assert TEST_URL is not None
    proxy = RabbitConnectionProxy()
    await proxy.start()
    connection = await aio_pika.connect(TEST_URL)
    stop = asyncio.Event()
    gate = asyncio.Event()
    adapter = FakeAdapter(gate)
    replay_store = VolatileTerminalReplayStore()
    runtime = build_runtime(adapter, url=proxy.url, replay_store=replay_store)
    task = asyncio.create_task(runtime.run(stop))
    try:
        await runtime.wait_ready()
        commands, results = await prepare_broker(connection)
        raw = command_bytes()
        command = SourceCollectRequested.model_validate_json(raw)
        await publish_command(commands, raw)
        await asyncio.wait_for(adapter.started.wait(), timeout=10)

        await proxy.drop_connections()
        gate.set()

        result = await wait_for_message(results)
        stored = await replay_store.load(command.message_id)
        assert stored is not None
        assert result.body == stored.message.body
        await result.ack()
        assert adapter.calls == 1
    finally:
        stop.set()
        await task
        await connection.close()
        await proxy.close()
