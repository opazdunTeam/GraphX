"""RabbitMQ transport for source collection commands and terminal results."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Literal, Protocol

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)

from graphx_workers.runtime.errors import RuntimeCapacityError, RuntimeFatalError
from graphx_workers.runtime.processor import InvalidCommandError, SourceCommandProcessor
from graphx_workers.runtime.replay import ReplayStoreCapacityError
from graphx_workers.runtime.terminal import PreparedTerminalMessage

COMMANDS_EXCHANGE = "commands.topic"
EVENTS_EXCHANGE = "events.topic"
SOURCE_QUEUE = "source.collect.q"
REQUESTED_ROUTING_KEY: Literal["source.collect.requested.v1"] = "source.collect.requested.v1"


class IncomingDelivery(Protocol):
    @property
    def body(self) -> bytes: ...

    async def ack(self) -> None: ...

    async def reject(self, *, requeue: bool = False) -> None: ...

    async def nack(self, *, requeue: bool = True) -> None: ...


class ResultPublisher(Protocol):
    async def publish(self, message: PreparedTerminalMessage) -> None: ...


class AioPikaResultPublisher:
    def __init__(self, exchange: AbstractExchange, *, timeout: timedelta) -> None:
        self._exchange = exchange
        self._timeout = timeout.total_seconds()

    async def publish(self, message: PreparedTerminalMessage) -> None:
        outgoing = aio_pika.Message(
            message.body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(message.message_id),
            correlation_id=str(message.correlation_id),
            type=message.message_type,
        )
        async with asyncio.timeout(self._timeout):
            await self._exchange.publish(
                outgoing,
                routing_key=message.message_type,
                mandatory=True,
            )


class SourceDeliveryHandler:
    def __init__(
        self,
        processor: SourceCommandProcessor,
        publisher: ResultPublisher,
        logger: logging.Logger,
        on_fatal_error: Callable[[RuntimeFatalError], None] | None = None,
    ) -> None:
        self._processor = processor
        self._publisher = publisher
        self._logger = logger
        self._on_fatal_error = on_fatal_error

    async def handle(self, delivery: IncomingDelivery) -> None:
        try:
            terminal = await self._processor.prepare(delivery.body)
        except InvalidCommandError:
            self._logger.warning("rejecting invalid source command")
            await delivery.reject(requeue=False)
            return
        except RuntimeFatalError as error:
            context: dict[str, object] = {"error_code": "RUNTIME_FATAL_ERROR"}
            if isinstance(error, ReplayStoreCapacityError):
                context.update(
                    error_code="REPLAY_CAPACITY_EXHAUSTED",
                    replay_entries=error.entries,
                    replay_bytes=error.bytes_used,
                )
            elif isinstance(error, RuntimeCapacityError):
                context["error_code"] = "RUNTIME_CAPACITY_EXHAUSTED"
            self._logger.critical(
                "runtime cannot continue safely; stopping consumer", extra={"context": context}
            )
            await self._nack_if_possible(delivery)
            if self._on_fatal_error is not None:
                self._on_fatal_error(error)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            self._logger.exception("source command processing failed before result publication")
            await self._nack_if_possible(delivery)
            return

        try:
            await self._publisher.publish(terminal)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._logger.warning(
                "terminal result was not confirmed; source command will be redelivered",
                extra={"context": {"message_id": str(terminal.message_id)}},
            )
            await self._nack_if_possible(delivery)
            return

        try:
            await delivery.ack()
        except Exception:
            self._logger.warning(
                "terminal result was confirmed but command ACK failed; duplicate delivery is safe",
                extra={"context": {"message_id": str(terminal.message_id)}},
            )

    async def _nack_if_possible(self, delivery: IncomingDelivery) -> None:
        try:
            await delivery.nack(requeue=True)
        except Exception:
            self._logger.warning("delivery channel closed before NACK; broker will redeliver")


ConnectionFactory = Callable[..., Awaitable[AbstractRobustConnection]]


class SourceRabbitRuntime:
    def __init__(
        self,
        *,
        url: str,
        processor: SourceCommandProcessor,
        prefetch: int,
        concurrency: int,
        connect_timeout: timedelta,
        publish_timeout: timedelta,
        shutdown_timeout: timedelta,
        logger: logging.Logger,
        connection_factory: ConnectionFactory = aio_pika.connect_robust,
    ) -> None:
        self._url = url
        self._processor = processor
        self._prefetch = prefetch
        self._concurrency = concurrency
        self._connect_timeout = connect_timeout.total_seconds()
        self._publish_timeout = publish_timeout
        self._shutdown_timeout = shutdown_timeout.total_seconds()
        self._logger = logger
        self._connection_factory = connection_factory
        self._connection: AbstractRobustConnection | None = None
        self._consumer_channel: AbstractChannel | None = None
        self._publisher_channel: AbstractChannel | None = None
        self._queue: AbstractQueue | None = None
        self._consumer_tag: str | None = None
        self._handler: SourceDeliveryHandler | None = None
        self._active: set[asyncio.Task[None]] = set()
        self._ready = asyncio.Event()
        self._fatal = asyncio.Event()
        self._fatal_error: BaseException | None = None
        self._slots = asyncio.Semaphore(concurrency)

    async def run(self, stop: asyncio.Event) -> None:
        fatal_error: BaseException | None = None
        try:
            await self._start()
            self._ready.set()
            self._logger.info(
                "source RabbitMQ runtime started",
                extra={
                    "context": {
                        "prefetch": self._prefetch,
                        "concurrency": self._concurrency,
                    }
                },
            )
            stop_wait = asyncio.create_task(stop.wait())
            fatal_wait = asyncio.create_task(self._fatal.wait())
            done, pending = await asyncio.wait(
                (stop_wait, fatal_wait),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if fatal_wait in done:
                fatal_error = self._fatal_error or RuntimeError("source runtime failed")
        finally:
            shutdown = asyncio.create_task(self._shutdown())
            try:
                await asyncio.shield(shutdown)
            except asyncio.CancelledError:
                await shutdown
                raise
        if fatal_error is not None:
            raise fatal_error

    async def wait_ready(self, *, timeout: float = 10.0) -> None:
        async with asyncio.timeout(timeout):
            await self._ready.wait()

    async def _start(self) -> None:
        self._connection = await self._connection_factory(
            self._url,
            timeout=self._connect_timeout,
        )
        self._consumer_channel = await self._connection.channel(publisher_confirms=False)
        await self._consumer_channel.set_qos(prefetch_count=self._prefetch)
        command_exchange = await self._consumer_channel.declare_exchange(
            COMMANDS_EXCHANGE,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        self._queue = await self._consumer_channel.declare_queue(SOURCE_QUEUE, durable=True)
        await self._queue.bind(command_exchange, routing_key=REQUESTED_ROUTING_KEY)

        self._publisher_channel = await self._connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        )
        event_exchange = await self._publisher_channel.declare_exchange(
            EVENTS_EXCHANGE,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        self._handler = SourceDeliveryHandler(
            self._processor,
            AioPikaResultPublisher(event_exchange, timeout=self._publish_timeout),
            self._logger,
            self._stop_for_capacity,
        )
        self._consumer_tag = await self._queue.consume(self._consume, no_ack=False)

    def _stop_for_capacity(self, error: RuntimeFatalError) -> None:
        if self._fatal_error is None:
            self._fatal_error = error
            self._ready.clear()
            self._fatal.set()

    async def _consume(self, message: AbstractIncomingMessage) -> None:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("RabbitMQ callback is not running in an asyncio task")
        self._active.add(task)
        try:
            if self._handler is None:
                raise RuntimeError("source delivery handler is not initialized")
            async with self._slots:
                await self._handler.handle(message)
        finally:
            self._active.discard(task)

    async def _shutdown(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._shutdown_timeout
        close_reserve = min(1.0, self._shutdown_timeout * 0.2)
        work_deadline = deadline - close_reserve

        if self._queue is not None and self._consumer_tag is not None:
            try:
                async with asyncio.timeout_at(work_deadline):
                    await self._queue.cancel(self._consumer_tag)
            except Exception:
                self._logger.warning("failed to cancel source consumer cleanly")
            self._consumer_tag = None

        active = set(self._active)
        pending: set[asyncio.Task[None]] = set()
        if active:
            _, pending = await asyncio.wait(
                active,
                timeout=max(work_deadline - loop.time(), 0.0),
            )
        await self._processor.cancel_inflight(
            timeout=max(work_deadline - loop.time(), 0.0),
        )
        if pending:
            for task in pending:
                task.cancel()
            await asyncio.wait(
                pending,
                timeout=max(work_deadline - loop.time(), 0.0),
            )

        await self._close_all(
            (self._consumer_channel, self._publisher_channel, self._connection),
            deadline=deadline,
        )
        self._logger.info("source RabbitMQ runtime stopped")

    @staticmethod
    async def _close_all(resources: tuple[object | None, ...], *, deadline: float) -> None:
        tasks: set[asyncio.Task[None]] = set()
        for resource in resources:
            if resource is None:
                continue
            close = getattr(resource, "close", None)
            if close is not None:
                tasks.add(asyncio.create_task(close()))
        if not tasks:
            return
        done, pending = await asyncio.wait(
            tasks,
            timeout=max(deadline - asyncio.get_running_loop().time(), 0.0),
        )
        for task in done:
            if not task.cancelled():
                task.exception()
        for task in pending:
            task.cancel()
