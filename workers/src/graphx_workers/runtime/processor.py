"""Transport-independent source command processing."""

import asyncio
import logging
from collections.abc import AsyncIterable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Protocol
from uuid import UUID

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.errors import ErrorCode
from graphx_workers.contracts.messages import (
    SourceCollectCompleted,
    SourceCollectFailed,
    SourceCollectRequested,
    parse_source_message,
)
from graphx_workers.contracts.results import SourceResult
from graphx_workers.contracts.schema import ContractSchemaRegistry
from graphx_workers.runtime.errors import RuntimeInfrastructureUncertainError
from graphx_workers.runtime.replay import (
    ReplayEntryTooLargeError,
    StoredTerminalMessage,
    TerminalReplayStore,
)
from graphx_workers.runtime.requests import to_collect_request
from graphx_workers.runtime.results import (
    ResultSerializationLimitError,
    build_inline_source_result,
)
from graphx_workers.runtime.terminal import PreparedTerminalMessage, TerminalMessageFactory
from graphx_workers.sdk.adapter import (
    AdapterContext,
    AdapterMetadata,
    CollectResult,
    SourceAdapter,
)
from graphx_workers.sdk.errors import AdapterError

RUNTIME_VERSION = "graphx-worker-runtime/1"


class InvalidCommandError(ValueError):
    """The delivery is permanently invalid and must not be requeued."""


class RuntimeAdapterContext(AdapterContext, Protocol):
    @property
    def saved_artifacts(self) -> Sequence[ArtifactReference]: ...

    async def save_manifest(
        self,
        content: AsyncIterable[bytes],
        *,
        local_ref: str,
        source_uri: str,
        retrieved_at: datetime,
        max_bytes: int,
    ) -> ArtifactReference: ...


class AdapterContextFactory(Protocol):
    async def create(
        self,
        *,
        job_id: UUID,
        attempt_no: int,
        correlation_id: UUID,
        deadline: datetime,
    ) -> RuntimeAdapterContext: ...


class ResultFinalizer(Protocol):
    async def finalize(
        self,
        command: SourceCollectRequested,
        collected: CollectResult,
        context: RuntimeAdapterContext,
        *,
        force_manifest: bool = False,
    ) -> SourceResult: ...


class InlineResultFinalizer:
    async def finalize(
        self,
        command: SourceCollectRequested,
        collected: CollectResult,
        context: RuntimeAdapterContext,
        *,
        force_manifest: bool = False,
    ) -> SourceResult:
        del context, force_manifest
        return build_inline_source_result(
            command.payload.job_id,
            command.payload.attempt_no,
            collected,
        )


@dataclass(frozen=True, slots=True)
class AdapterRegistration:
    adapter: SourceAdapter
    metadata: AdapterMetadata = field(init=False)

    def __post_init__(self) -> None:
        metadata = getattr(self.adapter, "metadata", None)
        if not isinstance(metadata, AdapterMetadata):
            raise TypeError("source adapter must expose AdapterMetadata")
        object.__setattr__(self, "metadata", metadata)

    @property
    def source(self) -> str:
        return self.metadata.source

    @property
    def version(self) -> str:
        return self.metadata.version


@dataclass(frozen=True, slots=True)
class _ExecutionOutcome:
    message: PreparedTerminalMessage
    adapter_version: str
    artifacts: tuple[ArtifactReference, ...]


class SourceCommandProcessor:
    def __init__(
        self,
        schema_registry: ContractSchemaRegistry,
        terminal_factory: TerminalMessageFactory,
        replay_store: TerminalReplayStore,
        context_factory: AdapterContextFactory,
        adapters: Sequence[AdapterRegistration],
        *,
        job_timeout: timedelta,
        result_finalizer: ResultFinalizer | None = None,
        clock: Callable[[], datetime] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        adapter_map = {registration.source: registration for registration in adapters}
        if len(adapter_map) != len(adapters):
            raise ValueError("source adapter registrations must be unique")
        self._schema_registry = schema_registry
        self._terminal_factory = terminal_factory
        self._replay_store = replay_store
        self._context_factory = context_factory
        self._adapters: Mapping[str, AdapterRegistration] = adapter_map
        self._job_timeout = job_timeout
        self._result_finalizer = result_finalizer or InlineResultFinalizer()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._logger = logger or logging.getLogger(__name__)
        self._inflight: dict[UUID, tuple[str, asyncio.Task[PreparedTerminalMessage]]] = {}
        self._lock = asyncio.Lock()

    async def prepare(self, raw: bytes) -> PreparedTerminalMessage:
        command = self._parse_command(raw)
        fingerprint = sha256(raw).hexdigest()
        message_id = command.message_id

        stored = await self._replay_store.load(message_id)
        if stored is not None:
            self._require_same_delivery(stored.command_fingerprint, fingerprint)
            self._validate_replay(command, stored.message)
            return stored.message

        async with self._lock:
            existing = self._inflight.get(message_id)
            if existing is not None:
                self._require_same_delivery(existing[0], fingerprint)
                task = existing[1]
            else:
                task = asyncio.create_task(self._execute_claimed(command, fingerprint))
                self._inflight[message_id] = (fingerprint, task)

        try:
            prepared = await asyncio.shield(task)
        except BaseException:
            if task.done():
                async with self._lock:
                    current = self._inflight.get(message_id)
                    if current is not None and current[1] is task:
                        self._inflight.pop(message_id, None)
            raise

        async with self._lock:
            current = self._inflight.get(message_id)
            if current is not None and current[1] is task:
                self._inflight.pop(message_id, None)
        return prepared

    async def _execute_claimed(
        self,
        command: SourceCollectRequested,
        fingerprint: str,
    ) -> PreparedTerminalMessage:
        async with self._replay_store.claim(
            command.message_id,
            timeout=self._job_timeout.total_seconds(),
        ):
            stored = await self._replay_store.load(command.message_id)
            if stored is not None:
                self._require_same_delivery(stored.command_fingerprint, fingerprint)
                self._validate_replay(command, stored.message)
                return stored.message

            outcome = await self._execute(command, fingerprint)
            try:
                stored = await self._replay_store.put_if_absent(
                    command.message_id,
                    fingerprint,
                    outcome.message,
                )
            except ReplayEntryTooLargeError as error:
                stored = await self._store_oversized_failure(
                    command,
                    fingerprint,
                    outcome,
                    error,
                )
            self._require_same_delivery(stored.command_fingerprint, fingerprint)
            self._validate_replay(command, stored.message)
            return stored.message

    async def cancel_inflight(self, *, timeout: float) -> None:
        """Cancel adapter calls that must not outlive runtime shutdown."""
        async with self._lock:
            tasks = {entry[1] for entry in self._inflight.values()}
        for task in tasks:
            task.cancel()
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=max(timeout, 0.0))
            for task in pending:
                task.cancel()
            async with self._lock:
                for message_id, entry in tuple(self._inflight.items()):
                    if entry[1] in tasks and entry[1].done():
                        self._inflight.pop(message_id, None)

    def _parse_command(self, raw: bytes) -> SourceCollectRequested:
        try:
            value = self._schema_registry.validate_message_json(raw)
            message = parse_source_message(value)
        except Exception as error:
            raise InvalidCommandError("invalid source command") from error
        if not isinstance(message, SourceCollectRequested):
            raise InvalidCommandError("delivery is not a source collection command")
        return message

    async def _execute(
        self,
        command: SourceCollectRequested,
        fingerprint: str,
    ) -> _ExecutionOutcome:
        registration = self._adapters.get(command.payload.source)
        if registration is None:
            return _ExecutionOutcome(
                message=self._terminal_factory.failed(
                    command,
                    adapter_version=RUNTIME_VERSION,
                    artifacts=(),
                    error=AdapterError(
                        ErrorCode.SOURCE_UNSUPPORTED_OPERATION,
                        "source adapter is not available",
                    ),
                ),
                adapter_version=RUNTIME_VERSION,
                artifacts=(),
            )

        if command.payload.operation not in registration.metadata.supported_operations:
            return _ExecutionOutcome(
                message=self._terminal_factory.failed(
                    command,
                    adapter_version=registration.version,
                    artifacts=(),
                    error=AdapterError(
                        ErrorCode.SOURCE_UNSUPPORTED_OPERATION,
                        "source operation is not supported by the adapter",
                    ),
                ),
                adapter_version=registration.version,
                artifacts=(),
            )

        context: RuntimeAdapterContext | None = None
        try:
            now = self._clock()
            effective_deadline = min(
                command.payload.deadline,
                now + timedelta(seconds=command.payload.limits.timeout_seconds),
                now + self._job_timeout,
            )
            remaining = effective_deadline - now
            if remaining <= timedelta(0):
                raise TimeoutError
            async with asyncio.timeout(remaining.total_seconds()):
                context = await self._context_factory.create(
                    job_id=command.payload.job_id,
                    attempt_no=command.payload.attempt_no,
                    correlation_id=command.correlation_id,
                    deadline=effective_deadline,
                )
                collected = await registration.adapter.collect(
                    to_collect_request(command.payload),
                    context,
                )
                if collected.adapter_version != registration.version:
                    raise ValueError("adapter result version does not match its metadata")
                result = await self._result_finalizer.finalize(command, collected, context)
                completed = self._terminal_factory.completed(command, result)
                if (
                    result.outcome == "succeeded"
                    and result.manifest is None
                    and not self._replay_store.entry_fits(fingerprint, completed)
                ):
                    result = await self._result_finalizer.finalize(
                        command,
                        collected,
                        context,
                        force_manifest=True,
                    )
                    completed = self._terminal_factory.completed(command, result)
            artifacts = self._artifacts(context)
            return _ExecutionOutcome(
                message=completed,
                adapter_version=registration.version,
                artifacts=artifacts,
            )
        except AdapterError as error:
            artifacts = self._artifacts(context)
            return _ExecutionOutcome(
                message=self._terminal_factory.failed(
                    command,
                    adapter_version=registration.version,
                    artifacts=artifacts,
                    error=error,
                ),
                adapter_version=registration.version,
                artifacts=artifacts,
            )
        except TimeoutError:
            artifacts = self._artifacts(context)
            return _ExecutionOutcome(
                message=self._terminal_factory.failed(
                    command,
                    adapter_version=registration.version,
                    artifacts=artifacts,
                    error=AdapterError(ErrorCode.SOURCE_TIMEOUT, "source collection timed out"),
                ),
                adapter_version=registration.version,
                artifacts=artifacts,
            )
        except RuntimeInfrastructureUncertainError:
            # A PUT may have committed even though its result is not observable yet.
            # Publishing a terminal receipt here could orphan immutable evidence.
            raise
        except ResultSerializationLimitError:
            artifacts = self._artifacts(context)
            return _ExecutionOutcome(
                message=self._terminal_factory.failed(
                    command,
                    adapter_version=registration.version,
                    artifacts=artifacts,
                    error=AdapterError(
                        ErrorCode.CONTRACT_VALIDATION_FAILED,
                        "source result exceeds configured serialization limits",
                    ),
                ),
                adapter_version=registration.version,
                artifacts=artifacts,
            )
        except Exception:
            self._logger.exception(
                "source adapter failed unexpectedly",
                extra={
                    "context": {
                        "job_id": str(command.payload.job_id),
                        "attempt_no": command.payload.attempt_no,
                        "source": command.payload.source,
                    }
                },
            )
            artifacts = self._artifacts(context)
            return _ExecutionOutcome(
                message=self._terminal_factory.failed(
                    command,
                    adapter_version=registration.version,
                    artifacts=artifacts,
                    error=AdapterError(
                        ErrorCode.INTERNAL_ADAPTER_ERROR,
                        "source adapter failed unexpectedly",
                    ),
                ),
                adapter_version=registration.version,
                artifacts=artifacts,
            )

    async def _store_oversized_failure(
        self,
        command: SourceCollectRequested,
        fingerprint: str,
        outcome: _ExecutionOutcome,
        original_error: ReplayEntryTooLargeError,
    ) -> StoredTerminalMessage:
        occurred_at = self._clock()
        candidate = self._terminal_factory.failed(
            command,
            adapter_version=outcome.adapter_version,
            artifacts=outcome.artifacts,
            error=AdapterError(
                ErrorCode.CONTRACT_VALIDATION_FAILED,
                "source result exceeds terminal replay entry limit",
            ),
            occurred_at=occurred_at,
        )
        if not self._replay_store.entry_fits(fingerprint, candidate):
            raise original_error
        return await self._replay_store.put_if_absent(
            command.message_id,
            fingerprint,
            candidate,
        )

    @staticmethod
    def _artifacts(context: RuntimeAdapterContext | None) -> tuple[ArtifactReference, ...]:
        return () if context is None else tuple(context.saved_artifacts)

    @staticmethod
    def _require_same_delivery(expected: str, actual: str) -> None:
        if expected != actual:
            raise InvalidCommandError("message_id was reused with different command bytes")

    def _validate_replay(
        self,
        command: SourceCollectRequested,
        prepared: PreparedTerminalMessage,
    ) -> None:
        document = self._schema_registry.validate_message_json(prepared.body)
        message = parse_source_message(document)
        if not isinstance(message, SourceCollectCompleted | SourceCollectFailed):
            raise ValueError("terminal replay entry does not contain a terminal result")
        if (
            message.message_id != prepared.message_id
            or message.type != prepared.message_type
            or message.correlation_id != prepared.correlation_id
            or message.correlation_id != command.correlation_id
            or message.causation_id != command.message_id
            or message.payload.job_id != command.payload.job_id
            or message.payload.attempt_no != command.payload.attempt_no
        ):
            raise ValueError("terminal replay entry does not match its source command")
