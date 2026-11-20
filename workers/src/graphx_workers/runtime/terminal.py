"""Build and validate terminal source-result messages."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID, uuid5

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.messages import (
    SourceCollectCompleted,
    SourceCollectError,
    SourceCollectFailed,
    SourceCollectFailedPayload,
    SourceCollectRequested,
)
from graphx_workers.contracts.results import SourceResult
from graphx_workers.contracts.schema import ContractSchemaRegistry
from graphx_workers.sdk.errors import AdapterError

RESULT_MESSAGE_NAMESPACE = UUID("64ba3f47-e7d5-4e19-946a-2bcd454fe63a")
COMPLETED_ROUTING_KEY: Literal["source.collect.completed.v1"] = "source.collect.completed.v1"
FAILED_ROUTING_KEY: Literal["source.collect.failed.v1"] = "source.collect.failed.v1"


@dataclass(frozen=True, slots=True)
class PreparedTerminalMessage:
    message_id: UUID
    message_type: str
    correlation_id: UUID
    body: bytes


class TerminalMessageFactory:
    def __init__(
        self,
        registry: ContractSchemaRegistry,
        *,
        producer: str = "graphx-source-worker",
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._registry = registry
        self._producer = producer
        self._clock = clock or (lambda: datetime.now(UTC))

    def completed(
        self,
        command: SourceCollectRequested,
        result: SourceResult,
    ) -> PreparedTerminalMessage:
        envelope = SourceCollectCompleted(
            message_id=self._result_message_id(command.message_id),
            type=COMPLETED_ROUTING_KEY,
            schema_version=1,
            occurred_at=self._clock(),
            correlation_id=command.correlation_id,
            causation_id=command.message_id,
            producer=self._producer,
            root_command_id=command.root_command_id,
            payload=result,
        )
        return self._prepare(envelope)

    def failed(
        self,
        command: SourceCollectRequested,
        *,
        adapter_version: str,
        artifacts: Sequence[ArtifactReference],
        error: AdapterError,
        occurred_at: datetime | None = None,
    ) -> PreparedTerminalMessage:
        envelope = SourceCollectFailed(
            message_id=self._result_message_id(command.message_id),
            type=FAILED_ROUTING_KEY,
            schema_version=1,
            occurred_at=occurred_at or self._clock(),
            correlation_id=command.correlation_id,
            causation_id=command.message_id,
            producer=self._producer,
            root_command_id=command.root_command_id,
            payload=SourceCollectFailedPayload(
                job_id=command.payload.job_id,
                attempt_no=command.payload.attempt_no,
                adapter_version=adapter_version,
                artifacts=list(artifacts),
                error=SourceCollectError(
                    code=error.code,
                    classification=error.classification,
                    message=error.safe_detail,
                    retry_after_seconds=error.retry_after_seconds,
                ),
            ),
        )
        return self._prepare(envelope)

    def _prepare(
        self,
        envelope: SourceCollectCompleted | SourceCollectFailed,
    ) -> PreparedTerminalMessage:
        body = envelope.model_dump_json().encode("utf-8")
        self._registry.validate_message_json(body)
        return PreparedTerminalMessage(
            message_id=envelope.message_id,
            message_type=envelope.type,
            correlation_id=envelope.correlation_id,
            body=body,
        )

    @staticmethod
    def _result_message_id(command_message_id: UUID) -> UUID:
        return uuid5(RESULT_MESSAGE_NAMESPACE, str(command_message_id))
