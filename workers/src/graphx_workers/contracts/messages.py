"""Transport envelope shared by worker commands and events."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class MessageEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message_id: UUID
    message_type: str = Field(min_length=1)
    schema_version: int = Field(ge=1)
    occurred_at: datetime
    correlation_id: str = Field(min_length=1)
    causation_id: str | None = None
    producer: str = Field(min_length=1)
    root_command_id: UUID
    payload: dict[str, Any]
