"""References to immutable objects stored by the runtime."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ArtifactReference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["raw_response", "document", "manifest", "screenshot", "other"]
    object_key: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    media_type: str = Field(min_length=1)
    byte_size: int = Field(ge=0)
    source_uri: str = Field(min_length=1)
    retrieved_at: datetime
    published_at: datetime | None = None
    http_metadata: dict[str, str] = Field(default_factory=dict)
