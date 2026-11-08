"""References to immutable objects stored by the runtime."""

from typing import Literal

from pydantic import AnyUrl, AwareDatetime, BaseModel, ConfigDict, Field


class ArtifactReference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    local_ref: str = Field(min_length=1)
    kind: Literal["raw_response", "document", "manifest", "screenshot", "other"]
    object_key: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    media_type: str = Field(min_length=1)
    byte_size: int = Field(ge=0)
    source_uri: AnyUrl
    retrieved_at: AwareDatetime
    published_at: AwareDatetime | None = None
    http_metadata: dict[str, str] = Field(default_factory=dict)
