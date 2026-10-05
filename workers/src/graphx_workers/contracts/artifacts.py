"""References to immutable objects stored by the runtime."""

from pydantic import BaseModel, ConfigDict, Field


class ArtifactReference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    object_key: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    media_type: str = Field(min_length=1)
    byte_size: int = Field(ge=0)
