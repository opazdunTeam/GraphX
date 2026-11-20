"""References to immutable objects stored by the runtime."""

import base64
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import AnyUrl, AwareDatetime, BaseModel, ConfigDict, Field, field_validator

SAFE_HTTP_METADATA = frozenset({"etag", "last-modified", "content-language"})


def artifact_object_key(job_id: UUID | str, attempt_no: int, local_ref: str) -> str:
    encoded = base64.urlsafe_b64encode(local_ref.encode()).decode().rstrip("=")
    return f"artifacts/v1/{job_id}/{attempt_no}/{encoded}"


class ArtifactReference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    local_ref: str = Field(min_length=1, max_length=200, pattern=r"^[^\x00-\x1f\x7f-\x9f]+$")
    kind: Literal["raw_response", "document", "manifest", "screenshot", "other"]
    object_key: str = Field(
        min_length=1,
        max_length=1024,
        pattern=r"^artifacts/v1/[0-9a-f-]{36}/[1-9][0-9]*/[A-Za-z0-9_-]+$",
    )
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    media_type: str = Field(min_length=1, max_length=200)
    byte_size: int = Field(ge=0)
    source_uri: AnyUrl
    retrieved_at: AwareDatetime
    published_at: AwareDatetime | None = None
    http_metadata: dict[str, str] = Field(default_factory=dict)
    original_filename: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        pattern=r"^[^/\\\x00-\x1f\x7f-\x9f]+$",
    )
    parent_local_ref: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
        pattern=r"^[^\x00-\x1f\x7f-\x9f]+$",
    )

    @field_validator("source_uri", mode="before")
    @classmethod
    def validate_raw_source_uri_port(cls, value: object) -> object:
        parsed = urlsplit(str(value))
        try:
            port = parsed.port
        except ValueError as error:
            raise ValueError("source_uri port must be between 1 and 65535") from error
        if port is not None:
            authority = parsed.netloc.rsplit("@", 1)[-1]
            suffix = authority.rsplit("]", 1)[-1] if authority.startswith("[") else authority
            port_text = (
                suffix.removeprefix(":") if authority.startswith("[") else suffix.rpartition(":")[2]
            )
            if port_text != str(port) or not 1 <= port <= 65535:
                raise ValueError("source_uri port must be canonical and between 1 and 65535")
        return value

    @field_validator("source_uri")
    @classmethod
    def validate_source_uri(cls, value: AnyUrl) -> AnyUrl:
        parsed = urlsplit(str(value))
        if len(str(value)) > 2048 or parsed.scheme not in {"http", "https"}:
            raise ValueError("source_uri must use http or https and fit the transport limit")
        if not parsed.hostname or parsed.username is not None or parsed.password is not None:
            raise ValueError("source_uri must have a host and must not contain userinfo")
        if parsed.query or parsed.fragment:
            raise ValueError("source_uri must not contain query parameters or a fragment")
        port = parsed.port
        if port is not None:
            authority = parsed.netloc.rsplit("@", 1)[-1]
            port_text = (
                authority.rsplit("]", 1)[-1].removeprefix(":")
                if authority.startswith("[")
                else authority.rpartition(":")[2]
            )
            if not 1 <= port <= 65535 or port_text != str(port):
                raise ValueError("source_uri port must be canonical and between 1 and 65535")
        return value

    @field_validator("http_metadata")
    @classmethod
    def validate_http_metadata(cls, values: dict[str, str]) -> dict[str, str]:
        for name, value in values.items():
            if name not in SAFE_HTTP_METADATA:
                raise ValueError("http_metadata contains a non-allowlisted name")
            if not value or len(value) > 512 or "\r" in value or "\n" in value:
                raise ValueError("http_metadata contains an unsafe value")
        return values

    @field_validator("original_filename")
    @classmethod
    def validate_original_filename(cls, value: str | None) -> str | None:
        if value is not None and value != value.strip(" ."):
            raise ValueError("original_filename must not have boundary spaces or dots")
        return value
