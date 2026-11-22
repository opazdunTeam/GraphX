"""Environment-backed worker configuration."""

import re
from datetime import timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import Field, PositiveInt, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_MAX_SOURCE_MANIFEST_BYTES = 16 * 1024 * 1024
_MAX_SOURCE_RESULT_SCALAR_CHARS = 1024 * 1024
_MAX_SOURCE_RESULT_JSON_NODES = 100_000
_SOURCE_SERIALIZATION_MEMORY_BUDGET_BYTES = 64 * 1024 * 1024
_JSON_TOKEN_ALLOCATION_FACTOR = 12
_JSON_NODE_ALLOCATION_ESTIMATE_BYTES = 256
_JSON_SCALAR_STORAGE_FACTOR = 4
_JCS_ALLOCATION_FACTOR = 2
DEFAULT_SOURCE_RESULT_MAX_TOTAL_SCALAR_CHARS = 1024 * 1024


class WorkerKind(StrEnum):
    SOURCE = "source"
    DOCUMENT = "document"
    BROWSER = "browser"


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: str = "local"
    log_level: str = "info"
    log_format: Literal["json", "text"] = "json"
    worker_kind: WorkerKind = WorkerKind.SOURCE
    worker_concurrency: PositiveInt = 4
    worker_heartbeat_interval: timedelta = Field(default=timedelta(seconds=15))
    worker_job_timeout: timedelta = Field(default=timedelta(minutes=5))
    worker_shutdown_timeout: timedelta = Field(default=timedelta(seconds=30))
    playwright_enabled: bool = False

    rabbitmq_url: str | None = None
    rabbitmq_prefetch_source: PositiveInt = 4
    rabbitmq_connect_timeout: timedelta = Field(default=timedelta(seconds=10))
    rabbitmq_publish_timeout: timedelta = Field(default=timedelta(seconds=15))
    contract_schema_root: Path | None = None
    terminal_replay_root: Path = Path(".data/worker-replay")
    terminal_replay_retention: timedelta = Field(default=timedelta(days=30))
    terminal_replay_cleanup_interval: timedelta = Field(default=timedelta(minutes=10))
    terminal_replay_max_entries: PositiveInt = 100_000
    terminal_replay_max_bytes: PositiveInt = 10 * 1024 * 1024 * 1024
    terminal_replay_max_entry_bytes: PositiveInt = 2 * 1024 * 1024
    source_result_inline_max_bytes: PositiveInt = 512 * 1024
    source_result_terminal_max_bytes: PositiveInt = 1024 * 1024
    source_result_manifest_max_bytes: PositiveInt = 16 * 1024 * 1024
    source_result_max_scalar_chars: PositiveInt = 256 * 1024
    source_result_max_total_scalar_chars: PositiveInt = DEFAULT_SOURCE_RESULT_MAX_TOTAL_SCALAR_CHARS
    source_result_max_json_nodes: PositiveInt = 20_000
    s3_endpoint: str | None = None
    s3_region: str = "local"
    s3_bucket_artifacts: str | None = None
    s3_access_key: SecretStr | None = None
    s3_secret_key: SecretStr | None = None
    s3_use_path_style: bool = True
    s3_allow_overwrite: bool = False
    s3_require_versioning: bool = False
    s3_connect_timeout: timedelta = Field(default=timedelta(seconds=10))
    s3_read_timeout: timedelta = Field(default=timedelta(seconds=30))
    s3_reconciliation_timeout: timedelta = Field(default=timedelta(seconds=3))
    s3_max_inflight_operations: PositiveInt = 16
    artifact_max_bytes: PositiveInt = 100 * 1024 * 1024
    artifact_spool_max_memory_bytes: PositiveInt = 1024 * 1024
    artifact_spool_directory: Path | None = None
    artifact_allowed_media_types: str = (
        "application/json,application/pdf,application/xml,application/zip,"
        "text/csv,text/html,text/plain,text/xml,image/jpeg,image/png,image/webp"
    )

    @field_validator(
        "worker_heartbeat_interval",
        "worker_job_timeout",
        "worker_shutdown_timeout",
        "rabbitmq_connect_timeout",
        "rabbitmq_publish_timeout",
        "s3_connect_timeout",
        "s3_read_timeout",
        "s3_reconciliation_timeout",
        "terminal_replay_retention",
        "terminal_replay_cleanup_interval",
        mode="before",
    )
    @classmethod
    def parse_duration(cls, value: Any) -> Any:
        if not isinstance(value, str):
            return value

        match = re.fullmatch(r"(?P<amount>[1-9][0-9]*)(?P<unit>ms|s|m|h)", value.strip())
        if match is None:
            raise ValueError("duration must use a positive integer followed by ms, s, m, or h")

        amount = int(match.group("amount"))
        unit = match.group("unit")
        factors = {
            "ms": timedelta(milliseconds=amount),
            "s": timedelta(seconds=amount),
            "m": timedelta(minutes=amount),
            "h": timedelta(hours=amount),
        }
        return factors[unit]

    def safe_summary(self) -> dict[str, str | int | bool]:
        return {
            "environment": self.app_env,
            "worker_kind": self.worker_kind.value,
            "concurrency": self.worker_concurrency,
            "prefetch": self.rabbitmq_prefetch_source,
            "playwright_enabled": self.playwright_enabled,
            "rabbitmq_configured": self.rabbitmq_url is not None,
            "object_store_configured": all((self.s3_endpoint, self.s3_bucket_artifacts)),
            "object_store_requires_versioning": self.s3_require_versioning,
            "inline_result_max_bytes": self.source_result_inline_max_bytes,
            "terminal_result_max_bytes": self.source_result_terminal_max_bytes,
            "manifest_max_bytes": self.source_result_manifest_max_bytes,
            "result_max_scalar_chars": self.source_result_max_scalar_chars,
            "result_max_total_scalar_chars": self.source_result_max_total_scalar_chars,
            "result_max_json_nodes": self.source_result_max_json_nodes,
        }

    @model_validator(mode="after")
    def validate_replay_policy(self) -> Self:
        if self.terminal_replay_retention < timedelta(hours=24):
            raise ValueError("TERMINAL_REPLAY_RETENTION must be at least 24 hours")
        if self.terminal_replay_max_entry_bytes > self.terminal_replay_max_bytes:
            raise ValueError(
                "TERMINAL_REPLAY_MAX_ENTRY_BYTES cannot exceed TERMINAL_REPLAY_MAX_BYTES"
            )
        if self.terminal_replay_max_entry_bytes < 4096:
            raise ValueError("TERMINAL_REPLAY_MAX_ENTRY_BYTES must be at least 4096 bytes")
        if self.s3_allow_overwrite:
            raise ValueError("S3_ALLOW_OVERWRITE must remain false for immutable artifacts")
        configured = (
            self.s3_endpoint,
            self.s3_bucket_artifacts,
            self.s3_access_key,
            self.s3_secret_key,
        )
        if any(value is not None for value in configured) and not all(
            value is not None for value in configured
        ):
            raise ValueError(
                "S3_ENDPOINT, S3_BUCKET_ARTIFACTS, S3_ACCESS_KEY and S3_SECRET_KEY "
                "must be configured together"
            )
        if all(value is not None for value in configured):
            endpoint, bucket, access_key, secret_key = configured
            assert isinstance(endpoint, str)
            assert isinstance(bucket, str)
            assert isinstance(access_key, SecretStr)
            assert isinstance(secret_key, SecretStr)
            if not endpoint.strip() or not bucket.strip() or not self.s3_region.strip():
                raise ValueError("S3 endpoint, bucket and region must be non-empty")
            if not access_key.get_secret_value() or not secret_key.get_secret_value():
                raise ValueError("S3 credentials must be non-empty")
            if self.s3_max_inflight_operations < self.worker_concurrency:
                raise ValueError(
                    "S3_MAX_INFLIGHT_OPERATIONS cannot be lower than WORKER_CONCURRENCY"
                )
        if self.artifact_spool_max_memory_bytes > self.artifact_max_bytes:
            raise ValueError("ARTIFACT_SPOOL_MAX_MEMORY_BYTES cannot exceed ARTIFACT_MAX_BYTES")
        if self.artifact_max_bytes > 5 * 1024 * 1024 * 1024:
            raise ValueError("ARTIFACT_MAX_BYTES cannot exceed the S3 PutObject limit")
        allowed_media_types = self.allowed_artifact_media_types()
        if not allowed_media_types:
            raise ValueError("ARTIFACT_ALLOWED_MEDIA_TYPES must not be empty")
        if self.worker_kind is WorkerKind.SOURCE:
            self._validate_source_result_policy(allowed_media_types)
        return self

    def _validate_source_result_policy(self, allowed_media_types: frozenset[str]) -> None:
        if self.source_result_inline_max_bytes >= self.terminal_replay_max_entry_bytes:
            raise ValueError(
                "SOURCE_RESULT_INLINE_MAX_BYTES must be lower than TERMINAL_REPLAY_MAX_ENTRY_BYTES"
            )
        if self.source_result_terminal_max_bytes < self.source_result_inline_max_bytes:
            raise ValueError(
                "SOURCE_RESULT_TERMINAL_MAX_BYTES cannot be lower than "
                "SOURCE_RESULT_INLINE_MAX_BYTES"
            )
        if self.source_result_terminal_max_bytes >= self.terminal_replay_max_entry_bytes:
            raise ValueError(
                "SOURCE_RESULT_TERMINAL_MAX_BYTES must be lower than "
                "TERMINAL_REPLAY_MAX_ENTRY_BYTES"
            )
        if self.source_result_manifest_max_bytes < max(4096, self.source_result_inline_max_bytes):
            raise ValueError(
                "SOURCE_RESULT_MANIFEST_MAX_BYTES must be at least 4096 bytes and not "
                "lower than SOURCE_RESULT_INLINE_MAX_BYTES"
            )
        if self.source_result_manifest_max_bytes > self.artifact_max_bytes:
            raise ValueError("SOURCE_RESULT_MANIFEST_MAX_BYTES cannot exceed ARTIFACT_MAX_BYTES")
        if self.source_result_manifest_max_bytes > _MAX_SOURCE_MANIFEST_BYTES:
            raise ValueError("SOURCE_RESULT_MANIFEST_MAX_BYTES cannot exceed 16777216 bytes")
        if self.source_result_max_scalar_chars > _MAX_SOURCE_RESULT_SCALAR_CHARS:
            raise ValueError("SOURCE_RESULT_MAX_SCALAR_CHARS cannot exceed 1048576 characters")
        if self.source_result_max_total_scalar_chars > _MAX_SOURCE_MANIFEST_BYTES:
            raise ValueError(
                "SOURCE_RESULT_MAX_TOTAL_SCALAR_CHARS cannot exceed 16777216 characters"
            )
        if self.source_result_max_json_nodes > _MAX_SOURCE_RESULT_JSON_NODES:
            raise ValueError("SOURCE_RESULT_MAX_JSON_NODES cannot exceed 100000")
        per_worker_serialization_budget = (
            _JSON_TOKEN_ALLOCATION_FACTOR * self.source_result_max_scalar_chars
            + _JSON_SCALAR_STORAGE_FACTOR * self.source_result_max_total_scalar_chars
            + _JCS_ALLOCATION_FACTOR * self.source_result_terminal_max_bytes
            + min(
                self.artifact_spool_max_memory_bytes,
                self.source_result_manifest_max_bytes,
            )
            + _JSON_NODE_ALLOCATION_ESTIMATE_BYTES * self.source_result_max_json_nodes
        )
        if (
            per_worker_serialization_budget * self.worker_concurrency
            > _SOURCE_SERIALIZATION_MEMORY_BUDGET_BYTES
        ):
            raise ValueError(
                "source result serialization limits and WORKER_CONCURRENCY exceed "
                "the 64 MiB process budget"
            )
        if "application/json" not in allowed_media_types:
            raise ValueError(
                "ARTIFACT_ALLOWED_MEDIA_TYPES must include application/json for source workers"
            )

    def allowed_artifact_media_types(self) -> frozenset[str]:
        return frozenset(
            value.strip().lower()
            for value in self.artifact_allowed_media_types.split(",")
            if value.strip()
        )

    def require_object_store(self) -> tuple[str, str, str, str]:
        if (
            self.s3_endpoint is None
            or self.s3_bucket_artifacts is None
            or self.s3_access_key is None
            or self.s3_secret_key is None
        ):
            raise ValueError("complete S3 configuration is required for artifact storage")
        return (
            self.s3_endpoint,
            self.s3_bucket_artifacts,
            self.s3_access_key.get_secret_value(),
            self.s3_secret_key.get_secret_value(),
        )

    def require_rabbitmq_url(self) -> str:
        if self.rabbitmq_url is None or not self.rabbitmq_url.strip():
            raise ValueError("RABBITMQ_URL is required for the source worker runtime")
        return self.rabbitmq_url

    def resolve_contract_schema_root(self) -> Path:
        if self.contract_schema_root is not None:
            return self.contract_schema_root
        return Path(__file__).resolve().parents[3] / "contracts" / "schemas"
