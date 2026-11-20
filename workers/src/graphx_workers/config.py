"""Environment-backed worker configuration."""

import re
from datetime import timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import Field, PositiveInt, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    s3_endpoint: str | None = None
    s3_bucket_artifacts: str | None = None

    @field_validator(
        "worker_heartbeat_interval",
        "worker_job_timeout",
        "worker_shutdown_timeout",
        "rabbitmq_connect_timeout",
        "rabbitmq_publish_timeout",
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
        return self

    def require_rabbitmq_url(self) -> str:
        if self.rabbitmq_url is None or not self.rabbitmq_url.strip():
            raise ValueError("RABBITMQ_URL is required for the source worker runtime")
        return self.rabbitmq_url

    def resolve_contract_schema_root(self) -> Path:
        if self.contract_schema_root is not None:
            return self.contract_schema_root
        return Path(__file__).resolve().parents[3] / "contracts" / "schemas"
