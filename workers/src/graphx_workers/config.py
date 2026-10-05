"""Environment-backed worker configuration."""

import re
from datetime import timedelta
from enum import StrEnum
from typing import Any

from pydantic import Field, PositiveInt, field_validator
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
    log_format: str = "json"
    worker_kind: WorkerKind = WorkerKind.SOURCE
    worker_concurrency: PositiveInt = 4
    worker_heartbeat_interval: timedelta = Field(default=timedelta(seconds=15))
    worker_job_timeout: timedelta = Field(default=timedelta(minutes=5))
    playwright_enabled: bool = False

    rabbitmq_url: str | None = None
    s3_endpoint: str | None = None
    s3_bucket_artifacts: str | None = None

    @field_validator("worker_heartbeat_interval", "worker_job_timeout", mode="before")
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
            "playwright_enabled": self.playwright_enabled,
            "rabbitmq_configured": self.rabbitmq_url is not None,
            "object_store_configured": all((self.s3_endpoint, self.s3_bucket_artifacts)),
        }
