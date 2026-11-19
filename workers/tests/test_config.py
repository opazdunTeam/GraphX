from datetime import timedelta

import pytest
from pydantic import ValidationError
from pytest import MonkeyPatch

from graphx_workers.config import WorkerKind, WorkerSettings


def test_settings_read_worker_environment(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("WORKER_KIND", "document")
    monkeypatch.setenv("WORKER_CONCURRENCY", "2")
    monkeypatch.setenv("WORKER_HEARTBEAT_INTERVAL", "10s")
    monkeypatch.setenv("LOG_FORMAT", "text")

    settings = WorkerSettings()

    assert settings.worker_kind is WorkerKind.DOCUMENT
    assert settings.worker_concurrency == 2
    assert settings.worker_heartbeat_interval == timedelta(seconds=10)
    assert settings.log_format == "text"


def test_safe_summary_does_not_contain_rabbitmq_url() -> None:
    settings = WorkerSettings(rabbitmq_url="amqp://user:secret@example.test/vhost")
    summary = settings.safe_summary()

    assert "rabbitmq_url" not in summary
    assert summary["rabbitmq_configured"] is True


def test_settings_reject_unsupported_log_format() -> None:
    with pytest.raises(ValidationError, match="log_format"):
        WorkerSettings.model_validate({"log_format": "xml"})
