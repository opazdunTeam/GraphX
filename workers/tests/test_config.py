from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError
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
    assert summary["prefetch"] == 4


def test_source_runtime_settings_are_parsed(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("RABBITMQ_PREFETCH_SOURCE", "7")
    monkeypatch.setenv("RABBITMQ_CONNECT_TIMEOUT", "3s")
    monkeypatch.setenv("RABBITMQ_PUBLISH_TIMEOUT", "4s")
    monkeypatch.setenv("WORKER_SHUTDOWN_TIMEOUT", "5s")
    monkeypatch.setenv("CONTRACT_SCHEMA_ROOT", "C:/graphx/contracts/schemas")
    monkeypatch.setenv("TERMINAL_REPLAY_ROOT", "C:/graphx/replay")
    monkeypatch.setenv("TERMINAL_REPLAY_RETENTION", "48h")
    monkeypatch.setenv("TERMINAL_REPLAY_CLEANUP_INTERVAL", "5m")
    monkeypatch.setenv("TERMINAL_REPLAY_MAX_ENTRIES", "123")
    monkeypatch.setenv("TERMINAL_REPLAY_MAX_BYTES", "456789")
    monkeypatch.setenv("TERMINAL_REPLAY_MAX_ENTRY_BYTES", "12345")
    monkeypatch.setenv("S3_MAX_INFLIGHT_OPERATIONS", "9")

    settings = WorkerSettings()

    assert settings.rabbitmq_prefetch_source == 7
    assert settings.rabbitmq_connect_timeout == timedelta(seconds=3)
    assert settings.rabbitmq_publish_timeout == timedelta(seconds=4)
    assert settings.worker_shutdown_timeout == timedelta(seconds=5)
    assert settings.resolve_contract_schema_root() == Path("C:/graphx/contracts/schemas")
    assert settings.terminal_replay_root == Path("C:/graphx/replay")
    assert settings.terminal_replay_retention == timedelta(hours=48)
    assert settings.terminal_replay_cleanup_interval == timedelta(minutes=5)
    assert settings.terminal_replay_max_entries == 123
    assert settings.terminal_replay_max_bytes == 456789
    assert settings.terminal_replay_max_entry_bytes == 12345
    assert settings.s3_max_inflight_operations == 9


def test_source_runtime_requires_non_empty_rabbitmq_url() -> None:
    with pytest.raises(ValueError, match="RABBITMQ_URL"):
        WorkerSettings(rabbitmq_url=" ").require_rabbitmq_url()


def test_settings_reject_unsupported_log_format() -> None:
    with pytest.raises(ValidationError, match="log_format"):
        WorkerSettings.model_validate({"log_format": "xml"})


def test_settings_reject_unsafe_replay_policy() -> None:
    with pytest.raises(ValidationError, match="at least 24 hours"):
        WorkerSettings(terminal_replay_retention=timedelta(hours=23))

    with pytest.raises(ValidationError, match="cannot exceed"):
        WorkerSettings(terminal_replay_max_bytes=10, terminal_replay_max_entry_bytes=11)

    with pytest.raises(ValidationError, match="at least 4096"):
        WorkerSettings(terminal_replay_max_entry_bytes=4095)


def test_object_store_capacity_covers_worker_concurrency() -> None:
    complete_store = {
        "s3_endpoint": "http://minio.test:9000",
        "s3_bucket_artifacts": "artifacts",
        "s3_access_key": "access",
        "s3_secret_key": "secret",
    }

    with pytest.raises(ValidationError, match="cannot be lower than WORKER_CONCURRENCY"):
        WorkerSettings.model_validate(
            complete_store
            | {
                "worker_concurrency": 5,
                "s3_max_inflight_operations": 4,
            }
        )

    settings = WorkerSettings.model_validate(
        complete_store
        | {
            "worker_concurrency": 4,
            "s3_max_inflight_operations": 4,
        }
    )
    assert settings.s3_max_inflight_operations == settings.worker_concurrency


def test_object_storage_configuration_is_complete_and_never_allows_overwrite() -> None:
    with pytest.raises(ValidationError, match="configured together"):
        WorkerSettings(s3_endpoint="http://minio:9000")
    with pytest.raises(ValidationError, match="must remain false"):
        WorkerSettings(s3_allow_overwrite=True)

    settings = WorkerSettings(
        s3_endpoint="http://minio:9000",
        s3_bucket_artifacts="artifacts",
        s3_access_key=SecretStr("access"),
        s3_secret_key=SecretStr("secret"),
        s3_require_versioning=True,
    )
    assert settings.require_object_store() == (
        "http://minio:9000",
        "artifacts",
        "access",
        "secret",
    )
    assert settings.safe_summary()["object_store_requires_versioning"] is True
    assert "secret" not in str(settings.safe_summary())
