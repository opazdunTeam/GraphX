from datetime import timedelta

from pytest import MonkeyPatch

from graphx_workers.config import WorkerKind, WorkerSettings


def test_settings_read_worker_environment(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("WORKER_KIND", "document")
    monkeypatch.setenv("WORKER_CONCURRENCY", "2")
    monkeypatch.setenv("WORKER_HEARTBEAT_INTERVAL", "10s")

    settings = WorkerSettings()

    assert settings.worker_kind is WorkerKind.DOCUMENT
    assert settings.worker_concurrency == 2
    assert settings.worker_heartbeat_interval == timedelta(seconds=10)


def test_safe_summary_does_not_contain_rabbitmq_url() -> None:
    settings = WorkerSettings(rabbitmq_url="amqp://user:secret@example.test/vhost")
    summary = settings.safe_summary()

    assert "rabbitmq_url" not in summary
    assert summary["rabbitmq_configured"] is True
