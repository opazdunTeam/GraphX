import json
import logging
import sys
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Literal
from uuid import UUID

import pytest

from graphx_workers.runtime.logging import JsonFormatter, TextFormatter, configure_logging


class ContextLogRecord(logging.LogRecord):
    context: dict[str, object]


class RuntimeState(StrEnum):
    READY = "ready"


def test_json_formatter_preserves_structured_context() -> None:
    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="configuration valid",
        args=(),
        exc_info=None,
    )
    record.context = {"worker_kind": "source", "concurrency": 4}

    event = json.loads(JsonFormatter().format(record))

    assert event["service"] == "graphx-workers"
    assert event["message"] == "configuration valid"
    assert event["worker_kind"] == "source"
    assert event["concurrency"] == 4


def test_text_formatter_preserves_structured_context() -> None:
    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="configuration valid",
        args=(),
        exc_info=None,
    )
    record.context = {"worker_kind": "document", "configured": True}

    output = TextFormatter().format(record)

    assert 'service="graphx-workers"' in output
    assert 'message="configuration valid"' in output
    assert 'worker_kind="document"' in output
    assert "configured=true" in output


@pytest.mark.parametrize(
    ("output_format", "formatter_type"),
    [("json", JsonFormatter), ("text", TextFormatter)],
)
def test_configure_logging_selects_requested_formatter(
    output_format: Literal["json", "text"],
    formatter_type: type[logging.Formatter],
) -> None:
    logger = configure_logging("info", output_format)

    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0].formatter, formatter_type)
    assert isinstance(logger.handlers[0], logging.StreamHandler)
    assert logger.handlers[0].stream is sys.stdout


def test_structured_context_cannot_replace_base_event_fields() -> None:
    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg="actual message",
        args=(),
        exc_info=None,
    )
    record.context = {
        "timestamp": "replacement",
        "level": "debug",
        "service": "replacement",
        "message": "replacement",
        "exception": "forged",
    }

    event = json.loads(JsonFormatter().format(record))

    assert event["level"] == "warning"
    assert event["message"] == "actual message"
    assert event["service"] == "graphx-workers"
    assert "exception" not in event


@pytest.mark.parametrize("formatter", [JsonFormatter(), TextFormatter()])
def test_formatter_converts_common_runtime_values(formatter: logging.Formatter) -> None:
    identifier = UUID("12345678-1234-5678-1234-567812345678")
    occurred_at = datetime(2026, 11, 17, 19, 0, tzinfo=UTC)
    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="result received",
        args=(),
        exc_info=None,
    )
    record.context = {
        "message_id": identifier,
        "occurred_at": occurred_at,
        "manifest_path": Path("artifacts/manifest.json"),
        "score": Decimal("0.75"),
        "state": RuntimeState.READY,
    }

    output = formatter.format(record)

    assert str(identifier) in output
    assert occurred_at.isoformat() in output
    assert "artifacts" in output
    assert "0.75" in output
    assert "ready" in output


@pytest.mark.parametrize("formatter", [JsonFormatter(), TextFormatter()])
def test_formatter_redacts_unknown_binary_and_recursive_values(
    formatter: logging.Formatter,
) -> None:
    class SensitiveValue:
        def __str__(self) -> str:
            return "secret-from-string-conversion"

    recursive: list[object] = []
    recursive.append(recursive)
    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="safe message",
        args=(),
        exc_info=None,
    )
    record.context = {
        "source": SensitiveValue(),
        "manifest_path": b"secret-binary-content",
        "state": recursive,
        "score": float("nan"),
        "configured": {1: "not-logged"},
    }

    output = formatter.format(record)

    assert "secret-from-string-conversion" not in output
    assert "secret-binary-content" not in output
    assert "<SensitiveValue>" in output
    assert "<bytes>" in output
    assert "<list:redacted>" in output
    assert "<non-finite-float>" in output
    assert "<dict:redacted>" in output
    assert "not-logged" not in output


@pytest.mark.parametrize("formatter", [JsonFormatter(), TextFormatter()])
def test_formatter_redacts_nested_secret_and_payload_fields(
    formatter: logging.Formatter,
) -> None:
    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="request completed",
        args=(),
        exc_info=None,
    )
    record.context = {
        "apiKey": "secret-token",
        "state": {
            "authorization": "Bearer secret-authorization",
            "response_headers": {"x-sensitive": "secret-header"},
        },
        "payload": {"document": "very-sensitive-body"},
        "status_code": 200,
    }

    output = formatter.format(record)

    assert "secret-token" not in output
    assert "secret-authorization" not in output
    assert "secret-header" not in output
    assert "very-sensitive-body" not in output
    assert output.count("<redacted>") == 2
    assert "<dict:redacted>" in output
    assert "200" in output


@pytest.mark.parametrize("formatter", [JsonFormatter(), TextFormatter()])
def test_formatter_redacts_compact_secret_names_and_urls(
    formatter: logging.Formatter,
) -> None:
    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="request completed",
        args=(),
        exc_info=None,
    )
    record.context = {
        "apikey": "secret-a",
        "x-apikey": "secret-b",
        "accesskey": "secret-c",
        "clientsecret": "secret-d",
        "sessionid": "secret-e",
        "connectionstring": "secret-f",
        "request_url": "https://user:pass@example.test/?token=secret-g#fragment",
    }

    output = formatter.format(record)

    for secret in (
        "secret-a",
        "secret-b",
        "secret-c",
        "secret-d",
        "secret-e",
        "secret-f",
        "secret-g",
    ):
        assert secret not in output
    assert "user:pass" not in output
    assert output.count("<redacted>") == 7


@pytest.mark.parametrize("formatter", [JsonFormatter(), TextFormatter()])
def test_formatter_filters_non_operational_context_fields(
    formatter: logging.Formatter,
) -> None:
    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="operation completed",
        args=(),
        exc_info=None,
    )
    record.context = {"arbitrary_data": "must-not-be-logged", "status": "succeeded"}

    output = formatter.format(record)

    assert "must-not-be-logged" not in output
    assert "arbitrary_data" not in output
    assert "succeeded" in output


@pytest.mark.parametrize("formatter", [JsonFormatter(), TextFormatter()])
def test_formatter_handles_pathological_scalars(formatter: logging.Formatter) -> None:
    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="invalid surrogate: \ud800",
        args=(),
        exc_info=None,
    )
    record.context = {
        "concurrency": 10**10_000,
        "source": "source-\ud800-name",
        "manifest_path": Path("x" * 1_000),
        "score": Decimal("9" * 1_000),
        "unknown-\ud800-key": "must-not-be-logged",
    }

    output = formatter.format(record)

    output.encode("utf-8")
    assert "<int:out-of-range>" in output
    assert "\ud800" not in output
    assert "\ufffd" in output
    assert "<truncated>" in output


@pytest.mark.parametrize("formatter", [JsonFormatter(), TextFormatter()])
def test_formatter_survives_unformattable_message(formatter: logging.Formatter) -> None:
    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="invalid integer: %s",
        args=(10**10_000,),
        exc_info=None,
    )
    record.context = {"status": "failed"}

    output = formatter.format(record)

    assert "<unformattable-message>" in output
    assert "failed" in output


@pytest.mark.parametrize("formatter", [JsonFormatter(), TextFormatter()])
def test_formatter_bounds_large_strings_collections_and_events(
    formatter: logging.Formatter,
) -> None:
    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="m" * 2_000,
        args=(),
        exc_info=None,
    )
    large_operational_fields = (
        "adapter_version",
        "artifact_count",
        "claim_count",
        "completed",
        "configured",
        "correlation_id",
        "deadline",
        "duration_ms",
        "environment",
        "error_code",
        "heartbeat_seconds",
        "investigation_id",
        "job_id",
        "manifest_path",
        "mapping_version",
        "message_id",
        "normalization_version",
        "occurred_at",
        "operation",
        "outcome",
        "page_number",
        "parser_version",
        "purpose",
        "record_count",
        "request_id",
        "retry_delay_seconds",
        "route",
        "score",
        "source",
        "state",
        "status",
        "status_code",
        "total",
        "trace_id",
        "worker_kind",
    )
    record.context = {key: "x" * 1_000 for key in large_operational_fields}

    output = formatter.format(record)

    assert "<truncated>" in output
    assert "context_truncated" in output
    assert len(output.encode("utf-8")) < 16 * 1_024


def test_text_formatter_filters_unsafe_non_operational_context_key() -> None:
    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="safe message",
        args=(),
        exc_info=None,
    )
    record.context = {"unsafe\ninjected=field": "value"}

    output = TextFormatter().format(record)

    assert "\n" not in output
    assert "injected" not in output


@pytest.mark.parametrize("formatter", [JsonFormatter(), TextFormatter()])
def test_formatter_does_not_log_exception_message_or_traceback(
    formatter: logging.Formatter,
) -> None:
    try:
        raise ValueError("token=secret-value")
    except ValueError:
        exc_info = sys.exc_info()

    record = ContextLogRecord(
        name="graphx_workers",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="operation failed",
        args=(),
        exc_info=exc_info,
    )
    record.context = {"exception": "forged"}

    output = formatter.format(record)

    assert "ValueError" in output
    assert "builtins" in output
    assert "secret-value" not in output
    assert "Traceback" not in output
