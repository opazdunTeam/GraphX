"""Structured stdout logging with allowlisted and redacted operational context."""

import json
import logging
import math
import re
import sys
from collections.abc import Mapping
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum
from itertools import islice
from pathlib import PurePath
from typing import Any, Literal
from uuid import UUID

_MAX_COLLECTION_ITEMS = 50
_MAX_INTEGER_BITS = 256
_MAX_KEY_LENGTH = 128
_MAX_STRING_LENGTH = 512
_MAX_MESSAGE_LENGTH = 1_024
_MAX_EVENT_BYTES = 16 * 1_024
_REDACTED = "<redacted>"
_RESERVED_EVENT_FIELDS = frozenset(
    {"timestamp", "level", "service", "message", "exception", "context_truncated"}
)
_SAFE_TEXT_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
_CAMEL_CASE_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_ALPHANUMERIC = re.compile(r"[^a-z0-9]+")
_SURROGATE = re.compile(r"[\ud800-\udfff]")
_ALLOWED_CONTEXT_FIELDS = frozenset(
    {
        "adapter_version",
        "artifact_count",
        "attempt_no",
        "claim_count",
        "completed",
        "concurrency",
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
        "object_store_configured",
        "occurred_at",
        "operation",
        "outcome",
        "page_number",
        "parser_version",
        "playwright_enabled",
        "purpose",
        "rabbitmq_configured",
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
    }
)
_SECRET_KEY_PARTS = frozenset(
    {
        "authorization",
        "cookie",
        "credential",
        "passwd",
        "password",
        "secret",
        "token",
    }
)
_SECRET_COMPACT_SUFFIXES = (
    "accesstoken",
    "refreshtoken",
    "authorization",
    "clientsecret",
    "connectionstring",
    "credential",
    "credentials",
    "password",
    "passwd",
    "sessionid",
    "apikey",
    "accesskey",
    "privatekey",
    "signingkey",
    "secret",
    "token",
    "cookie",
)


def _bounded_string(value: str, limit: int = _MAX_STRING_LENGTH) -> str:
    marker = "<truncated>"
    value = _SURROGATE.sub("\ufffd", value)
    if len(value) <= limit:
        return value
    return f"{value[: limit - len(marker)]}{marker}"


def _normalize_key(key: str) -> str:
    key = _bounded_string(key, _MAX_KEY_LENGTH)
    snake_case = _CAMEL_CASE_BOUNDARY.sub("_", key)
    return _NON_ALPHANUMERIC.sub("_", snake_case.casefold()).strip("_")


def _is_sensitive_key(key: str) -> bool:
    normalized = _normalize_key(key)
    parts = set(normalized.split("_"))
    compact = normalized.replace("_", "")

    if parts & _SECRET_KEY_PARTS:
        return True
    if "key" in parts and parts & {"access", "api", "client", "private", "signing"}:
        return True
    if compact.endswith(_SECRET_COMPACT_SUFFIXES):
        return True
    if normalized in {"url", "uri"} or normalized.endswith(("_url", "_uri")):
        return True
    return normalized in {
        "body",
        "content",
        "document",
        "form",
        "headers",
        "params",
        "query",
        "response",
        "payload",
    } or normalized.endswith(
        (
            "_body",
            "_content",
            "_document",
            "_form",
            "_headers",
            "_params",
            "_query",
            "_response",
            "_payload",
        )
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, Enum):
        return _json_safe(value.value)
    if isinstance(value, str):
        return _bounded_string(value)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value if value.bit_length() <= _MAX_INTEGER_BITS else "<int:out-of-range>"
    if isinstance(value, float):
        return value if math.isfinite(value) else "<non-finite-float>"
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, PurePath):
        return _bounded_string(str(value))
    if isinstance(value, Decimal):
        return _bounded_string(str(value))
    if isinstance(value, (bytes, bytearray, memoryview)):
        return f"<{type(value).__name__}>"
    if isinstance(value, (Mapping, list, tuple, set, frozenset)):
        return f"<{type(value).__name__}:redacted>"
    return f"<{type(value).__name__}>"


def _safe_message(record: logging.LogRecord) -> str:
    try:
        return _bounded_string(record.getMessage(), _MAX_MESSAGE_LENGTH)
    except Exception:
        return "<unformattable-message>"


def _event(record: logging.LogRecord) -> dict[str, Any]:
    event: dict[str, Any] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "level": _bounded_string(record.levelname.lower(), _MAX_KEY_LENGTH),
        "service": "graphx-workers",
        "message": _safe_message(record),
    }
    context = getattr(record, "context", None)
    if isinstance(context, dict):
        context_items = list(islice(context.items(), _MAX_COLLECTION_ITEMS + 1))
        for key, value in context_items[:_MAX_COLLECTION_ITEMS]:
            if not isinstance(key, str):
                continue
            normalized_key = _normalize_key(key)
            if not normalized_key or normalized_key in _RESERVED_EVENT_FIELDS:
                continue
            if _is_sensitive_key(normalized_key):
                event[normalized_key] = _REDACTED
            elif normalized_key in _ALLOWED_CONTEXT_FIELDS:
                event[normalized_key] = _json_safe(value)
        if len(context_items) > _MAX_COLLECTION_ITEMS:
            event["context_truncated"] = True
    if record.exc_info:
        exception_type = record.exc_info[0]
        if exception_type is not None:
            event["exception"] = {
                "type": _bounded_string(exception_type.__name__, _MAX_KEY_LENGTH),
                "module": _bounded_string(exception_type.__module__, _MAX_KEY_LENGTH),
            }
    if (
        len(json.dumps(event, ensure_ascii=False, allow_nan=False).encode("utf-8"))
        > _MAX_EVENT_BYTES
    ):
        event = {
            key: value
            for key, value in event.items()
            if key in {"timestamp", "level", "service", "message", "exception"}
        }
        event["context_truncated"] = True
    return event


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            _event(record), ensure_ascii=False, allow_nan=False, separators=(",", ":")
        )


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event = _event(record)
        return " ".join(
            f"{key if _SAFE_TEXT_KEY.fullmatch(key) else json.dumps(key, ensure_ascii=False)}="
            f"{json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':'))}"
            for key, value in event.items()
        )


def configure_logging(
    level: str, output_format: Literal["json", "text"] = "json"
) -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if output_format == "json" else TextFormatter())

    logger = logging.getLogger("graphx_workers")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(level.upper())
    logger.propagate = False
    return logger
