"""Typed error boundary used by adapters and the runtime."""

from enum import StrEnum


class ErrorCode(StrEnum):
    SOURCE_TIMEOUT = "SOURCE_TIMEOUT"
    SOURCE_RATE_LIMITED = "SOURCE_RATE_LIMITED"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    SOURCE_ACCESS_DENIED = "SOURCE_ACCESS_DENIED"
    SOURCE_ACTION_REQUIRED = "SOURCE_ACTION_REQUIRED"
    SOURCE_FORMAT_CHANGED = "SOURCE_FORMAT_CHANGED"
    DOCUMENT_UNSUPPORTED = "DOCUMENT_UNSUPPORTED"
    CONTRACT_VALIDATION_FAILED = "CONTRACT_VALIDATION_FAILED"
    INTERNAL_ADAPTER_ERROR = "INTERNAL_ADAPTER_ERROR"


class AdapterError(RuntimeError):
    def __init__(self, code: ErrorCode, safe_detail: str) -> None:
        super().__init__(safe_detail)
        self.code = code
        self.safe_detail = safe_detail
