"""Typed error boundary used by adapters and the runtime."""

from enum import StrEnum


class ErrorCode(StrEnum):
    SOURCE_TIMEOUT = "SOURCE_TIMEOUT"
    SOURCE_NETWORK_ERROR = "SOURCE_NETWORK_ERROR"
    SOURCE_RATE_LIMITED = "SOURCE_RATE_LIMITED"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    SOURCE_ACCESS_DENIED = "SOURCE_ACCESS_DENIED"
    SOURCE_ACTION_REQUIRED = "SOURCE_ACTION_REQUIRED"
    SOURCE_FORMAT_CHANGED = "SOURCE_FORMAT_CHANGED"
    SOURCE_INVALID_RESPONSE = "SOURCE_INVALID_RESPONSE"
    SOURCE_UNSUPPORTED_OPERATION = "SOURCE_UNSUPPORTED_OPERATION"
    DOCUMENT_UNSUPPORTED = "DOCUMENT_UNSUPPORTED"
    CONTRACT_VALIDATION_FAILED = "CONTRACT_VALIDATION_FAILED"
    INTERNAL_ADAPTER_ERROR = "INTERNAL_ADAPTER_ERROR"


class ErrorClassification(StrEnum):
    TRANSIENT = "transient"
    RATE_LIMITED = "rate_limited"
    PERMANENT = "permanent"
    ACTION_REQUIRED = "action_required"
    BUG = "bug"


ERROR_CLASSIFICATION: dict[ErrorCode, ErrorClassification] = {
    ErrorCode.SOURCE_TIMEOUT: ErrorClassification.TRANSIENT,
    ErrorCode.SOURCE_NETWORK_ERROR: ErrorClassification.TRANSIENT,
    ErrorCode.SOURCE_RATE_LIMITED: ErrorClassification.RATE_LIMITED,
    ErrorCode.SOURCE_UNAVAILABLE: ErrorClassification.TRANSIENT,
    ErrorCode.SOURCE_ACCESS_DENIED: ErrorClassification.PERMANENT,
    ErrorCode.SOURCE_ACTION_REQUIRED: ErrorClassification.ACTION_REQUIRED,
    ErrorCode.SOURCE_FORMAT_CHANGED: ErrorClassification.PERMANENT,
    ErrorCode.SOURCE_INVALID_RESPONSE: ErrorClassification.PERMANENT,
    ErrorCode.SOURCE_UNSUPPORTED_OPERATION: ErrorClassification.PERMANENT,
    ErrorCode.DOCUMENT_UNSUPPORTED: ErrorClassification.PERMANENT,
    ErrorCode.CONTRACT_VALIDATION_FAILED: ErrorClassification.BUG,
    ErrorCode.INTERNAL_ADAPTER_ERROR: ErrorClassification.BUG,
}


class AdapterError(RuntimeError):
    def __init__(
        self,
        code: ErrorCode,
        safe_detail: str,
        *,
        retry_after_seconds: int | None = None,
    ) -> None:
        if not safe_detail:
            raise ValueError("safe_detail must not be empty")
        if retry_after_seconds is not None and retry_after_seconds < 1:
            raise ValueError("retry_after_seconds must be positive")
        super().__init__(safe_detail)
        self.code = code
        self.classification = ERROR_CLASSIFICATION[code]
        self.safe_detail = safe_detail
        self.retry_after_seconds = retry_after_seconds
