"""Adapter exceptions backed by the canonical transport error catalog."""

from graphx_workers.contracts.errors import (
    ERROR_CLASSIFICATION,
    ErrorClassification,
    ErrorCode,
)

__all__ = [
    "ERROR_CLASSIFICATION",
    "AdapterError",
    "ErrorClassification",
    "ErrorCode",
    "ActionRequiredError",
    "PermanentAdapterError",
    "RateLimitedError",
    "TransientAdapterError",
]

MAX_SAFE_ERROR_DETAIL_LENGTH = 2_000


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
        if len(safe_detail) > MAX_SAFE_ERROR_DETAIL_LENGTH:
            raise ValueError(
                f"safe_detail must not exceed {MAX_SAFE_ERROR_DETAIL_LENGTH} characters"
            )
        if retry_after_seconds is not None and retry_after_seconds < 1:
            raise ValueError("retry_after_seconds must be positive")
        super().__init__(safe_detail)
        self.code = code
        self.classification = ERROR_CLASSIFICATION[code]
        self.safe_detail = safe_detail
        self.retry_after_seconds = retry_after_seconds


class _ClassifiedAdapterError(AdapterError):
    expected_classification: ErrorClassification

    def __init__(
        self,
        code: ErrorCode,
        safe_detail: str,
        *,
        retry_after_seconds: int | None = None,
    ) -> None:
        if ERROR_CLASSIFICATION[code] is not self.expected_classification:
            raise ValueError(
                f"{code.value} is not a {self.expected_classification.value} error code"
            )
        super().__init__(
            code,
            safe_detail,
            retry_after_seconds=retry_after_seconds,
        )


class TransientAdapterError(_ClassifiedAdapterError):
    expected_classification = ErrorClassification.TRANSIENT


class RateLimitedError(_ClassifiedAdapterError):
    expected_classification = ErrorClassification.RATE_LIMITED


class PermanentAdapterError(_ClassifiedAdapterError):
    expected_classification = ErrorClassification.PERMANENT


class ActionRequiredError(_ClassifiedAdapterError):
    expected_classification = ErrorClassification.ACTION_REQUIRED
