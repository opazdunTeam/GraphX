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
]


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
