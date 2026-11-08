"""Stable interfaces available to source adapters."""

from graphx_workers.sdk.adapter import (
    AdapterContext,
    CollectRequest,
    CollectResult,
    HTTPClient,
    HTTPResponse,
    ProgressReporter,
    RateLimiter,
    SecretProvider,
    SourceAdapter,
)
from graphx_workers.sdk.errors import AdapterError, ErrorClassification, ErrorCode

__all__ = [
    "AdapterContext",
    "AdapterError",
    "CollectRequest",
    "CollectResult",
    "ErrorClassification",
    "ErrorCode",
    "HTTPClient",
    "HTTPResponse",
    "ProgressReporter",
    "RateLimiter",
    "SecretProvider",
    "SourceAdapter",
]
