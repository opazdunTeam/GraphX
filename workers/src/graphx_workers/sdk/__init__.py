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
from graphx_workers.sdk.errors import (
    ActionRequiredError,
    AdapterError,
    ErrorClassification,
    ErrorCode,
    PermanentAdapterError,
    RateLimitedError,
    TransientAdapterError,
)

__all__ = [
    "AdapterContext",
    "AdapterError",
    "ActionRequiredError",
    "CollectRequest",
    "CollectResult",
    "ErrorClassification",
    "ErrorCode",
    "HTTPClient",
    "HTTPResponse",
    "ProgressReporter",
    "PermanentAdapterError",
    "RateLimiter",
    "RateLimitedError",
    "SecretProvider",
    "SourceAdapter",
    "TransientAdapterError",
]
