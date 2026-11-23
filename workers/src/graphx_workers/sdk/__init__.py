"""Stable interfaces available to source adapters."""

from graphx_workers.contracts.artifacts import ArtifactReference
from graphx_workers.contracts.claims import Claim
from graphx_workers.contracts.records import SourceRecord
from graphx_workers.sdk.adapter import (
    AdapterContext,
    AdapterMetadata,
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
    "AdapterMetadata",
    "AdapterError",
    "ActionRequiredError",
    "ArtifactReference",
    "Claim",
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
    "SourceRecord",
    "SourceAdapter",
    "TransientAdapterError",
]
