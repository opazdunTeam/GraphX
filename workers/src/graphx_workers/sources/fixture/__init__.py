"""Deterministic source adapter used by the first vertical slice."""

from graphx_workers.sources.fixture.adapter import FixtureAdapter
from graphx_workers.sources.fixture.metadata import (
    ADAPTER_VERSION,
    FIXTURE_CASE_IDENTIFIER,
    SOURCE_CODE,
    SUPPORTED_OPERATION,
)

__all__ = [
    "ADAPTER_VERSION",
    "FIXTURE_CASE_IDENTIFIER",
    "FixtureAdapter",
    "SOURCE_CODE",
    "SUPPORTED_OPERATION",
]
