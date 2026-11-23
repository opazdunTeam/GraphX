"""In-process deterministic fixture responses; no network access is performed."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

FixtureCase = Literal["success", "not_found", "transient", "permanent"]

_RETRIEVED_AT = datetime(2026, 11, 2, 12, 0, 3, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class FixtureResponse:
    content: bytes
    source_uri: str
    retrieved_at: datetime
    etag: str


class FixtureUnavailableError(ConnectionError):
    """Synthetic pre-response failure used to exercise transient handling."""


class FixtureClient:
    def fetch(self, fixture_case: FixtureCase) -> FixtureResponse:
        if fixture_case == "transient":
            raise FixtureUnavailableError("fixture source is temporarily unavailable")
        if fixture_case == "success":
            return FixtureResponse(
                content=(
                    b'{"case":"success","person":{"external_id":"fixture-person-1",'
                    b'"name":"Ada Fixture"}}'
                ),
                source_uri="https://example.test/fixture/person/1",
                retrieved_at=_RETRIEVED_AT,
                etag="fixture-response-v1",
            )
        if fixture_case == "not_found":
            return FixtureResponse(
                content=b'{"case":"not_found","person":null}',
                source_uri="https://example.test/fixture/person/not-found",
                retrieved_at=_RETRIEVED_AT,
                etag="fixture-not-found-v1",
            )
        return FixtureResponse(
            content=b'{"case":"permanent","person":',
            source_uri="https://example.test/fixture/person/invalid",
            retrieved_at=_RETRIEVED_AT,
            etag="fixture-invalid-v1",
        )
