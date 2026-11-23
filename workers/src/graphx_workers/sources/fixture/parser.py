"""Strict parser for the synthetic fixture response shape."""

import json
from dataclasses import dataclass
from typing import Any, Literal, cast


class FixtureFormatError(ValueError):
    """Fixture bytes do not match the declared source response shape."""


@dataclass(frozen=True, slots=True)
class FixturePerson:
    external_id: str
    name: str


@dataclass(frozen=True, slots=True)
class ParsedFixtureResponse:
    outcome: Literal["success", "not_found"]
    person: FixturePerson | None


def parse_fixture_response(content: bytes) -> ParsedFixtureResponse:
    try:
        decoded = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FixtureFormatError("fixture response is not valid JSON") from error
    if not isinstance(decoded, dict) or set(decoded) != {"case", "person"}:
        raise FixtureFormatError("fixture response has an unexpected top-level shape")
    case = decoded["case"]
    person = decoded["person"]
    if case == "not_found" and person is None:
        return ParsedFixtureResponse(outcome="not_found", person=None)
    if case != "success" or not isinstance(person, dict):
        raise FixtureFormatError("fixture response outcome and person are inconsistent")
    return ParsedFixtureResponse(
        outcome="success", person=_parse_person(cast(dict[str, Any], person))
    )


def _parse_person(value: dict[str, Any]) -> FixturePerson:
    if set(value) != {"external_id", "name"}:
        raise FixtureFormatError("fixture person has an unexpected shape")
    external_id = value["external_id"]
    name = value["name"]
    if not isinstance(external_id, str) or not external_id.strip():
        raise FixtureFormatError("fixture person external_id is invalid")
    if not isinstance(name, str) or not name.strip():
        raise FixtureFormatError("fixture person name is invalid")
    return FixturePerson(external_id=external_id, name=name)
