"""Shared semantic checks for values that will be persisted by Go."""

from collections.abc import Mapping
from math import isfinite
from unicodedata import category

MAX_SAFE_INTEGER = 2**53 - 1


def has_meaningful_text(value: str) -> bool:
    return any(not char.isspace() and category(char) != "Cc" for char in value)


def validate_persisted_text(value: str, field: str) -> str:
    if "\x00" in value:
        raise ValueError(f"{field} must not contain U+0000")
    if not has_meaningful_text(value):
        raise ValueError(f"{field} must not be blank")
    return value


def validate_jsonb_compatible[T](value: T, field: str) -> T:
    """Require the canonical JSON domain accepted by JCS and PostgreSQL JSONB."""
    pending = [value]
    visited: set[int] = set()
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            if "\x00" in item:
                raise ValueError(f"{field} must not contain U+0000")
            continue
        if item is None or isinstance(item, bool):
            continue
        if isinstance(item, int):
            if abs(item) > MAX_SAFE_INTEGER:
                raise ValueError(f"{field} integer is outside the safe integer domain")
            continue
        if isinstance(item, float):
            if not isfinite(item):
                raise ValueError(f"{field} must contain only finite JSON numbers")
            continue
        if isinstance(item, Mapping):
            identity = id(item)
            if identity in visited:
                continue
            visited.add(identity)
            if any(not isinstance(key, str) for key in item):
                raise ValueError(f"{field} object keys must be strings")
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            identity = id(item)
            if identity in visited:
                continue
            visited.add(identity)
            pending.extend(item)
        else:
            raise ValueError(f"{field} contains a non-JSON value of type {type(item).__name__}")
    return value
