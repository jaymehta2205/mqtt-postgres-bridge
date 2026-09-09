"""Topic templates, payload decoding and the topic-to-table router."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .ports import Message, Row

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
_PLACEHOLDER = re.compile(r"^\{([a-z_][a-z0-9_]*)\}$")


class MappingError(ValueError):
    """A topic template, table or column name in the configuration is invalid."""


class RejectedMessage(Exception):
    """The message cannot be turned into a row; ``str(exc)`` is the reason."""


def is_identifier(name: str) -> bool:
    """True if ``name`` is a plain lowercase SQL identifier (no quoting surprises)."""
    return _IDENTIFIER.match(name) is not None


def validate_table_name(table: str) -> str:
    parts = table.split(".")
    if not 1 <= len(parts) <= 2 or not all(is_identifier(part) for part in parts):
        raise MappingError(
            f"invalid table name {table!r}: use lowercase letters, digits and underscores, "
            "optionally qualified with a schema"
        )
    return table


@dataclass(frozen=True, slots=True)
class TopicTemplate:
    """A topic pattern such as ``plant/{site}/{machine}/energy``.

    ``{name}`` captures one topic level into a column called ``name``. Plain MQTT
    wildcards are allowed too: ``+`` matches one level without capturing it and a
    trailing ``#`` matches the remainder of the topic.
    """

    template: str
    filter: str
    """The MQTT subscription filter that delivers every topic this template matches."""
    names: tuple[str, ...]
    pattern: re.Pattern[str]

    @classmethod
    def parse(cls, template: str) -> TopicTemplate:
        levels = template.split("/")
        if "" in levels:
            raise MappingError(f"topic template {template!r} has an empty level")
        filter_levels: list[str] = []
        regex_levels: list[str] = []
        names: list[str] = []
        for index, level in enumerate(levels):
            if placeholder := _PLACEHOLDER.match(level):
                name = placeholder.group(1)
                if name in names:
                    raise MappingError(f"duplicate placeholder {{{name}}} in {template!r}")
                names.append(name)
                filter_levels.append("+")
                regex_levels.append("([^/]+)")
            elif level == "+":
                filter_levels.append("+")
                regex_levels.append("[^/]+")
            elif level == "#":
                if index != len(levels) - 1:
                    raise MappingError(f"'#' must be the last level in {template!r}")
                filter_levels.append("#")
            elif any(char in level for char in "{}+#"):
                raise MappingError(f"invalid level {level!r} in topic template {template!r}")
            else:
                filter_levels.append(level)
                regex_levels.append(re.escape(level))
        regex = "/".join(regex_levels)
        if filter_levels[-1] == "#":
            # Like the broker, "a/#" matches "a" itself as well as everything below it.
            regex = f"{regex}(?:/.*)?" if regex else ".*"
        return cls(template, "/".join(filter_levels), tuple(names), re.compile(f"^{regex}$"))

    def match(self, topic: str) -> dict[str, str] | None:
        """Captured levels keyed by placeholder name, or None if the topic does not match."""
        matched = self.pattern.match(topic)
        if matched is None:
            return None
        return dict(zip(self.names, matched.groups(), strict=True))


def decode_payload(payload: bytes) -> dict[str, Any]:
    """Parse a JSON object payload; anything else is a :class:`RejectedMessage`."""
    try:
        decoded = json.loads(payload)
    except ValueError as exc:  # JSONDecodeError and UnicodeDecodeError
        raise RejectedMessage(f"payload is not valid JSON: {exc}") from None
    if not isinstance(decoded, dict):
        raise RejectedMessage(f"payload must be a JSON object, got {type(decoded).__name__}")
    return decoded


def _to_sql_value(value: Any) -> Any:
    """Scalars pass through; nested objects and arrays are serialised for json/jsonb columns."""
    if isinstance(value, dict | list):
        return json.dumps(value, separators=(",", ":"))
    return value


@dataclass(frozen=True, slots=True)
class TableMapping:
    """Routes messages matching ``topic`` into ``table``.

    Columns are emitted in this order: topic captures, payload fields, then the
    timestamp column. With ``fields`` unset every top-level payload key becomes a
    column; with it set only the listed keys are used and missing ones are NULL.
    """

    topic: TopicTemplate
    table: str
    fields: tuple[str, ...] | None = None
    timestamp_column: str | None = "received_at"

    def __post_init__(self) -> None:
        validate_table_name(self.table)
        names = list(self.topic.names)
        if self.timestamp_column is not None:
            names.append(self.timestamp_column)
        names.extend(self.fields or ())
        seen: set[str] = set()
        for name in names:
            if not is_identifier(name):
                raise MappingError(f"invalid column name {name!r} for table {self.table!r}")
            if name in seen:
                raise MappingError(f"column {name!r} is defined twice for table {self.table!r}")
            seen.add(name)

    def to_row(self, message: Message, captures: Mapping[str, str]) -> Row:
        payload = decode_payload(message.payload)
        if self.fields is None:
            for key in payload:
                if key in captures or key == self.timestamp_column:
                    raise RejectedMessage(
                        f"payload field {key!r} collides with a column derived from the topic"
                    )
                if not is_identifier(key):
                    raise RejectedMessage(f"payload field {key!r} is not a valid column name")
            keys: tuple[str, ...] = tuple(payload)
        else:
            keys = self.fields
        columns = [*captures, *keys]
        values: list[Any] = [*captures.values(), *(_to_sql_value(payload.get(k)) for k in keys)]
        if self.timestamp_column is not None:
            columns.append(self.timestamp_column)
            values.append(datetime.fromtimestamp(message.received_at, tz=UTC))
        return Row(self.table, tuple(columns), tuple(values))


class TopicRouter:
    """Turns a message into a row using the first mapping whose template matches."""

    def __init__(self, mappings: Sequence[TableMapping]) -> None:
        if not mappings:
            raise MappingError("at least one mapping is required")
        self._mappings = tuple(mappings)

    @property
    def filters(self) -> tuple[str, ...]:
        """Distinct MQTT subscription filters, in configuration order."""
        return tuple(dict.fromkeys(mapping.topic.filter for mapping in self._mappings))

    def route(self, message: Message) -> Row:
        for mapping in self._mappings:
            captures = mapping.topic.match(message.topic)
            if captures is not None:
                return mapping.to_row(message, captures)
        raise RejectedMessage(f"no mapping matches topic {message.topic!r}")
