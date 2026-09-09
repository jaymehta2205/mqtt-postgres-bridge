"""YAML configuration: loading, ``${ENV}`` expansion and validation."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .bridge import BridgeSettings, RetryPolicy
from .mapping import TableMapping, TopicTemplate

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_MISSING = object()


class ConfigError(ValueError):
    """The configuration file is missing, malformed or fails validation."""


@dataclass(frozen=True, slots=True)
class MqttConfig:
    host: str = "localhost"
    port: int = 1883
    username: str | None = None
    password: str | None = None
    client_id: str = "mqtt-pg-bridge"
    qos: int = 1


@dataclass(frozen=True, slots=True)
class PostgresConfig:
    dsn: str
    pool_size: int = 4


@dataclass(frozen=True, slots=True)
class Config:
    mqtt: MqttConfig
    postgres: PostgresConfig
    spool_path: Path
    bridge: BridgeSettings
    mappings: tuple[TableMapping, ...]


def load_config(path: str | Path) -> Config:
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read {path}: {exc.strerror}") from exc
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path} is not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"{path} must contain a mapping at the top level")
    return parse_config(expand_env(raw))


def expand_env(value: Any) -> Any:
    """Replace ``${NAME}`` and ``${NAME:-default}`` in string leaves with environment values."""
    if isinstance(value, str):
        return _ENV_REF.sub(_resolve_env, value)
    if isinstance(value, dict):
        return {key: expand_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [expand_env(item) for item in value]
    return value


def _resolve_env(match: re.Match[str]) -> str:
    name, default = match.group(1), match.group(2)
    resolved = os.environ.get(name, default)
    if resolved is None:
        raise ConfigError(
            f"environment variable {name} is not set (referenced as {match.group(0)})"
        )
    return resolved


def parse_config(raw: Mapping[str, Any]) -> Config:
    _check_keys(raw, "top level", {"mqtt", "postgres", "spool", "batching", "retry", "mappings"})
    mqtt = _section(raw, "mqtt", required=False)
    postgres = _section(raw, "postgres")
    spool = _section(raw, "spool", required=False)
    batching = _section(raw, "batching", required=False)
    retry = _section(raw, "retry", required=False)
    mappings = raw.get("mappings")
    if not isinstance(mappings, list) or not mappings:
        raise ConfigError("'mappings' must be a non-empty list")

    _check_keys(mqtt, "mqtt", {"host", "port", "username", "password", "client_id", "qos"})
    _check_keys(postgres, "postgres", {"dsn", "pool_size"})
    _check_keys(spool, "spool", {"path"})
    _check_keys(batching, "batching", {"batch_size", "flush_interval", "queue_size"})
    _check_keys(retry, "retry", {"max_attempts", "initial_delay", "max_delay", "multiplier"})

    qos = _field(mqtt, "mqtt", "qos", int, 1)
    if qos not in (0, 1, 2):
        raise ConfigError("mqtt.qos must be 0, 1 or 2")
    port = _field(mqtt, "mqtt", "port", int, 1883)
    if not 1 <= port <= 65535:
        raise ConfigError("mqtt.port must be between 1 and 65535")
    pool_size = _field(postgres, "postgres", "pool_size", int, 4)
    if pool_size < 1:
        raise ConfigError("postgres.pool_size must be at least 1")

    try:
        settings = BridgeSettings(
            batch_size=_field(batching, "batching", "batch_size", int, 500),
            flush_interval=_field(batching, "batching", "flush_interval", float, 2.0),
            queue_size=_field(batching, "batching", "queue_size", int, 10_000),
            retry=RetryPolicy(
                max_attempts=_field(retry, "retry", "max_attempts", int, 3),
                initial_delay=_field(retry, "retry", "initial_delay", float, 0.5),
                max_delay=_field(retry, "retry", "max_delay", float, 30.0),
                multiplier=_field(retry, "retry", "multiplier", float, 2.0),
            ),
        )
        parsed_mappings = tuple(_parse_mapping(item, index) for index, item in enumerate(mappings))
    except ConfigError:
        raise
    except ValueError as exc:  # includes MappingError
        raise ConfigError(str(exc)) from exc

    return Config(
        mqtt=MqttConfig(
            host=_field(mqtt, "mqtt", "host", str, "localhost"),
            port=port,
            username=_field(mqtt, "mqtt", "username", str, None),
            password=_field(mqtt, "mqtt", "password", str, None),
            client_id=_field(mqtt, "mqtt", "client_id", str, "mqtt-pg-bridge"),
            qos=qos,
        ),
        postgres=PostgresConfig(dsn=_field(postgres, "postgres", "dsn", str), pool_size=pool_size),
        spool_path=Path(_field(spool, "spool", "path", str, "spool.sqlite")),
        bridge=settings,
        mappings=parsed_mappings,
    )


def _parse_mapping(item: Any, index: int) -> TableMapping:
    name = f"mappings[{index}]"
    if not isinstance(item, dict):
        raise ConfigError(f"{name} must be a mapping with 'topic' and 'table'")
    _check_keys(item, name, {"topic", "table", "fields", "timestamp_column"})
    fields = item.get("fields")
    if fields is not None and not (
        isinstance(fields, list) and all(isinstance(f, str) for f in fields)
    ):
        raise ConfigError(f"{name}.fields must be a list of column names")
    # Absent means the default column; an explicit null disables the timestamp column.
    timestamp_column = item.get("timestamp_column", "received_at")
    if timestamp_column is not None and not isinstance(timestamp_column, str):
        raise ConfigError(f"{name}.timestamp_column must be a string or null")
    return TableMapping(
        topic=TopicTemplate.parse(_field(item, name, "topic", str)),
        table=_field(item, name, "table", str),
        fields=None if fields is None else tuple(fields),
        timestamp_column=timestamp_column,
    )


def _section(raw: Mapping[str, Any], key: str, *, required: bool = True) -> dict[str, Any]:
    value = raw.get(key)
    if value is None:
        if required:
            raise ConfigError(f"missing section '{key}'")
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"'{key}' must be a mapping")
    return value


def _check_keys(section: Mapping[str, Any], name: str, allowed: set[str]) -> None:
    unknown = sorted(set(section) - allowed)
    if unknown:
        raise ConfigError(f"unknown key(s) in {name}: {', '.join(map(str, unknown))}")


def _field(
    section: Mapping[str, Any], name: str, key: str, type_: type, default: Any = _MISSING
) -> Any:
    """Fetch ``section[key]`` checking its type; ``default=None`` marks an optional field."""
    value = section.get(key, default)
    if value is _MISSING:
        raise ConfigError(f"{name}.{key} is required")
    if value is None and default is None:
        return None
    if type_ is float and isinstance(value, int) and not isinstance(value, bool):
        value = float(value)
    if not isinstance(value, type_) or (isinstance(value, bool) and type_ is not bool):
        raise ConfigError(f"{name}.{key} must be {type_.__name__}, got {type(value).__name__}")
    return value
