from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from mqtt_pg_bridge.config import ConfigError, expand_env, load_config, parse_config

REPO = Path(__file__).resolve().parents[1]
MINIMAL: dict[str, Any] = {
    "postgres": {"dsn": "postgresql://localhost/telemetry"},
    "mappings": [{"topic": "a/{x}", "table": "t"}],
}


def test_sample_config_loads_and_resolves_env_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PG_PASSWORD", raising=False)
    config = load_config(REPO / "config.yaml")

    assert config.postgres.dsn == "postgresql://bridge:bridge@localhost:5432/telemetry"
    assert config.mqtt.host == "localhost"
    assert config.mqtt.qos == 1
    assert config.mqtt.username is None
    assert config.spool_path == Path("./spool.sqlite")
    assert (config.bridge.batch_size, config.bridge.flush_interval) == (500, 2.0)
    assert config.bridge.retry.max_attempts == 3
    assert [m.table for m in config.mappings] == ["energy_readings", "machine_status"]
    assert config.mappings[0].topic.filter == "plant/+/+/energy"
    assert config.mappings[0].fields == ("kwh", "voltage_v", "current_a", "power_factor")
    assert config.mappings[1].fields is None


def test_env_references_are_expanded_in_string_leaves(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PG_PASSWORD", "s3cret")
    monkeypatch.delenv("DB", raising=False)
    raw = {"dsn": "pg://u:${PG_PASSWORD}@h/${DB:-telemetry}", "n": 5, "l": ["${PG_PASSWORD}"]}
    assert expand_env(raw) == {"dsn": "pg://u:s3cret@h/telemetry", "n": 5, "l": ["s3cret"]}


def test_unset_env_variable_without_default_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NOPE", raising=False)
    with pytest.raises(ConfigError, match="NOPE"):
        expand_env("${NOPE}")


def test_minimal_config_gets_defaults() -> None:
    config = parse_config(MINIMAL)
    assert config.mqtt.port == 1883
    assert config.postgres.pool_size == 4
    assert config.bridge.queue_size == 10_000
    assert config.bridge.retry.max_delay == 30.0
    assert config.mappings[0].timestamp_column == "received_at"


def test_timestamp_column_null_disables_it() -> None:
    raw = {**MINIMAL, "mappings": [{"topic": "a/{x}", "table": "t", "timestamp_column": None}]}
    assert parse_config(raw).mappings[0].timestamp_column is None


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ({**MINIMAL, "mappings": []}, "non-empty list"),
        ({"mappings": MINIMAL["mappings"]}, "missing section 'postgres'"),
        ({**MINIMAL, "extra": 1}, "unknown key.*top level: extra"),
        ({**MINIMAL, "batching": {"flush_intervl": 1}}, "unknown key.*batching: flush_intervl"),
        ({**MINIMAL, "batching": {"batch_size": "many"}}, "batch_size must be int"),
        ({**MINIMAL, "batching": {"batch_size": 0}}, "batch_size"),
        ({**MINIMAL, "retry": {"max_attempts": 0}}, "max_attempts"),
        ({**MINIMAL, "mqtt": {"qos": 3}}, "qos"),
        ({**MINIMAL, "mqtt": {"port": True}}, "port must be int"),
        ({**MINIMAL, "mappings": [{"topic": "a/{x}/{x}", "table": "t"}]}, "duplicate placeholder"),
        ({**MINIMAL, "mappings": [{"topic": "a/{x}", "table": "Bad"}]}, "invalid table name"),
        ({**MINIMAL, "mappings": [{"topic": "a/{x}", "table": "t", "fields": "kwh"}]}, "list of"),
        (
            {**MINIMAL, "mappings": [{"topic": "a/{x}", "table": "t", "timestamp_column": 5}]},
            "null",
        ),
        ({**MINIMAL, "mappings": ["a/{x}"]}, r"mappings\[0\] must be a mapping"),
        ({**MINIMAL, "mappings": [{"table": "t"}]}, r"mappings\[0\].topic is required"),
    ],
)
def test_validation_errors(raw: dict[str, Any], message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        parse_config(raw)


def test_unreadable_or_malformed_files(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read"):
        load_config(tmp_path / "missing.yaml")

    bad = tmp_path / "bad.yaml"
    bad.write_text("- just\n- a list\n")
    with pytest.raises(ConfigError, match="mapping at the top level"):
        load_config(bad)

    bad.write_text("postgres: [unclosed\n")
    with pytest.raises(ConfigError, match="not valid YAML"):
        load_config(bad)
