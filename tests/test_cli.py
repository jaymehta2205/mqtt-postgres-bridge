from __future__ import annotations

from pathlib import Path

import pytest

from mqtt_pg_bridge import Message, Spool
from mqtt_pg_bridge.cli import build_parser, main


def write_config(tmp_path: Path) -> Path:
    config = tmp_path / "config.yaml"
    spool = (tmp_path / "spool.sqlite").as_posix()
    config.write_text(
        "postgres:\n"
        "  dsn: postgresql://localhost/telemetry\n"
        f"spool:\n  path: {spool}\n"
        "mappings:\n"
        "  - topic: plant/{site}/energy\n"
        "    table: energy\n"
    )
    return config


def test_inspect_reports_spool_depth_and_dead_letters(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = write_config(tmp_path)
    with Spool(tmp_path / "spool.sqlite") as spool:
        spool.push([Message("plant/a/energy", b'{"kwh": 1}', 0.0)])
        spool.dead_letter(Message("plant/b/energy", b"not json", 0.0), "payload is not valid JSON")

    assert main(["inspect", "--config", str(config)]) == 0
    out = capsys.readouterr().out
    assert "pending:      1" in out
    assert "dead letters: 1" in out
    assert "plant/b/energy: payload is not valid JSON" in out
    assert "not json" in out


def test_invalid_config_exits_with_status_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = tmp_path / "config.yaml"
    config.write_text("postgres:\n  dsn: postgresql://localhost/telemetry\nmappings: []\n")

    assert main(["run", "--config", str(config)]) == 2
    assert "error: 'mappings' must be a non-empty list" in capsys.readouterr().err


def test_config_flag_is_required() -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["run"])
    assert exc.value.code == 2
