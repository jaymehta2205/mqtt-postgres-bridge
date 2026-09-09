"""Command line entry point: ``mqtt-pg-bridge run|inspect --config config.yaml``."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

from .bridge import Bridge
from .config import Config, ConfigError, load_config
from .mapping import TopicRouter
from .mqtt_source import MqttSource
from .pg_sink import PostgresSink
from .spool import Spool

log = logging.getLogger("mqtt_pg_bridge")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mqtt-pg-bridge",
        description="Subscribe to MQTT topics and batch JSON telemetry into PostgreSQL.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", required=True, metavar="PATH", help="path to config.yaml")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", parents=[common], help="bridge messages until interrupted")
    inspect = commands.add_parser(
        "inspect", parents=[common], help="show spool depth and recent dead letters"
    )
    inspect.add_argument(
        "--dead-letters", type=int, default=10, metavar="N", help="dead letters to list"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.command == "inspect":
        return inspect_spool(config, args.dead_letters)
    try:
        # aiomqtt relies on loop.add_reader(), which the default Proactor loop on Windows lacks.
        with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
            runner.run(run_bridge(config))
    except KeyboardInterrupt:
        # The runner cancelled the main task on Ctrl+C; Bridge.run drained before propagating.
        log.info("interrupted")
    return 0


async def run_bridge(config: Config) -> None:
    source = MqttSource(
        config.mqtt.host,
        config.mqtt.port,
        username=config.mqtt.username,
        password=config.mqtt.password,
        client_id=config.mqtt.client_id,
        qos=config.mqtt.qos,
    )
    sink = PostgresSink(config.postgres.dsn, pool_size=config.postgres.pool_size)
    with Spool(config.spool_path) as spool:
        bridge = Bridge(source, sink, TopicRouter(config.mappings), spool, config.bridge)
        with contextlib.suppress(NotImplementedError):  # no signal handlers on Windows
            asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, bridge.stop)
        try:
            await bridge.run()
        finally:
            await sink.close()


def inspect_spool(config: Config, limit: int) -> int:
    with Spool(config.spool_path) as spool:
        print(f"spool file:   {config.spool_path}")
        print(f"pending:      {spool.pending()}")
        print(f"dead letters: {spool.dead_letter_count()}")
        for entry in spool.dead_letters(limit):
            failed_at = datetime.fromtimestamp(entry.failed_at, tz=UTC).isoformat(
                timespec="seconds"
            )
            payload = entry.message.payload.decode("utf-8", errors="replace")
            print(f"  [{entry.id}] {failed_at} {entry.message.topic}: {entry.reason}")
            print(f"      {payload[:200]}")
    return 0
