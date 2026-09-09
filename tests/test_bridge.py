"""End-to-end tests: fake broker and sink, real SQLite spool, real bridge."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest
from support import FAST, FakeSink, FakeSource, eventually, make_router

from mqtt_pg_bridge import Bridge, BridgeSettings, Message, Spool


@pytest.fixture
def spool(tmp_path: Path) -> Iterator[Spool]:
    with Spool(tmp_path / "spool.sqlite") as spool:
        yield spool


def start(
    source: FakeSource, sink: FakeSink, spool: Spool, settings: BridgeSettings = FAST
) -> tuple[Bridge, asyncio.Task[None]]:
    bridge = Bridge(source, sink, make_router(), spool, settings)
    return bridge, asyncio.create_task(bridge.run())


async def finish(source: FakeSource, task: asyncio.Task[None]) -> None:
    source.close()
    await asyncio.wait_for(task, 5)


async def test_routes_messages_to_tables_end_to_end(spool: Spool) -> None:
    source, sink = FakeSource(), FakeSink()
    bridge, task = start(source, sink, spool)
    source.publish(
        "plant/pune/press-01/energy",
        {"kwh": 12.5, "voltage_v": 415.2, "ignored": 1},
        received_at=1_700_000_000.0,
    )
    source.publish("plant/pune/press-02/energy", {"kwh": 3.0})
    source.publish(
        "plant/pune/press-01/status", {"state": "running", "rpm": 1450, "alarms": ["E12"]}
    )
    await finish(source, task)

    assert source.filters == ("plant/+/+/energy", "plant/+/+/status")
    energy = [row for row in sink.rows if row.table == "energy_readings"]
    status = [row for row in sink.rows if row.table == "machine_status"]
    assert [row.columns for row in energy] == [
        ("site", "machine", "kwh", "voltage_v", "received_at")
    ] * 2
    assert energy[0].values == (
        "pune",
        "press-01",
        12.5,
        415.2,
        datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC),
    )
    assert energy[1].values[:4] == ("pune", "press-02", 3.0, None)  # missing field -> NULL
    assert status[0].columns == ("site", "machine", "state", "rpm", "alarms", "received_at")
    assert status[0].values[2:5] == ("running", 1450, '["E12"]')
    assert (bridge.stats.received, bridge.stats.written, bridge.stats.dead_lettered) == (3, 3, 0)
    assert spool.pending() == 0


async def test_batches_are_capped_at_batch_size_and_keep_order(spool: Spool) -> None:
    source, sink = FakeSource(), FakeSink()
    for i in range(10):
        source.publish(f"plant/s/m{i}/energy", {"kwh": i})
    _, task = start(source, sink, spool)
    await finish(source, task)

    assert [len(write) for write in sink.writes] == [4, 4, 2]
    assert [row.values[1] for row in sink.rows] == [f"m{i}" for i in range(10)]


async def test_partial_batch_is_flushed_after_flush_interval(spool: Spool) -> None:
    source, sink = FakeSource(), FakeSink()
    bridge, task = start(source, sink, spool)
    source.publish("plant/s/m1/energy", {"kwh": 1})

    await eventually(lambda: len(sink.rows) == 1)  # the source stays open
    bridge.stop()
    await asyncio.wait_for(task, 5)


async def test_unroutable_messages_are_dead_lettered_with_reasons(spool: Spool) -> None:
    source, sink = FakeSource(), FakeSink()
    bridge, task = start(source, sink, spool)
    source.publish("plant/s/m1/temperature", {"c": 40})  # no mapping
    source.publish("plant/s/m1/energy", b"not json")
    source.publish("plant/s/m1/status", "[1, 2, 3]")  # not an object
    source.publish("plant/s/m1/status", {"site": "x"})  # collides with a topic capture
    source.publish("plant/s/m1/energy", {"kwh": 2})
    await finish(source, task)

    assert [row.values[:3] for row in sink.rows] == [("s", "m1", 2)]
    dead = list(reversed(spool.dead_letters(10)))  # oldest first
    assert [d.message.topic.rsplit("/", 1)[1] for d in dead] == [
        "temperature",
        "energy",
        "status",
        "status",
    ]
    assert "no mapping matches" in dead[0].reason
    assert "not valid JSON" in dead[1].reason
    assert "must be a JSON object" in dead[2].reason
    assert "collides" in dead[3].reason
    assert dead[1].message.payload == b"not json"
    assert bridge.stats.dead_lettered == 4
    assert spool.pending() == 0


async def test_sink_outage_spools_and_replays_without_loss(spool: Spool) -> None:
    source, sink = FakeSource(), FakeSink()
    sink.available = False
    bridge, task = start(source, sink, spool)
    for i in range(6):
        source.publish(f"plant/s/m{i}/energy", {"kwh": i})
    await eventually(lambda: spool.pending() == 6)
    assert not bridge.sink_healthy
    assert sink.rows == []

    # While the sink is down, new messages go straight to the spool.
    source.publish("plant/s/m6/energy", {"kwh": 6})
    await eventually(lambda: spool.pending() == 7)

    sink.available = True
    await eventually(lambda: len(sink.rows) == 7 and spool.pending() == 0)
    assert bridge.sink_healthy

    # Once recovered, live messages bypass the spool again.
    source.publish("plant/s/m7/energy", {"kwh": 7})
    await eventually(lambda: len(sink.rows) == 8)
    await finish(source, task)

    assert sorted(row.values[1] for row in sink.rows) == [f"m{i}" for i in range(8)]
    assert (bridge.stats.spooled, bridge.stats.replayed, bridge.stats.written) == (7, 7, 8)


async def test_spooled_messages_survive_a_restart(tmp_path: Path) -> None:
    path = tmp_path / "spool.sqlite"
    sink = FakeSink()
    sink.available = False

    with Spool(path) as spool:
        source = FakeSource()
        _, task = start(source, sink, spool)
        source.publish("plant/s/m1/energy", {"kwh": 1})
        source.publish("plant/s/m2/energy", {"kwh": 2})
        await eventually(lambda: spool.pending() == 2)
        await finish(source, task)  # stopped while the sink is still down

    sink.available = True
    with Spool(path) as spool:  # a new process picks up the same file
        assert spool.pending() == 2
        source = FakeSource()
        bridge, task = start(source, sink, spool)
        await eventually(lambda: spool.pending() == 0)
        await finish(source, task)

    assert [row.values[1] for row in sink.rows] == ["m1", "m2"]
    assert bridge.stats.replayed == 2


async def test_permanently_rejected_rows_are_isolated_and_dead_lettered(spool: Spool) -> None:
    source, sink = FakeSource(), FakeSink()
    sink.reject_when = lambda row: "value too long" if row.values[1] == "m2" else None
    bridge, task = start(source, sink, spool)
    for i in range(1, 4):
        source.publish(f"plant/s/m{i}/energy", {"kwh": i})
    await finish(source, task)

    assert [row.values[1] for row in sink.rows] == ["m1", "m3"]
    (dead,) = spool.dead_letters(10)
    assert dead.message.topic == "plant/s/m2/energy"
    assert "value too long" in dead.reason
    assert (bridge.stats.written, bridge.stats.dead_lettered) == (2, 1)
    assert [len(write) for write in sink.writes] == [3, 1, 1, 1]  # one batch, then row by row


async def test_full_queue_pauses_the_source(spool: Spool) -> None:
    source, sink = FakeSource(), FakeSink()
    sink.gate = asyncio.Event()  # the database stalls
    settings = BridgeSettings(batch_size=2, flush_interval=0.01, queue_size=2, retry=FAST.retry)
    bridge, task = start(source, sink, spool, settings)
    for i in range(10):
        source.publish(f"plant/s/m{i}/energy", {"kwh": i})
    await asyncio.sleep(0.1)

    # 2 rows in the blocked write, 2 in the queue, 1 waiting for a free slot: 5 taken in.
    assert bridge.stats.received == 5
    sink.gate.set()
    await finish(source, task)
    assert len(sink.rows) == 10


async def test_cancelling_run_still_flushes_queued_messages(spool: Spool) -> None:
    source, sink = FakeSource(), FakeSink()
    bridge, task = start(source, sink, spool)
    for i in range(3):
        source.publish(f"plant/s/m{i}/energy", {"kwh": i})
    await eventually(lambda: bridge.stats.received == 3)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(sink.rows) == 3


async def test_source_failure_propagates_after_draining(spool: Spool) -> None:
    class BrokenSource:
        async def messages(self, filters: Sequence[str]) -> AsyncIterator[Message]:
            yield Message("plant/s/m1/energy", b'{"kwh": 1}', 0.0)
            raise ConnectionError("broker gone")

    sink = FakeSink()
    bridge = Bridge(BrokenSource(), sink, make_router(), spool, FAST)
    with pytest.raises(ConnectionError, match="broker gone"):
        await asyncio.wait_for(bridge.run(), 5)
    assert len(sink.rows) == 1
