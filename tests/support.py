"""In-process fakes and helpers shared by the tests. No broker, no database."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable, Sequence
from typing import Any

from mqtt_pg_bridge import (
    BridgeSettings,
    Message,
    RetryPolicy,
    Row,
    RowRejected,
    SinkUnavailable,
    TableMapping,
    TopicRouter,
    TopicTemplate,
)

# Small batches and short timers keep the bridge tests fast without changing the logic.
FAST = BridgeSettings(
    batch_size=4,
    flush_interval=0.05,
    queue_size=100,
    retry=RetryPolicy(max_attempts=2, initial_delay=0.01, max_delay=0.02),
)


def make_router() -> TopicRouter:
    return TopicRouter(
        [
            TableMapping(
                TopicTemplate.parse("plant/{site}/{machine}/energy"),
                "energy_readings",
                fields=("kwh", "voltage_v"),
            ),
            TableMapping(TopicTemplate.parse("plant/{site}/{machine}/status"), "machine_status"),
        ]
    )


class FakeSource:
    """A MessageSource the test publishes into; ``close`` ends the iterator."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[Message | None] = asyncio.Queue()
        self.filters: tuple[str, ...] = ()

    def publish(
        self,
        topic: str,
        payload: bytes | str | dict[str, Any],
        received_at: float | None = None,
    ) -> Message:
        if isinstance(payload, dict):
            payload = json.dumps(payload).encode()
        elif isinstance(payload, str):
            payload = payload.encode()
        message = Message(topic, payload, time.time() if received_at is None else received_at)
        self._queue.put_nowait(message)
        return message

    def close(self) -> None:
        self._queue.put_nowait(None)

    async def messages(self, filters: Sequence[str]) -> AsyncIterator[Message]:
        self.filters = tuple(filters)
        while (message := await self._queue.get()) is not None:
            yield message


class FakeSink:
    """An in-memory RowSink whose availability and rejection rule the test controls."""

    def __init__(self) -> None:
        self.rows: list[Row] = []
        self.writes: list[list[Row]] = []
        self.available = True
        self.reject_when: Callable[[Row], str | None] = lambda _row: None
        self.gate: asyncio.Event | None = None
        """When set, every write blocks until the event is set (a stalled database)."""

    async def write(self, rows: Sequence[Row]) -> None:
        if self.gate is not None:
            await self.gate.wait()
        self.writes.append(list(rows))
        if not self.available:
            raise SinkUnavailable("fake sink is offline")
        for row in rows:
            reason = self.reject_when(row)
            if reason is not None:
                raise RowRejected(reason)
        self.rows.extend(rows)


async def eventually(condition: Callable[[], bool], timeout: float = 3.0) -> None:
    """Poll ``condition`` until it holds or ``timeout`` seconds pass."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(0.005)
