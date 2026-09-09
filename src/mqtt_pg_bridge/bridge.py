"""The bridge core: batching, retry with backoff, spooling and replay.

Data flow::

    source ──► bounded queue ──► batcher ──► sink
                                   │           ▲
                       (sink down) ▼           │ replay (sink back)
                                 SQLite spool ─┘

Three tasks cooperate: ``_ingest`` moves messages from the source into a bounded
queue (blocking, and therefore pausing the source, when it is full); ``_batch_loop``
groups them by size or age and delivers each batch; ``_replay_loop`` drains the
spool whenever it has content, probing a down sink with exponential backoff.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Sequence
from dataclasses import dataclass, field

from .mapping import RejectedMessage, TopicRouter
from .ports import Message, MessageSource, Row, RowRejected, RowSink, SinkUnavailable
from .spool import Spool

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Exponential backoff with jitter, capped at ``max_delay`` seconds."""

    max_attempts: int = 3
    """Write attempts on the live path before a batch is spooled."""
    initial_delay: float = 0.5
    max_delay: float = 30.0
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("retry.max_attempts must be at least 1")
        if not 0 <= self.initial_delay <= self.max_delay:
            raise ValueError("retry delays must satisfy 0 <= initial_delay <= max_delay")
        if self.multiplier < 1:
            raise ValueError("retry.multiplier must be at least 1")

    def delay(self, attempt: int) -> float:
        """Seconds to wait before retry number ``attempt`` (1-based)."""
        capped = min(self.initial_delay * self.multiplier ** (attempt - 1), self.max_delay)
        return random.uniform(capped / 2, capped)


@dataclass(frozen=True, slots=True)
class BridgeSettings:
    batch_size: int = 500
    flush_interval: float = 2.0
    """Seconds a partial batch may wait for more messages before it is flushed."""
    queue_size: int = 10_000
    """Messages held in memory before the source is paused (backpressure)."""
    retry: RetryPolicy = field(default_factory=RetryPolicy)

    def __post_init__(self) -> None:
        if self.batch_size < 1 or self.queue_size < 1:
            raise ValueError("batch_size and queue_size must be at least 1")
        if self.flush_interval <= 0:
            raise ValueError("flush_interval must be positive")


@dataclass(slots=True)
class Stats:
    received: int = 0
    written: int = 0
    """Rows written to the sink, whether live or replayed from the spool."""
    spooled: int = 0
    replayed: int = 0
    dead_lettered: int = 0


class Bridge:
    def __init__(
        self,
        source: MessageSource,
        sink: RowSink,
        router: TopicRouter,
        spool: Spool,
        settings: BridgeSettings | None = None,
    ) -> None:
        self._source = source
        self._sink = sink
        self._router = router
        self._spool = spool
        self._settings = settings or BridgeSettings()
        self._queue: asyncio.Queue[Message | None] = asyncio.Queue(self._settings.queue_size)
        self._stop = asyncio.Event()
        self._spool_changed = asyncio.Event()
        self._sink_healthy = True
        self.stats = Stats()

    @property
    def sink_healthy(self) -> bool:
        """False while writes go to the spool because the sink is down."""
        return self._sink_healthy

    def stop(self) -> None:
        """Ask :meth:`run` to stop consuming and drain what is already queued."""
        self._stop.set()

    async def run(self) -> None:
        """Run until the source ends or :meth:`stop` is called, then drain the queue.

        Cancelling the task that runs this coroutine has the same effect as
        :meth:`stop`: queued messages are still flushed (to the sink, or to the
        spool if the sink is down) before the cancellation propagates.
        """
        ingest = asyncio.create_task(self._ingest(), name="bridge-ingest")
        batcher = asyncio.create_task(self._batch_loop(), name="bridge-batcher")
        replayer = asyncio.create_task(self._replay_loop(), name="bridge-replayer")
        stop = asyncio.create_task(self._stop.wait(), name="bridge-stop")
        try:
            done, _ = await asyncio.wait(
                {ingest, batcher, replayer, stop}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in (ingest, batcher, replayer):
                if task in done:
                    task.result()  # the source ended, or a loop failed: surface it
        finally:
            await self._shutdown(ingest, batcher, replayer, stop)

    async def _shutdown(
        self,
        ingest: asyncio.Task[None],
        batcher: asyncio.Task[None],
        replayer: asyncio.Task[None],
        stop: asyncio.Task[bool],
    ) -> None:
        """Stop ingesting, flush whatever is queued, then stop the replayer."""
        stop.cancel()
        ingest.cancel()
        await asyncio.gather(ingest, stop, return_exceptions=True)
        if not batcher.done():
            await self._queue.put(None)
            await batcher
        replayer.cancel()
        await asyncio.gather(replayer, return_exceptions=True)
        log.info("bridge stopped: %s", self.stats)

    async def _ingest(self) -> None:
        async for message in self._source.messages(self._router.filters):
            self.stats.received += 1
            await self._queue.put(message)

    async def _batch_loop(self) -> None:
        while (batch := await self._next_batch()) is not None:
            await self._deliver(batch)

    async def _next_batch(self) -> list[Message] | None:
        """Wait for one message, then gather more until the batch is full or
        ``flush_interval`` has passed. Returns None once the stop sentinel is seen."""
        first = await self._queue.get()
        if first is None:
            return None
        batch = [first]
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._settings.flush_interval
        while len(batch) < self._settings.batch_size:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                async with asyncio.timeout(remaining):
                    message = await self._queue.get()
            except TimeoutError:
                break
            if message is None:
                self._queue.put_nowait(None)  # leave the sentinel for the next call
                break
            batch.append(message)
        return batch

    async def _deliver(self, batch: Sequence[Message]) -> None:
        if not self._sink_healthy:
            self._to_spool(batch)
            return
        pairs = self._prepare(batch)
        try:
            await self._write_with_retry(pairs)
        except SinkUnavailable as exc:
            log.warning(
                "sink unavailable (%s); spooling %d messages until it recovers", exc, len(pairs)
            )
            self._sink_healthy = False
            self._to_spool([message for message, _ in pairs])

    def _prepare(self, batch: Sequence[Message]) -> list[tuple[Message, Row]]:
        """Map messages to rows, dead-lettering the ones that cannot be mapped."""
        pairs: list[tuple[Message, Row]] = []
        for message in batch:
            try:
                pairs.append((message, self._router.route(message)))
            except RejectedMessage as exc:
                self._dead_letter(message, str(exc))
        return pairs

    async def _write_with_retry(self, pairs: Sequence[tuple[Message, Row]]) -> None:
        policy = self._settings.retry
        for attempt in range(1, policy.max_attempts + 1):
            try:
                await self._write(pairs)
                return
            except SinkUnavailable as exc:
                if attempt == policy.max_attempts:
                    raise
                delay = policy.delay(attempt)
                log.warning("write failed (%s); retrying in %.2fs", exc, delay)
                await asyncio.sleep(delay)

    async def _write(self, pairs: Sequence[tuple[Message, Row]]) -> None:
        """Write rows; on a permanent rejection retry one by one to isolate the culprits."""
        if not pairs:
            return
        try:
            await self._sink.write([row for _, row in pairs])
        except RowRejected as exc:
            if len(pairs) == 1:
                self._dead_letter(pairs[0][0], f"rejected by sink: {exc}")
                return
            log.warning(
                "batch of %d rows rejected (%s); writing rows one at a time", len(pairs), exc
            )
            for pair in pairs:
                await self._write([pair])
            return
        self.stats.written += len(pairs)

    async def _replay_loop(self) -> None:
        """Drain the spool whenever it has content; probe a down sink with backoff."""
        policy = self._settings.retry
        failed_probes = 0
        while True:
            self._spool_changed.clear()
            if self._spool.pending() == 0:
                await self._spool_changed.wait()
                continue
            if failed_probes:
                await asyncio.sleep(policy.delay(failed_probes))
            entries = self._spool.peek(self._settings.batch_size)
            pairs = self._prepare([message for _, message in entries])
            try:
                await self._write(pairs)
            except SinkUnavailable as exc:
                self._sink_healthy = False
                failed_probes += 1
                log.info("sink still unavailable (%s); probe %d failed", exc, failed_probes)
                continue
            if not self._sink_healthy:
                log.info("sink recovered; replaying spooled messages")
            self._sink_healthy = True
            failed_probes = 0
            self._spool.ack([spool_id for spool_id, _ in entries])
            self.stats.replayed += len(entries)
            log.info(
                "replayed %d spooled messages, %d remaining", len(entries), self._spool.pending()
            )

    def _to_spool(self, batch: Sequence[Message]) -> None:
        self._spool.push(batch)
        self.stats.spooled += len(batch)
        self._spool_changed.set()

    def _dead_letter(self, message: Message, reason: str) -> None:
        log.warning("dead-lettering message on %s: %s", message.topic, reason)
        self._spool.dead_letter(message, reason)
        self.stats.dead_lettered += 1
