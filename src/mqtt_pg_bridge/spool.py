"""SQLite-backed spool and dead-letter store.

The spool holds raw messages that could not be delivered because the sink was
unavailable; the bridge replays them once it is back. Dead letters are messages
that can never be delivered (unparseable payload, no matching mapping, rejected
by the sink) and are kept, with the reason, for inspection.

Access is synchronous on purpose: every call is one short transaction against a
local file, which is far cheaper than a network round-trip and fine to run on
the event loop at the batch sizes the bridge works with.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from .ports import Message

_SCHEMA = """
CREATE TABLE IF NOT EXISTS spool (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    topic       TEXT    NOT NULL,
    payload     BLOB    NOT NULL,
    received_at REAL    NOT NULL
);
CREATE TABLE IF NOT EXISTS dead_letter (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    topic       TEXT    NOT NULL,
    payload     BLOB    NOT NULL,
    received_at REAL    NOT NULL,
    reason      TEXT    NOT NULL,
    failed_at   REAL    NOT NULL
);
"""


@dataclass(frozen=True, slots=True)
class DeadLetter:
    id: int
    message: Message
    reason: str
    failed_at: float


class Spool:
    def __init__(self, path: str | Path) -> None:
        self._conn = sqlite3.connect(str(path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Spool:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def push(self, messages: Sequence[Message]) -> None:
        """Append messages to the spool in one transaction."""
        with self._conn:
            self._conn.executemany(
                "INSERT INTO spool (topic, payload, received_at) VALUES (?, ?, ?)",
                [(m.topic, m.payload, m.received_at) for m in messages],
            )

    def peek(self, limit: int) -> list[tuple[int, Message]]:
        """The oldest ``limit`` spooled messages with their ids, left in place."""
        rows = self._conn.execute(
            "SELECT id, topic, payload, received_at FROM spool ORDER BY id LIMIT ?", (limit,)
        ).fetchall()
        return [(row_id, Message(topic, bytes(payload), at)) for row_id, topic, payload, at in rows]

    def ack(self, ids: Sequence[int]) -> None:
        """Remove delivered messages from the spool."""
        with self._conn:
            self._conn.executemany("DELETE FROM spool WHERE id = ?", [(i,) for i in ids])

    def pending(self) -> int:
        return self._count("spool")

    def dead_letter(self, message: Message, reason: str) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO dead_letter (topic, payload, received_at, reason, failed_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (message.topic, message.payload, message.received_at, reason, time.time()),
            )

    def dead_letters(self, limit: int) -> list[DeadLetter]:
        """The most recent ``limit`` dead letters, newest first."""
        rows = self._conn.execute(
            "SELECT id, topic, payload, received_at, reason, failed_at "
            "FROM dead_letter ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            DeadLetter(row_id, Message(topic, bytes(payload), at), reason, failed_at)
            for row_id, topic, payload, at, reason, failed_at in rows
        ]

    def dead_letter_count(self) -> int:
        return self._count("dead_letter")

    def _count(self, table: str) -> int:
        (count,) = self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return int(count)
