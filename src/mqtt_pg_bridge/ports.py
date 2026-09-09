"""Interfaces between the bridge core and the outside world.

The bridge only ever talks to a :class:`MessageSource` and a :class:`RowSink`.
Production wires in the aiomqtt and asyncpg adapters; the test-suite wires in
in-process fakes. Nothing in the core imports a network library.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class Message:
    """A raw message exactly as it arrived from the broker."""

    topic: str
    payload: bytes
    received_at: float
    """Unix timestamp (seconds) at which the bridge received the message."""


@dataclass(frozen=True, slots=True)
class Row:
    """One row destined for one table; ``columns`` and ``values`` are parallel."""

    table: str
    columns: tuple[str, ...]
    values: tuple[Any, ...]


class SinkUnavailable(Exception):
    """The sink cannot accept writes right now (connection refused, timeout, ...).

    Transient: the bridge retries, then spools the batch and probes the sink
    with backoff until it recovers.
    """


class RowRejected(Exception):
    """The sink refused the rows for a reason a retry will not fix
    (unknown table or column, type mismatch, constraint violation).

    Permanent: the bridge isolates the offending rows and dead-letters them.
    """


class MessageSource(Protocol):
    def messages(self, filters: Sequence[str]) -> AsyncIterator[Message]:
        """Subscribe to ``filters`` and yield messages until the source is closed.

        Implementations own reconnection; the iterator ends only when the source
        is shut down deliberately.
        """
        ...


class RowSink(Protocol):
    async def write(self, rows: Sequence[Row]) -> None:
        """Write all of ``rows`` atomically, or none of them.

        Raises:
            SinkUnavailable: on transient failures.
            RowRejected: when the rows can never be written as they are.
        """
        ...
