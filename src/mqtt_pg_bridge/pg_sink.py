"""asyncpg-backed :class:`~mqtt_pg_bridge.ports.RowSink`."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import asyncpg

from .ports import Row, RowRejected, SinkUnavailable

# Failures that no amount of retrying will fix: bad table or column, wrong type,
# constraint violation, insufficient privilege.
_PERMANENT = (
    asyncpg.SyntaxOrAccessError,
    asyncpg.DataError,
    asyncpg.IntegrityConstraintViolationError,
)
# Everything else from the driver or the socket is treated as transient.
_TRANSIENT = (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError)


def quote_identifier(name: str) -> str:
    """Double-quote a possibly schema-qualified identifier."""
    return ".".join('"' + part.replace('"', '""') + '"' for part in name.split("."))


def insert_statement(table: str, columns: Sequence[str]) -> str:
    column_list = ", ".join(quote_identifier(column) for column in columns)
    placeholders = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    return f"INSERT INTO {quote_identifier(table)} ({column_list}) VALUES ({placeholders})"


class PostgresSink:
    """Writes each batch in one transaction, one ``executemany`` per (table, column set).

    The connection pool is created lazily on the first write, so the bridge can
    start (and spool) while the database is still down.
    """

    def __init__(self, dsn: str, *, pool_size: int = 4, timeout: float = 10.0) -> None:
        self._dsn = dsn
        self._pool_size = pool_size
        self._timeout = timeout
        self._pool: asyncpg.Pool | None = None

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def write(self, rows: Sequence[Row]) -> None:
        grouped: dict[tuple[str, tuple[str, ...]], list[tuple[Any, ...]]] = {}
        for row in rows:
            grouped.setdefault((row.table, row.columns), []).append(row.values)
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn, conn.transaction():
                for (table, columns), values in grouped.items():
                    await conn.executemany(insert_statement(table, columns), values)
        except _PERMANENT as exc:
            raise RowRejected(f"{type(exc).__name__}: {exc}") from exc
        except _TRANSIENT as exc:
            raise SinkUnavailable(f"{type(exc).__name__}: {exc}") from exc

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self._dsn,
                min_size=1,
                max_size=self._pool_size,
                timeout=self._timeout,
                command_timeout=self._timeout,
            )
        return self._pool
