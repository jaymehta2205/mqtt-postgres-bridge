# mqtt-pg-bridge

Batch MQTT JSON telemetry into PostgreSQL, with a SQLite spool that survives database outages.

## Why

Every IoT project I have worked on ends up with the same small daemon: something subscribes to
a handful of MQTT topics, parses JSON, and inserts rows into PostgreSQL. I have written it more
than once, usually in a hurry, and usually with the same three bugs: one row per INSERT (slow),
an unbounded in-memory queue (falls over when the database is slow), and nothing at all for the
case where PostgreSQL is down for twenty minutes (data gone).

This repository is the version I wanted to have to hand. It is a personal reference
implementation, not a product: the core is small enough to read in one sitting, the broker and
the database are behind two interfaces so the whole pipeline can be tested in-process, and the
failure handling is the part I spent the most time on.

## Features

- Subscribe to any number of topic filters and route each topic to a table with a template such
  as `plant/{site}/{machine}/energy`; captured levels become columns.
- Payload fields become columns, either every top-level key or an explicit allow-list; nested
  objects and arrays are serialised for `json`/`jsonb` columns.
- Batched inserts: one transaction per batch, bounded by size and by age (`flush_interval`).
- Bounded in-memory queue. When it fills the MQTT client stops reading, so backpressure reaches
  the broker instead of your RAM.
- Retry with exponential backoff and jitter on transient database errors.
- SQLite spool: after retries are exhausted, batches are written to a local file and replayed
  when the database comes back, including across a restart of the bridge.
- Dead-letter store with reasons for messages that can never be written: not JSON, no matching
  topic, unsafe column name, or rejected by PostgreSQL (unknown column, type mismatch, constraint).
- Permanent rejections are isolated: one bad row does not take the rest of its batch with it.
- `${ENV}` and `${ENV:-default}` substitution in the config file, so passwords stay out of it.
- Broker and sink are injected interfaces. `asyncpg` and `aiomqtt` are the production adapters;
  the test-suite runs against an in-process fake broker and fake sink plus a real SQLite spool.

## Quick start

Requires Python 3.11 or newer. The `docker-compose.yml` starts Mosquitto and PostgreSQL with the
tables from `docker/init.sql`, which match the mappings in `config.yaml`.

```bash
git clone https://github.com/jaymehta2205/mqtt-postgres-bridge.git
cd mqtt-postgres-bridge
python -m venv .venv && source .venv/bin/activate    # .venv\Scripts\activate on Windows
pip install -e ".[dev]"

docker compose up -d
mqtt-pg-bridge run --config config.yaml
```

Publish something from another terminal:

```bash
mosquitto_pub -t plant/pune/press-01/energy \
  -m '{"kwh": 12.5, "voltage_v": 415.2, "current_a": 18.7, "power_factor": 0.93}'
mosquitto_pub -t plant/pune/press-01/status \
  -m '{"state": "running", "rpm": 1450, "alarms": []}'
```

## Usage example

A session against the compose stack, with PostgreSQL stopped and started again half-way through:

```text
$ mqtt-pg-bridge run --config config.yaml
2026-09-09 12:31:04,118 INFO    mqtt_pg_bridge.mqtt_source: connected to mqtt://localhost:1883, subscribed to plant/+/+/energy, plant/+/+/status
2026-09-09 12:31:40,902 WARNING mqtt_pg_bridge.bridge: dead-lettering message on plant/pune/press-01/status: payload is not valid JSON: Expecting value: line 1 column 1 (char 0)
2026-09-09 12:33:12,377 WARNING mqtt_pg_bridge.bridge: write failed (ConnectionRefusedError: [Errno 111] Connect call failed ('127.0.0.1', 5432)); retrying in 0.41s
2026-09-09 12:33:12,790 WARNING mqtt_pg_bridge.bridge: write failed (ConnectionRefusedError: [Errno 111] Connect call failed ('127.0.0.1', 5432)); retrying in 0.78s
2026-09-09 12:33:13,574 WARNING mqtt_pg_bridge.bridge: sink unavailable (ConnectionRefusedError: [Errno 111] Connect call failed ('127.0.0.1', 5432)); spooling 37 messages until it recovers
2026-09-09 12:33:13,581 INFO    mqtt_pg_bridge.bridge: sink still unavailable (ConnectionRefusedError: [Errno 111] Connect call failed ('127.0.0.1', 5432)); probe 1 failed
2026-09-09 12:33:14,009 INFO    mqtt_pg_bridge.bridge: sink still unavailable (ConnectionRefusedError: [Errno 111] Connect call failed ('127.0.0.1', 5432)); probe 2 failed
2026-09-09 12:33:15,215 INFO    mqtt_pg_bridge.bridge: sink still unavailable (ConnectionRefusedError: [Errno 111] Connect call failed ('127.0.0.1', 5432)); probe 3 failed
2026-09-09 12:33:17,860 INFO    mqtt_pg_bridge.bridge: sink still unavailable (ConnectionRefusedError: [Errno 111] Connect call failed ('127.0.0.1', 5432)); probe 4 failed
2026-09-09 12:33:23,406 INFO    mqtt_pg_bridge.bridge: sink recovered; replaying spooled messages
2026-09-09 12:33:23,441 INFO    mqtt_pg_bridge.bridge: replayed 500 spooled messages, 212 remaining
2026-09-09 12:33:23,468 INFO    mqtt_pg_bridge.bridge: replayed 212 spooled messages, 0 remaining
^C
2026-09-09 12:35:02,733 INFO    mqtt_pg_bridge.bridge: bridge stopped: Stats(received=1873, written=1872, spooled=712, replayed=712, dead_lettered=1)
2026-09-09 12:35:02,734 INFO    mqtt_pg_bridge: interrupted
```

The dead letter is kept in the spool file with its reason and the raw payload:

```text
$ mqtt-pg-bridge inspect --config config.yaml
spool file:   spool.sqlite
pending:      0
dead letters: 1
  [1] 2026-09-09T07:01:40+00:00 plant/pune/press-01/status: payload is not valid JSON: Expecting value: line 1 column 1 (char 0)
      not json
```

And the rows are where you would expect them:

```text
telemetry=# SELECT site, machine, kwh, voltage_v, received_at
telemetry-#   FROM energy_readings ORDER BY received_at DESC LIMIT 3;
 site | machine  | kwh  | voltage_v |          received_at
------+----------+------+-----------+-------------------------------
 pune | press-01 | 12.5 |     415.2 | 2026-09-09 07:01:12.418472+00
 pune | press-02 |  9.8 |     413.9 | 2026-09-09 07:01:12.207115+00
 pune | press-01 | 12.4 |     415.6 | 2026-09-09 07:01:11.902330+00
(3 rows)
```

## Configuration

`config.yaml` is the full sample. The interesting part is the mappings:

```yaml
mappings:
  - topic: plant/{site}/{machine}/energy
    table: energy_readings
    fields: [kwh, voltage_v, current_a, power_factor]

  - topic: plant/{site}/{machine}/status
    table: machine_status
```

- `{name}` captures one topic level into a column called `name`. `+` matches a level without
  capturing it; a trailing `#` matches the rest of the topic. The subscription filter is derived
  from the template (`plant/+/+/energy`), and the first mapping whose template matches wins.
- `fields` selects payload keys; missing keys are written as NULL. Without `fields`, every
  top-level key becomes a column and a key that is not a safe identifier (or collides with a
  topic capture) dead-letters the message rather than reaching SQL.
- Every row also gets a `received_at timestamptz` column. Rename it with `timestamp_column` or
  set it to `null` to drop it.
- Column names and table names are validated against `[a-z_][a-z0-9_]*` and double-quoted in
  the generated `INSERT`, so topic content never reaches SQL as syntax.

The bridge does not create tables. `docker/init.sql` shows the shape they need; use `double
precision`, `integer`, `text`, `boolean` or `jsonb` to match the JSON values you publish.

## Design notes

**Ports and adapters.** `ports.py` defines `MessageSource` (an async iterator of raw messages)
and `RowSink` (`write(rows)` that either succeeds atomically or raises). `mqtt_source.py` and
`pg_sink.py` implement them with `aiomqtt` and `asyncpg`; `tests/support.py` implements them in
memory. The core in `bridge.py` imports neither network library.

**Three cooperating tasks.** `_ingest` moves messages from the source into an
`asyncio.Queue(maxsize=queue_size)`; when the queue is full the `await put()` blocks, the MQTT
client stops reading from its socket, and the broker holds the backlog (the client connects with
a persistent session, so QoS 1 messages are also retained by the broker while the bridge is
disconnected). `_batch_loop` collects up to `batch_size` messages or waits at most
`flush_interval` since the first one, then delivers the batch. `_replay_loop` sleeps until the
spool has content, then drains it.

**Two kinds of failure.** The sink raises `SinkUnavailable` for anything that might pass on a
retry (connection refused, timeout, server closed the connection) and `RowRejected` for anything
that will not (`UndefinedColumnError`, `DataError`, constraint violations). Transient errors are
retried `max_attempts` times with exponential backoff and jitter; if they persist the batch is
pushed to the spool, the sink is marked down, and every later batch goes straight to the spool
while the replay task probes with growing delays (capped at `max_delay`). The first probe that
succeeds flips the sink back to healthy, the spool drains, and live writes resume. A permanent
error on a batch causes the rows to be written one at a time so only the offending ones are
dead-lettered.

**Spool.** One SQLite file in WAL mode with two tables: `spool` (raw topic, payload and receive
time, replayed oldest first) and `dead_letter` (the same plus a reason). Raw messages rather than
mapped rows are stored, so a mapping fix in the config also applies to whatever is still queued.
Calls are synchronous; each is a single small transaction on local disk.

**Shutdown.** `Bridge.run` returns when the source ends or `stop()` is called (the CLI wires
`SIGTERM` to it on POSIX), and treats cancellation of its own task the same way, which is what
`Ctrl+C` produces. In every case the in-flight batch and the queued messages are flushed to the
sink or the spool before it returns.

**Delivery semantics and limits.** Delivery is at-least-once: a batch whose transaction committed
just as the connection dropped can be written twice, and so can the good rows of a batch that
fails part-way through row-by-row isolation. Add a unique index if your schema needs
idempotence; the duplicate insert then becomes a `RowRejected` and lands in the dead letters.
Ordering is preserved on the live path but not across an outage, because replay runs alongside
live writes; rows carry `received_at` for that reason. The spool has no size cap and dead letters
are never rotated, so watch the file if the database is down for a long time. TLS for MQTT and
PostgreSQL is not exposed in the config, and one bridge process per `client_id` is assumed.

## Tests

```bash
pytest -q
ruff check . && ruff format --check .
mypy
```

The tests need no broker and no database. `tests/support.py` provides a `FakeSource` the test
publishes into and a `FakeSink` whose availability and rejection rule the test controls; the
spool is the real SQLite implementation on a temporary file. `tests/test_bridge.py` exercises the
whole pipeline end-to-end: routing to tables, batch size and flush interval, dead-lettering with
reasons, a sink outage followed by spooling, replay and recovery, spool contents surviving a
restart, isolation of permanently rejected rows, backpressure with a full queue, and draining on
cancellation. The other modules test topic templates, payload mapping, the spool, config parsing
and validation, and the CLI.

## Licence

MIT. See `LICENSE`.
