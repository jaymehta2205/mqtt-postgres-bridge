"""mqtt-pg-bridge: batch MQTT JSON telemetry into PostgreSQL, with a SQLite spool."""

from .bridge import Bridge, BridgeSettings, RetryPolicy, Stats
from .mapping import RejectedMessage, TableMapping, TopicRouter, TopicTemplate
from .ports import Message, MessageSource, Row, RowRejected, RowSink, SinkUnavailable
from .spool import DeadLetter, Spool

__version__ = "0.1.0"

__all__ = [
    "Bridge",
    "BridgeSettings",
    "DeadLetter",
    "Message",
    "MessageSource",
    "RejectedMessage",
    "RetryPolicy",
    "Row",
    "RowRejected",
    "RowSink",
    "SinkUnavailable",
    "Spool",
    "Stats",
    "TableMapping",
    "TopicRouter",
    "TopicTemplate",
]
