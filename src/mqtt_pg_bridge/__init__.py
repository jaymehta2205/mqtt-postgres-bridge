"""mqtt-pg-bridge: batch MQTT JSON telemetry into PostgreSQL, with a SQLite spool."""

from .mapping import RejectedMessage, TableMapping, TopicRouter, TopicTemplate
from .ports import Message, MessageSource, Row, RowRejected, RowSink, SinkUnavailable
from .spool import DeadLetter, Spool

__version__ = "0.1.0"

__all__ = [
    "DeadLetter",
    "Message",
    "MessageSource",
    "RejectedMessage",
    "Row",
    "RowRejected",
    "RowSink",
    "SinkUnavailable",
    "Spool",
    "TableMapping",
    "TopicRouter",
    "TopicTemplate",
]
