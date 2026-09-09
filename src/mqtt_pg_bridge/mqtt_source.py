"""aiomqtt-backed :class:`~mqtt_pg_bridge.ports.MessageSource` with reconnection."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Sequence

import aiomqtt

from .ports import Message

log = logging.getLogger(__name__)


class MqttSource:
    """Subscribes with a persistent session so the broker queues QoS 1/2 messages
    published while the bridge is disconnected, and reconnects after any error."""

    def __init__(
        self,
        host: str,
        port: int = 1883,
        *,
        username: str | None = None,
        password: str | None = None,
        client_id: str = "mqtt-pg-bridge",
        qos: int = 1,
        reconnect_delay: float = 5.0,
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._client_id = client_id
        self._qos = qos
        self._reconnect_delay = reconnect_delay

    async def messages(self, filters: Sequence[str]) -> AsyncIterator[Message]:
        while True:
            try:
                async with aiomqtt.Client(
                    self._host,
                    self._port,
                    username=self._username,
                    password=self._password,
                    identifier=self._client_id,
                    clean_session=False,
                ) as client:
                    for topic_filter in filters:
                        await client.subscribe(topic_filter, qos=self._qos)
                    log.info(
                        "connected to mqtt://%s:%d, subscribed to %s",
                        self._host,
                        self._port,
                        ", ".join(filters),
                    )
                    async for message in client.messages:
                        yield Message(message.topic.value, bytes(message.payload), time.time())
            except aiomqtt.MqttError as exc:
                log.warning(
                    "mqtt connection lost (%s); reconnecting in %.0fs", exc, self._reconnect_delay
                )
                await asyncio.sleep(self._reconnect_delay)
