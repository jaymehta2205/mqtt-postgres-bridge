from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, ClassVar

import pytest

from mqtt_pg_bridge import Message, TableMapping, TopicRouter, TopicTemplate
from mqtt_pg_bridge.mapping import MappingError, RejectedMessage, decode_payload


def message(topic: str, payload: object, received_at: float = 1_700_000_000.0) -> Message:
    return Message(topic, json.dumps(payload).encode(), received_at)


class TestTopicTemplate:
    def test_placeholders_become_wildcards_and_captures(self) -> None:
        template = TopicTemplate.parse("plant/{site}/{machine}/energy")
        assert template.filter == "plant/+/+/energy"
        assert template.names == ("site", "machine")
        assert template.match("plant/pune/press-01/energy") == {
            "site": "pune",
            "machine": "press-01",
        }

    @pytest.mark.parametrize(
        "topic",
        [
            "plant/pune/energy",
            "plant/pune/press-01/status",
            "plant/pune/press-01/energy/extra",
            "factory/pune/press-01/energy",
        ],
    )
    def test_non_matching_topics(self, topic: str) -> None:
        assert TopicTemplate.parse("plant/{site}/{machine}/energy").match(topic) is None

    def test_plain_wildcards(self) -> None:
        template = TopicTemplate.parse("sensors/+/{room}/#")
        assert template.filter == "sensors/+/+/#"
        assert template.match("sensors/b1/kitchen/temp/raw") == {"room": "kitchen"}
        assert template.match("sensors/b1/kitchen") == {"room": "kitchen"}  # like the broker
        assert template.match("sensors/b1") is None

    def test_hash_alone_matches_everything(self) -> None:
        assert TopicTemplate.parse("#").match("anything/at/all") == {}

    def test_literal_levels_are_not_regex(self) -> None:
        assert TopicTemplate.parse("a.b/{x}").match("aXb/1") is None
        assert TopicTemplate.parse("a.b/{x}").match("a.b/1") == {"x": "1"}

    @pytest.mark.parametrize(
        "template", ["", "a//b", "a/#/b", "a/{x}/{x}", "a/{Bad-Name}", "a/x{y}", "a/b+c"]
    )
    def test_invalid_templates(self, template: str) -> None:
        with pytest.raises(MappingError):
            TopicTemplate.parse(template)


class TestDecodePayload:
    def test_object(self) -> None:
        assert decode_payload(b'{"a": 1, "b": [2]}') == {"a": 1, "b": [2]}

    @pytest.mark.parametrize("payload", [b"", b"not json", b"[1, 2]", b'"str"', b"42", b"\xff\xfd"])
    def test_rejects_anything_but_an_object(self, payload: bytes) -> None:
        with pytest.raises(RejectedMessage):
            decode_payload(payload)


class TestTableMapping:
    template = TopicTemplate.parse("plant/{site}/{machine}/energy")
    captures: ClassVar[dict[str, str]] = {"site": "pune", "machine": "press-01"}

    def test_columns_in_order_with_selected_fields(self) -> None:
        mapping = TableMapping(self.template, "energy_readings", fields=("kwh", "voltage_v"))
        payload = {"voltage_v": 415.2, "kwh": 12.5, "extra": True}
        row = mapping.to_row(message("plant/pune/press-01/energy", payload), self.captures)
        assert row.table == "energy_readings"
        assert row.columns == ("site", "machine", "kwh", "voltage_v", "received_at")
        assert row.values == (
            "pune",
            "press-01",
            12.5,
            415.2,
            datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC),
        )

    def test_missing_selected_field_is_null(self) -> None:
        mapping = TableMapping(self.template, "energy_readings", fields=("kwh", "voltage_v"))
        row = mapping.to_row(message("plant/pune/press-01/energy", {"kwh": 1}), self.captures)
        assert row.values[2:4] == (1, None)

    def test_all_payload_fields_when_unset_and_nested_values_serialised(self) -> None:
        mapping = TableMapping(TopicTemplate.parse("m/{id}"), "status")
        payload = {"state": "run", "alarms": ["E1", "E2"], "meta": {"fw": "1.2"}, "note": None}
        row = mapping.to_row(message("m/7", payload), {"id": "7"})
        assert row.columns == ("id", "state", "alarms", "meta", "note", "received_at")
        assert row.values[1:5] == ("run", '["E1","E2"]', '{"fw":"1.2"}', None)

    def test_timestamp_column_can_be_renamed_or_disabled(self) -> None:
        template = TopicTemplate.parse("m/{id}")
        renamed = TableMapping(template, "t", timestamp_column="ts")
        disabled = TableMapping(template, "t", timestamp_column=None)
        assert renamed.to_row(message("m/1", {"a": 1}), {"id": "1"}).columns == ("id", "a", "ts")
        assert disabled.to_row(message("m/1", {"a": 1}), {"id": "1"}).columns == ("id", "a")

    @pytest.mark.parametrize(
        "payload", [{"id": "clash"}, {"received_at": 1}, {"bad-key": 1}, {"Upper": 1}, {"": 1}]
    )
    def test_rejects_unsafe_or_colliding_payload_keys(self, payload: dict[str, Any]) -> None:
        mapping = TableMapping(TopicTemplate.parse("m/{id}"), "t")
        with pytest.raises(RejectedMessage):
            mapping.to_row(message("m/1", payload), {"id": "1"})

    def test_schema_qualified_table_is_allowed(self) -> None:
        mapping = TableMapping(TopicTemplate.parse("m/{id}"), "telemetry.status")
        assert mapping.table == "telemetry.status"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"table": "Bad Table"},
            {"table": "a.b.c"},
            {"table": "t", "fields": ("id",)},  # duplicates the topic capture
            {"table": "t", "fields": ("drop table",)},
            {"table": "t", "fields": ("a", "a")},
            {"table": "t", "timestamp_column": "id"},
            {"table": "t", "timestamp_column": "Received At"},
        ],
    )
    def test_invalid_definitions(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(MappingError):
            TableMapping(TopicTemplate.parse("m/{id}"), **kwargs)


class TestTopicRouter:
    def test_first_match_wins_and_filters_are_deduplicated(self) -> None:
        energy = TableMapping(
            TopicTemplate.parse("plant/{site}/{machine}/energy"), "energy", fields=("kwh",)
        )
        catch_all = TableMapping(TopicTemplate.parse("plant/#"), "raw")
        shadowed = TableMapping(TopicTemplate.parse("plant/#"), "never_used")
        router = TopicRouter([energy, catch_all, shadowed])

        assert router.filters == ("plant/+/+/energy", "plant/#")
        assert router.route(message("plant/a/b/energy", {"kwh": 1})).table == "energy"
        assert router.route(message("plant/a/b/other", {"x": 1})).table == "raw"

    def test_unmatched_topic_is_rejected(self) -> None:
        router = TopicRouter([TableMapping(TopicTemplate.parse("plant/{site}/energy"), "energy")])
        with pytest.raises(RejectedMessage, match="no mapping matches"):
            router.route(message("other/topic", {}))

    def test_requires_at_least_one_mapping(self) -> None:
        with pytest.raises(MappingError):
            TopicRouter([])
