from __future__ import annotations

from pathlib import Path

from mqtt_pg_bridge import Message, Spool


def msg(i: int) -> Message:
    return Message(f"t/{i}", f'{{"i": {i}}}'.encode(), 1_700_000_000.0 + i)


def test_push_peek_ack_preserves_order(tmp_path: Path) -> None:
    with Spool(tmp_path / "s.sqlite") as spool:
        spool.push([msg(i) for i in range(5)])
        assert spool.pending() == 5

        entries = spool.peek(3)
        assert [m for _, m in entries] == [msg(0), msg(1), msg(2)]
        assert spool.pending() == 5  # peek does not consume

        spool.ack([spool_id for spool_id, _ in entries])
        assert spool.pending() == 2
        assert [m.topic for _, m in spool.peek(10)] == ["t/3", "t/4"]


def test_contents_survive_reopening_the_file(tmp_path: Path) -> None:
    path = tmp_path / "s.sqlite"
    with Spool(path) as spool:
        spool.push([msg(1)])
        spool.dead_letter(msg(2), "no mapping")

    with Spool(path) as spool:
        assert (spool.pending(), spool.dead_letter_count()) == (1, 1)
        assert spool.peek(1)[0][1] == msg(1)


def test_dead_letters_are_listed_newest_first_with_reason(tmp_path: Path) -> None:
    with Spool(tmp_path / "s.sqlite") as spool:
        spool.dead_letter(msg(1), "first")
        spool.dead_letter(msg(2), "second")

        letters = spool.dead_letters(10)
        assert [d.reason for d in letters] == ["second", "first"]
        assert letters[0].message == msg(2)
        assert letters[0].failed_at > letters[0].message.received_at
        assert [d.reason for d in spool.dead_letters(1)] == ["second"]


def test_empty_push_and_ack_are_harmless(tmp_path: Path) -> None:
    with Spool(tmp_path / "s.sqlite") as spool:
        spool.push([])
        spool.ack([])
        assert spool.pending() == 0
        assert spool.peek(5) == []
