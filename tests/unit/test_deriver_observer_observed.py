"""Unit tests for observer/observed pair derivation."""

from __future__ import annotations

from datetime import datetime, timezone

from foreman.jobs.deriver.observer_observed import (
    DeriverBatch,
    MessageRow,
    build_batches,
)


def _msg(id_: int, peer: str, session: str = "s1", workspace: str = "ws") -> MessageRow:
    return MessageRow(
        id=id_,
        public_id=f"pub-{id_}",
        workspace_name=workspace,
        session_name=session,
        peer_name=peer,
        content=f"hello from {peer} #{id_}",
        created_at=datetime(2026, 1, 1, 12, 0, id_, tzinfo=timezone.utc),
    )


def test_message_row_from_row_coerces_types():
    row = MessageRow.from_row(
        {
            "id": "42",  # JDBC sometimes hands back numeric strings
            "public_id": "pub-42",
            "workspace_name": "ws",
            "session_name": "s1",
            "peer_name": "alice",
            "content": "hello",
            "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        }
    )
    assert row.id == 42
    assert row.peer_name == "alice"


def test_single_peer_session_yields_self_observation_only():
    msgs = [_msg(1, "alice"), _msg(2, "alice")]
    batches = build_batches(msgs, session_peers={("ws", "s1"): ["alice"]})

    assert len(batches) == 1
    b = batches[0]
    assert b.observed == "alice"
    assert b.observers == ["alice"]
    assert [m.id for m in b.messages] == [1, 2]


def test_two_peer_session_creates_observers_for_each_peer():
    msgs = [_msg(1, "alice"), _msg(2, "bob"), _msg(3, "alice")]
    batches = build_batches(
        msgs, session_peers={("ws", "s1"): ["alice", "bob"]}
    )

    by_observed = {b.observed: b for b in batches}
    assert set(by_observed) == {"alice", "bob"}

    # Observed peer always appears first; the other session peer follows.
    assert by_observed["alice"].observers == ["alice", "bob"]
    assert by_observed["bob"].observers == ["bob", "alice"]
    assert [m.id for m in by_observed["alice"].messages] == [1, 3]
    assert [m.id for m in by_observed["bob"].messages] == [2]


def test_messages_in_different_sessions_create_separate_batches():
    msgs = [
        _msg(1, "alice", session="s1"),
        _msg(2, "alice", session="s2"),
    ]
    batches = build_batches(
        msgs,
        session_peers={
            ("ws", "s1"): ["alice"],
            ("ws", "s2"): ["alice", "bob"],
        },
    )
    by_session = {b.session_name: b for b in batches}
    assert by_session["s1"].observers == ["alice"]
    assert by_session["s2"].observers == ["alice", "bob"]


def test_unknown_session_falls_back_to_self_observation():
    msgs = [_msg(1, "alice", session="orphan")]
    batches = build_batches(msgs, session_peers={})

    assert batches[0].observers == ["alice"]


def test_messages_are_sorted_by_id_within_a_batch():
    msgs = [_msg(3, "alice"), _msg(1, "alice"), _msg(2, "alice")]
    batches = build_batches(msgs, session_peers={("ws", "s1"): ["alice"]})

    assert [m.id for m in batches[0].messages] == [1, 2, 3]


def test_no_duplicate_observer_when_observed_already_in_session_peers():
    msgs = [_msg(1, "alice")]
    batches = build_batches(
        msgs, session_peers={("ws", "s1"): ["bob", "alice", "carol"]}
    )

    # alice should appear exactly once even though she's already in session_peers.
    assert batches[0].observers.count("alice") == 1
    assert set(batches[0].observers) == {"alice", "bob", "carol"}
    assert batches[0].observers[0] == "alice"  # observed is hoisted to the front


def test_deriver_batch_dataclass_default_messages_is_independent():
    a = DeriverBatch(workspace_name="w", session_name="s", observed="x", observers=["x"])
    b = DeriverBatch(workspace_name="w", session_name="s", observed="y", observers=["y"])
    a.messages.append(_msg(1, "x"))
    assert b.messages == []
