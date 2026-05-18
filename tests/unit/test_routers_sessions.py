"""Unit tests for the sessions router."""

from __future__ import annotations

from tests.unit.conftest import (
    FakeResult,
    auth_header,
    queue_match,
    session_row,
)


def test_create_session_201(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("FROM workspaces"), FakeResult([{"?column?": 1}])),
        (queue_match("FROM sessions WHERE name"), FakeResult([])),
        (queue_match("INSERT INTO sessions"), FakeResult([session_row("sess-new")])),
    ]
    resp = client.post(
        "/workspaces/ws-a/sessions",
        json={"name": "sess-new"},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 201, resp.text


def test_delete_session_returns_409_when_active_peers_present(
    client, patch_session, workspace_token
):
    patch_session.queue = [
        (queue_match("FROM sessions WHERE name"), FakeResult([session_row()])),
        (queue_match("COUNT(*)", "session_peers"), FakeResult([{"c": 2}])),
    ]
    resp = client.delete(
        "/workspaces/ws-a/sessions/sess-1",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 409
    assert "active peer" in resp.json()["detail"]


def test_delete_session_204_when_empty(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("FROM sessions WHERE name"), FakeResult([session_row()])),
        (queue_match("COUNT(*)", "session_peers"), FakeResult([{"c": 0}])),
    ]
    resp = client.delete(
        "/workspaces/ws-a/sessions/sess-1",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 204


def test_delete_session_404_when_missing(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("FROM sessions WHERE name"), FakeResult([])),
    ]
    resp = client.delete(
        "/workspaces/ws-a/sessions/sess-missing",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 404


def test_delete_session_403_for_other_workspace(client, patch_session, workspace_token):
    resp = client.delete(
        "/workspaces/ws-other/sessions/sess-1",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 403


def test_session_context_returns_messages_oldest_first(client, patch_session, workspace_token):
    from tests.unit.conftest import message_row

    rows = [message_row(public_id=f"m{i}", token_count=10) for i in range(3)]
    # Stored newest-first by id DESC, returned oldest-first.
    patch_session.queue = [
        (
            queue_match("FROM sessions", "internal_metadata"),
            FakeResult([{"internal_metadata": {}}]),
        ),
        (queue_match("SELECT 1 FROM sessions"), FakeResult([{"?column?": 1}])),
        (queue_match("FROM messages", "ORDER BY id DESC"), FakeResult(rows)),
    ]
    resp = client.get(
        "/workspaces/ws-a/sessions/sess-1/context",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [m["public_id"] for m in body["messages"]] == ["m2", "m1", "m0"]


def test_clone_session_201(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("FROM sessions WHERE name"), FakeResult([session_row()])),
        (queue_match("INSERT INTO sessions"), FakeResult([session_row("sess-clone")])),
        (queue_match("INSERT INTO messages"), FakeResult([])),
        (queue_match("INSERT INTO session_peers"), FakeResult([])),
    ]
    resp = client.post(
        "/workspaces/ws-a/sessions/sess-1/clone",
        json={"target_name": "sess-clone"},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 201
    assert resp.json()["name"] == "sess-clone"


def test_add_session_peers_upserts(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("FROM sessions WHERE name", "workspace_name"), FakeResult([{"?column?": 1}])),
        (queue_match("INSERT INTO peers"), FakeResult([])),
        (queue_match("INSERT INTO session_peers"), FakeResult([])),
        (
            queue_match("FROM session_peers", "ORDER BY joined_at"),
            FakeResult(
                [
                    {
                        "session_name": "sess-1",
                        "peer_name": "peer-1",
                        "workspace_name": "ws-a",
                        "configuration": {"observe_others": True, "observe_me": True},
                        "joined_at": "2026-05-15T12:00:00+00:00",
                        "left_at": None,
                    }
                ]
            ),
        ),
    ]
    resp = client.post(
        "/workspaces/ws-a/sessions/sess-1/peers",
        json={"peers": {"peer-1": {"observe_others": True, "observe_me": True}}},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()[0]["peer_name"] == "peer-1"
