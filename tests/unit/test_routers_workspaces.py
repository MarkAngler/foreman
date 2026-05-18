"""Unit tests for the workspaces router.

Covers happy path, 403 (wrong workspace scope) and 404 (missing on update).
DB and vector_search are mocked via the conftest.
"""

from __future__ import annotations

from unittest.mock import patch

from tests.unit.conftest import (
    FakeResult,
    auth_header,
    queue_match,
    workspace_row,
)


def test_create_workspace_returns_201_when_inserted(client, patch_session, admin_token):
    patch_session.queue = [
        (queue_match("SELECT name", "FROM workspaces"), FakeResult([])),
        (queue_match("INSERT INTO workspaces"), FakeResult([workspace_row("ws-new")])),
    ]
    resp = client.post(
        "/workspaces",
        json={"name": "ws-new"},
        headers=auth_header(admin_token),
    )
    assert resp.status_code == 201
    assert resp.json()["name"] == "ws-new"


def test_create_workspace_returns_200_when_existing(client, patch_session, admin_token):
    patch_session.queue = [
        (queue_match("SELECT name", "FROM workspaces"), FakeResult([workspace_row("ws-a")])),
    ]
    resp = client.post(
        "/workspaces",
        json={"name": "ws-a"},
        headers=auth_header(admin_token),
    )
    assert resp.status_code == 200


def test_create_workspace_rejects_wrong_workspace_scope(client, patch_session, workspace_token):
    # workspace_token is scoped to 'ws-a'; creating 'ws-b' must 403.
    resp = client.post(
        "/workspaces",
        json={"name": "ws-b"},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 403


def test_list_workspaces_requires_admin(client, patch_session, workspace_token):
    resp = client.post("/workspaces/list", json={}, headers=auth_header(workspace_token))
    assert resp.status_code == 403


def test_list_workspaces_admin_ok(client, patch_session, admin_token):
    patch_session.queue = [
        (
            queue_match("SELECT name", "FROM workspaces"),
            FakeResult([workspace_row("ws-a"), workspace_row("ws-b")]),
        ),
    ]
    resp = client.post("/workspaces/list", json={}, headers=auth_header(admin_token))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert {w["name"] for w in body["items"]} == {"ws-a", "ws-b"}


def test_update_workspace_404_when_missing(client, patch_session, admin_token):
    patch_session.queue = [
        (queue_match("UPDATE workspaces"), FakeResult([])),
    ]
    resp = client.put(
        "/workspaces/ws-missing",
        json={"metadata": {"k": "v"}},
        headers=auth_header(admin_token),
    )
    assert resp.status_code == 404


def test_update_workspace_happy_path(client, patch_session, admin_token):
    patch_session.queue = [
        (queue_match("UPDATE workspaces"), FakeResult([workspace_row("ws-a", metadata={"k": "v"})])),
    ]
    resp = client.put(
        "/workspaces/ws-a",
        json={"metadata": {"k": "v"}},
        headers=auth_header(admin_token),
    )
    assert resp.status_code == 200
    assert resp.json()["metadata"] == {"k": "v"}


def test_search_workspace_messages_uses_vector_search(client, patch_session, workspace_token):
    from foreman.lib.vector_search import MessageHit

    async def fake_query(**kwargs):
        assert kwargs["workspace_name"] == "ws-a"
        return [
            MessageHit(public_id="m1", workspace_name="ws-a", peer_name="p", session_name="s", score=0.9)
        ]

    with patch("foreman.app.routers.workspaces.vector_search.query_messages", side_effect=fake_query):
        resp = client.post(
            "/workspaces/ws-a/search",
            json={"query": "hello"},
            headers=auth_header(workspace_token),
        )
    assert resp.status_code == 200
    assert resp.json() == [
        {"public_id": "m1", "workspace_name": "ws-a", "peer_name": "p", "session_name": "s", "score": 0.9}
    ]


def test_search_workspace_403_for_other_workspace(client, patch_session, workspace_token):
    resp = client.post(
        "/workspaces/ws-other/search",
        json={"query": "hello"},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 403


def test_create_workspace_rejects_invalid_name(client, patch_session, admin_token):
    resp = client.post(
        "/workspaces",
        json={"name": "has spaces"},
        headers=auth_header(admin_token),
    )
    assert resp.status_code == 422
