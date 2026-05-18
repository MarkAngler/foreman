"""Unit tests for the peers router."""

from __future__ import annotations

from unittest.mock import patch

from tests.unit.conftest import (
    FakeResult,
    auth_header,
    peer_row,
    queue_match,
)


def test_create_peer_201(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("FROM workspaces"), FakeResult([{"?column?": 1}])),
        (queue_match("SELECT name", "FROM peers"), FakeResult([])),
        (queue_match("INSERT INTO peers"), FakeResult([peer_row("peer-new")])),
    ]
    resp = client.post(
        "/workspaces/ws-a/peers",
        json={"name": "peer-new"},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["name"] == "peer-new"


def test_create_peer_returns_existing_200(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("FROM workspaces"), FakeResult([{"?column?": 1}])),
        (queue_match("SELECT name", "FROM peers"), FakeResult([peer_row("peer-1")])),
    ]
    resp = client.post(
        "/workspaces/ws-a/peers",
        json={"name": "peer-1"},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 200


def test_create_peer_missing_workspace_404(client, patch_session, admin_token):
    patch_session.queue = [
        (queue_match("FROM workspaces"), FakeResult([])),
    ]
    resp = client.post(
        "/workspaces/ws-missing/peers",
        json={"name": "peer-1"},
        headers=auth_header(admin_token),
    )
    assert resp.status_code == 404


def test_create_peer_403_for_other_workspace(client, patch_session, workspace_token):
    resp = client.post(
        "/workspaces/ws-other/peers",
        json={"name": "peer-1"},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 403


def test_list_peers_paginated(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("FROM peers"), FakeResult([peer_row("a"), peer_row("b")])),
    ]
    resp = client.post(
        "/workspaces/ws-a/peers/list", json={}, headers=auth_header(workspace_token)
    )
    assert resp.status_code == 200
    assert resp.json()["total"] == 2


def test_update_peer_404(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("UPDATE peers"), FakeResult([])),
    ]
    resp = client.put(
        "/workspaces/ws-a/peers/peer-missing",
        json={"metadata": {"k": "v"}},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 404


def test_search_peer_messages(client, patch_session, workspace_token):
    from foreman.lib.vector_search import MessageHit

    async def fake_query(**kwargs):
        assert kwargs["peer_name"] == "peer-1"
        return [MessageHit("m1", "ws-a", "peer-1", "sess-1", "body", 0.7)]

    with patch("foreman.app.routers.peers.vector_search.query_messages", side_effect=fake_query):
        resp = client.post(
            "/workspaces/ws-a/peers/peer-1/search",
            json={"query": "hi"},
            headers=auth_header(workspace_token),
        )
    assert resp.status_code == 200
    assert resp.json()[0]["public_id"] == "m1"
