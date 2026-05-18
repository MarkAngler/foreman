"""Unit tests for the conclusions router."""

from __future__ import annotations

from unittest.mock import patch

from tests.unit.conftest import (
    FakeResult,
    auth_header,
    conclusion_row,
    queue_match,
)


def test_create_conclusions_201(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("INSERT INTO documents"), FakeResult([conclusion_row("d1")])),
        (queue_match("INSERT INTO documents"), FakeResult([conclusion_row("d2")])),
    ]
    resp = client.post(
        "/workspaces/ws-a/conclusions",
        json={
            "conclusions": [
                {"observer": "peer-1", "observed": "peer-2", "content": "fact A"},
                {"observer": "peer-1", "observed": "peer-2", "content": "fact B"},
            ]
        },
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 201, resp.text
    assert {c["id"] for c in resp.json()} == {"d1", "d2"}


def test_create_conclusions_403_for_other_workspace(client, patch_session, workspace_token):
    resp = client.post(
        "/workspaces/ws-other/conclusions",
        json={"conclusions": [{"observer": "p1", "observed": "p2", "content": "x"}]},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 403


def test_query_conclusions_uses_vector_search_then_hydrates(
    client, patch_session, workspace_token
):
    from foreman.lib.vector_search import DocumentHit

    async def fake_query(**kwargs):
        return [
            DocumentHit("d1", "ws-a", "peer-1", "peer-2", "explicit", "x", 0.9),
            DocumentHit("d2", "ws-a", "peer-1", "peer-2", "explicit", "y", 0.8),
        ]

    patch_session.queue = [
        (queue_match("FROM documents", "ANY(:ids)"), FakeResult([conclusion_row("d1"), conclusion_row("d2")])),
    ]
    with patch(
        "foreman.app.routers.conclusions.vector_search.query_documents",
        side_effect=fake_query,
    ):
        resp = client.post(
            "/workspaces/ws-a/conclusions/query",
            json={"query": "tell me", "observer": "peer-1", "observed": "peer-2"},
            headers=auth_header(workspace_token),
        )
    assert resp.status_code == 200, resp.text
    assert [c["id"] for c in resp.json()] == ["d1", "d2"]


def test_query_conclusions_empty_when_no_hits(client, patch_session, workspace_token):
    async def fake_query(**kwargs):
        return []

    with patch(
        "foreman.app.routers.conclusions.vector_search.query_documents",
        side_effect=fake_query,
    ):
        resp = client.post(
            "/workspaces/ws-a/conclusions/query",
            json={"query": "anything"},
            headers=auth_header(workspace_token),
        )
    assert resp.status_code == 200
    assert resp.json() == []


def test_delete_conclusion_404(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("UPDATE documents", "deleted_at"), FakeResult([])),
    ]
    resp = client.delete(
        "/workspaces/ws-a/conclusions/d-missing",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 404


def test_delete_conclusion_204(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("UPDATE documents", "deleted_at"), FakeResult([{"id": "d1"}])),
    ]
    resp = client.delete(
        "/workspaces/ws-a/conclusions/d1",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 204


def test_list_conclusions_promotes_typed_filters(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("FROM documents"), FakeResult([conclusion_row("d1")])),
    ]
    resp = client.post(
        "/workspaces/ws-a/conclusions/list",
        json={"filters": {"observer": "peer-1", "level": "explicit", "tag": "x"}},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 200
    # The promoted clause goes to bind params; we check it was issued
    # by inspecting the executed SQL.
    sql, params = patch_session.executed[-1]
    assert "observer = :observer" in sql
    assert "level = :level" in sql
    assert params["observer"] == "peer-1"
    assert params["level"] == "explicit"
