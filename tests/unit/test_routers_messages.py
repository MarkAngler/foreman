"""Unit tests for the messages router."""

from __future__ import annotations

from unittest.mock import patch

from tests.unit.conftest import (
    FakeResult,
    auth_header,
    message_row,
    queue_match,
)


def test_create_messages_201_and_indexes(client, patch_session, workspace_token):
    inserted = [message_row("m1", content="hello"), message_row("m2", content="world")]
    patch_session.queue = [
        (queue_match("INSERT INTO messages"), FakeResult([inserted[0]])),
        (queue_match("INSERT INTO messages"), FakeResult([inserted[1]])),
    ]

    captured: list[dict] = []

    async def fake_embed(text: str):
        return [0.1] * 4

    async def fake_upsert(**kwargs):
        captured.append(kwargs)

    with patch("foreman.app.routers.messages.vector_search.embed", side_effect=fake_embed), patch(
        "foreman.app.routers.messages.vector_search.upsert_message", side_effect=fake_upsert
    ):
        resp = client.post(
            "/workspaces/ws-a/sessions/sess-1/messages",
            json={
                "messages": [
                    {"peer_name": "peer-1", "content": "hello"},
                    {"peer_name": "peer-1", "content": "world"},
                ]
            },
            headers=auth_header(workspace_token),
        )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert len(body) == 2
    # Inline indexing must happen before return.
    assert {c["public_id"] for c in captured} == {"m1", "m2"}


def test_create_messages_403_for_other_workspace(client, patch_session, workspace_token):
    resp = client.post(
        "/workspaces/ws-other/sessions/sess-1/messages",
        json={"messages": [{"peer_name": "p", "content": "hi"}]},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 403


def test_get_message_404(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("SELECT", "FROM messages", "WHERE"), FakeResult([])),
    ]
    resp = client.get(
        "/workspaces/ws-a/sessions/sess-1/messages/m-missing",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 404


def test_get_message_happy(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("SELECT", "FROM messages", "WHERE"), FakeResult([message_row("m-1")])),
    ]
    resp = client.get(
        "/workspaces/ws-a/sessions/sess-1/messages/m-1",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 200
    assert resp.json()["public_id"] == "m-1"


def test_update_message_404(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("UPDATE messages"), FakeResult([])),
    ]
    resp = client.put(
        "/workspaces/ws-a/sessions/sess-1/messages/m-missing",
        json={"metadata": {"k": "v"}},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 404


def test_list_messages_paginated(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("FROM messages"), FakeResult([message_row(f"m{i}") for i in range(3)])),
    ]
    resp = client.post(
        "/workspaces/ws-a/sessions/sess-1/messages/list",
        json={},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 200
    assert resp.json()["total"] == 3


def test_upload_messages_chunks_and_indexes(client, patch_session, workspace_token):
    text = "x" * 16_001  # forces 3 chunks at 8000 each
    inserted = [message_row(f"m{i}", content=text[i * 8000 : (i + 1) * 8000]) for i in range(3)]
    patch_session.queue = [
        (queue_match("INSERT INTO messages"), FakeResult([inserted[0]])),
        (queue_match("INSERT INTO messages"), FakeResult([inserted[1]])),
        (queue_match("INSERT INTO messages"), FakeResult([inserted[2]])),
    ]

    upserts: list[dict] = []

    async def fake_embed(text: str):
        return [0.0] * 4

    async def fake_upsert(**kwargs):
        upserts.append(kwargs)

    with patch("foreman.app.routers.messages.vector_search.embed", side_effect=fake_embed), patch(
        "foreman.app.routers.messages.vector_search.upsert_message", side_effect=fake_upsert
    ):
        resp = client.post(
            "/workspaces/ws-a/sessions/sess-1/messages/upload",
            data={"peer_name": "peer-1"},
            files={"file": ("doc.txt", text, "text/plain")},
            headers=auth_header(workspace_token),
        )

    assert resp.status_code == 201, resp.text
    assert len(resp.json()) == 3
    assert len(upserts) == 3
