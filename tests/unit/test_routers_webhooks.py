"""Unit tests for the webhooks router."""

from __future__ import annotations

from tests.unit.conftest import (
    FakeResult,
    auth_header,
    queue_match,
    webhook_row,
)


def test_register_endpoint_201(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("FROM workspaces"), FakeResult([{"?column?": 1}])),
        (queue_match("INSERT INTO webhook_endpoints"), FakeResult([webhook_row("wh-1")])),
    ]
    resp = client.post(
        "/workspaces/ws-a/webhooks",
        json={"url": "https://example.com/hook"},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["url"] == "https://example.com/hook"


def test_register_endpoint_rejects_loopback_url(client, patch_session, workspace_token):
    resp = client.post(
        "/workspaces/ws-a/webhooks",
        json={"url": "http://127.0.0.1/hook"},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 422


def test_register_endpoint_403_for_other_workspace(client, patch_session, workspace_token):
    resp = client.post(
        "/workspaces/ws-other/webhooks",
        json={"url": "https://example.com/hook"},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 403


def test_list_endpoints_paginated(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("FROM webhook_endpoints"), FakeResult([webhook_row("a"), webhook_row("b")])),
    ]
    resp = client.get(
        "/workspaces/ws-a/webhooks",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 200
    assert resp.json()["total"] == 2


def test_delete_endpoint_404(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("DELETE FROM webhook_endpoints"), FakeResult([])),
    ]
    resp = client.delete(
        "/workspaces/ws-a/webhooks/wh-missing",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 404


def test_delete_endpoint_204(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("DELETE FROM webhook_endpoints"), FakeResult([{"id": "wh-1"}])),
    ]
    resp = client.delete(
        "/workspaces/ws-a/webhooks/wh-1",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 204


def test_test_event_enqueues_queue_item(client, patch_session, workspace_token):
    patch_session.queue = [
        (queue_match("INSERT INTO queue_items"), FakeResult([])),
    ]
    resp = client.get(
        "/workspaces/ws-a/webhooks/test",
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "queued"
    assert body["work_unit_key"].startswith("webhook:test:ws-a:")
