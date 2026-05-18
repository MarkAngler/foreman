"""Unit tests for the keys router."""

from __future__ import annotations

import datetime

from tests.unit.conftest import auth_header

from foreman.lib import auth


def test_issue_workspace_key_admin_only_201(client, patch_session, admin_token):
    resp = client.post(
        "/keys",
        json={"workspace_name": "ws-a"},
        headers=auth_header(admin_token),
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["token"]
    # The token must validate to scope.workspace == "ws-a"
    scope = auth.decode(body["token"])
    assert scope.workspace == "ws-a"
    assert not scope.admin
    expires_at = datetime.datetime.fromisoformat(body["expires_at"])
    assert expires_at > datetime.datetime.now(datetime.timezone.utc)


def test_issue_peer_key_201(client, patch_session, admin_token):
    resp = client.post(
        "/keys",
        json={"workspace_name": "ws-a", "peer_name": "peer-1"},
        headers=auth_header(admin_token),
    )
    assert resp.status_code == 201
    scope = auth.decode(resp.json()["token"])
    assert scope.workspace == "ws-a"
    assert scope.peer == "peer-1"


def test_issue_session_key_201(client, patch_session, admin_token):
    resp = client.post(
        "/keys",
        json={"workspace_name": "ws-a", "session_name": "sess-1"},
        headers=auth_header(admin_token),
    )
    assert resp.status_code == 201
    scope = auth.decode(resp.json()["token"])
    assert scope.session == "sess-1"


def test_issue_admin_key_201(client, patch_session, admin_token):
    resp = client.post(
        "/keys",
        json={"admin": True},
        headers=auth_header(admin_token),
    )
    assert resp.status_code == 201
    scope = auth.decode(resp.json()["token"])
    assert scope.admin


def test_issue_403_for_non_admin(client, patch_session, workspace_token):
    resp = client.post(
        "/keys",
        json={"workspace_name": "ws-a"},
        headers=auth_header(workspace_token),
    )
    assert resp.status_code == 403


def test_issue_400_for_no_scope(client, patch_session, admin_token):
    resp = client.post(
        "/keys",
        json={},
        headers=auth_header(admin_token),
    )
    assert resp.status_code == 400


def test_issue_400_for_peer_and_session_combined(client, patch_session, admin_token):
    resp = client.post(
        "/keys",
        json={"workspace_name": "ws-a", "peer_name": "p1", "session_name": "s1"},
        headers=auth_header(admin_token),
    )
    assert resp.status_code == 400
