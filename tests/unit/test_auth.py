"""Unit tests for the JWT auth helper. No Databricks dependency — uses a fake key."""

from __future__ import annotations

from typing import Annotated
from unittest.mock import patch

import pytest
from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient

from foreman.lib import auth


@pytest.fixture(autouse=True)
def _stub_signing_key():
    auth.signing_key.cache_clear()
    with patch.object(auth, "signing_key", return_value="unit-test-key-do-not-use-in-prod"):
        yield


def test_workspace_scope_allows_matching_workspace():
    token = auth.issue(workspace="ws-a")
    scope = auth.decode(token)
    auth.require_workspace("ws-a", scope)


def test_workspace_scope_rejects_other_workspace():
    token = auth.issue(workspace="ws-a")
    scope = auth.decode(token)
    with pytest.raises(HTTPException) as exc:
        auth.require_workspace("ws-b", scope)
    assert exc.value.status_code == 403


def test_admin_scope_allows_anything():
    token = auth.issue(admin=True)
    scope = auth.decode(token)
    auth.require_workspace("any-workspace", scope)
    auth.require_peer("any-workspace", "any-peer", scope)
    auth.require_session("any-workspace", "any-session", scope)


def test_peer_scope_allows_matching_peer_only():
    token = auth.issue(workspace="ws-a", peer="peer-1")
    scope = auth.decode(token)
    auth.require_peer("ws-a", "peer-1", scope)
    with pytest.raises(HTTPException):
        auth.require_peer("ws-a", "peer-2", scope)


def test_session_scope_allows_matching_session_only():
    token = auth.issue(workspace="ws-a", session="sess-1")
    scope = auth.decode(token)
    auth.require_session("ws-a", "sess-1", scope)
    with pytest.raises(HTTPException):
        auth.require_session("ws-a", "sess-2", scope)


def test_workspace_token_implicit_access_to_all_peers_and_sessions():
    token = auth.issue(workspace="ws-a")
    scope = auth.decode(token)
    auth.require_peer("ws-a", "any-peer", scope)
    auth.require_session("ws-a", "any-session", scope)


def test_issue_requires_admin_or_workspace():
    with pytest.raises(ValueError):
        auth.issue()


def test_peer_scope_requires_workspace():
    with pytest.raises(ValueError):
        auth.issue(peer="peer-1")


def test_session_scope_requires_workspace():
    with pytest.raises(ValueError):
        auth.issue(session="sess-1")


def _probe_app() -> FastAPI:
    app = FastAPI()

    @app.get("/whoami")
    async def whoami(scope: Annotated[auth.Scope, Depends(auth.current_scope)]) -> dict:
        return {"admin": scope.admin, "workspace": scope.workspace}

    return app


def test_current_scope_reads_x_foreman_token_header():
    token = auth.issue(workspace="ws-a")
    client = TestClient(_probe_app())
    resp = client.get("/whoami", headers={"X-Foreman-Token": token})
    assert resp.status_code == 200
    assert resp.json() == {"admin": False, "workspace": "ws-a"}


def test_current_scope_falls_back_to_authorization_header():
    token = auth.issue(admin=True)
    client = TestClient(_probe_app())
    resp = client.get("/whoami", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    assert resp.json() == {"admin": True, "workspace": None}


def test_current_scope_prefers_x_foreman_token_over_authorization():
    foreman_token = auth.issue(workspace="ws-from-custom")
    client = TestClient(_probe_app())
    resp = client.get(
        "/whoami",
        headers={
            "Authorization": "Bearer some-other-bearer-the-proxy-uses",
            "X-Foreman-Token": foreman_token,
        },
    )
    assert resp.status_code == 200
    assert resp.json()["workspace"] == "ws-from-custom"


def test_current_scope_missing_token_returns_401():
    client = TestClient(_probe_app())
    resp = client.get("/whoami")
    assert resp.status_code == 401
