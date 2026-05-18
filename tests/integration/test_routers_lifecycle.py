"""End-to-end router lifecycle integration test.

Run against a deployed bundle:
    PYTHONPATH=src .venv/Scripts/python.exe -m pytest tests/integration/ -m integration

Requires:
    - lakebase reachable (Settings().lakebase_*)
    - vector_search messages_idx and documents_idx live
    - signing_key resolvable (Databricks secret)
"""

from __future__ import annotations

import datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from foreman.app.routers import all_routers
from foreman.lib import auth
from foreman.lib.lakebase import dispose_engine, init_engine

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def app() -> FastAPI:
    a = FastAPI()
    for r in all_routers():
        a.include_router(r)

    @a.on_event("startup")
    async def _startup() -> None:
        await init_engine()

    @a.on_event("shutdown")
    async def _shutdown() -> None:
        await dispose_engine()

    return a


@pytest.fixture(scope="module")
def client(app) -> TestClient:
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="module")
def admin_token() -> str:
    return auth.issue(admin=True)


def _ts() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S%f")


def test_full_workspace_peer_session_message_lifecycle(client, admin_token):
    headers = {"Authorization": f"Bearer {admin_token}"}
    ws_name = f"itest-ws-{_ts()}"
    peer_name = "peer-alice"
    sess_name = "conv-1"

    r = client.post("/workspaces", json={"name": ws_name}, headers=headers)
    assert r.status_code == 201, r.text

    r = client.post(
        f"/workspaces/{ws_name}/peers", json={"name": peer_name}, headers=headers
    )
    assert r.status_code == 201, r.text

    r = client.post(
        f"/workspaces/{ws_name}/sessions",
        json={
            "name": sess_name,
            "peers": {peer_name: {"observe_others": True, "observe_me": True}},
        },
        headers=headers,
    )
    assert r.status_code == 201, r.text

    r = client.post(
        f"/workspaces/{ws_name}/sessions/{sess_name}/messages",
        json={
            "messages": [
                {"peer_name": peer_name, "content": "Hello world from integration test"}
            ]
        },
        headers=headers,
    )
    assert r.status_code == 201, r.text
    msg = r.json()[0]

    r = client.get(
        f"/workspaces/{ws_name}/sessions/{sess_name}/messages/{msg['public_id']}",
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json()["content"] == "Hello world from integration test"

    r = client.get(
        f"/workspaces/{ws_name}/sessions/{sess_name}/context", headers=headers
    )
    assert r.status_code == 200
    assert any(m["public_id"] == msg["public_id"] for m in r.json()["messages"])


def test_jwt_scope_enforcement(client, admin_token):
    headers = {"Authorization": f"Bearer {admin_token}"}
    ws_a = f"itest-scope-a-{_ts()}"
    ws_b = f"itest-scope-b-{_ts()}"
    client.post("/workspaces", json={"name": ws_a}, headers=headers)
    client.post("/workspaces", json={"name": ws_b}, headers=headers)

    r = client.post("/keys", json={"workspace_name": ws_a}, headers=headers)
    assert r.status_code == 201
    ws_a_token = r.json()["token"]

    r = client.post(
        f"/workspaces/{ws_b}/peers",
        json={"name": "intruder"},
        headers={"Authorization": f"Bearer {ws_a_token}"},
    )
    assert r.status_code == 403
