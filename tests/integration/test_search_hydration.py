"""Verify message search returns hydrated `content`, not just IDs/scores.

Regression coverage for the bug where `foreman_search` returned MessageHit
records with no message body, making memory recall unusable.

Run against a deployed bundle:
    PYTHONPATH=src .venv/bin/python -m pytest tests/integration/test_search_hydration.py -m integration -v
"""

from __future__ import annotations

import datetime
import time

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
    return datetime.datetime.now(datetime.UTC).strftime("%Y%m%d%H%M%S%f")


def test_workspace_search_returns_message_content(client, admin_token):
    headers = {"Authorization": f"Bearer {admin_token}"}
    ws = f"itest-hydrate-{_ts()}"
    peer = "peer-search"
    sess = "conv-search"
    body_text = "I am Mark, a data engineer at Townebank, and I prefer terse responses"

    assert client.post("/workspaces", json={"name": ws}, headers=headers).status_code == 201
    assert (
        client.post(f"/workspaces/{ws}/peers", json={"name": peer}, headers=headers).status_code
        == 201
    )
    assert (
        client.post(
            f"/workspaces/{ws}/sessions",
            json={"name": sess, "peers": {peer: {}}},
            headers=headers,
        ).status_code
        == 201
    )
    r = client.post(
        f"/workspaces/{ws}/sessions/{sess}/messages",
        json={"messages": [{"peer_name": peer, "content": body_text}]},
        headers=headers,
    )
    assert r.status_code == 201, r.text
    public_id = r.json()[0]["public_id"]

    # messages_idx is Direct Access with inline upsert — should be searchable
    # immediately, but allow a short retry in case the index is briefly behind.
    found = None
    for _ in range(5):
        r = client.post(
            f"/workspaces/{ws}/search",
            json={"query": "data engineer Townebank"},
            headers=headers,
        )
        assert r.status_code == 200, r.text
        hits = r.json()
        found = next((h for h in hits if h["public_id"] == public_id), None)
        if found is not None:
            break
        time.sleep(1)

    assert found is not None, "newly created message not returned by search"
    assert "content" in found
    assert found["content"] == body_text
