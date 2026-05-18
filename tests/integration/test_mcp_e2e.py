"""End-to-end MCP → in-process FastAPI exercise (FOR-006).

Each foreman MCP tool is invoked through the real ForemanClient + httpx, but
the underlying transport is ASGITransport(app=foreman_fastapi_app). This is
the same code path that runs in production — only the network hop and the
DB/VS/LLM dependencies are swapped (FakeSession, stubbed vector_search,
stubbed dialectic agent). Regression coverage for FOR-001 (FastAPI Depends()
introspection survives @trace) and FOR-002/FOR-003 (dual-header auth and
non-silent redirects in ForemanClient).

This file is *not* marked @pytest.mark.integration — it runs in the default
fast loop so the FOR-001-class bugs cannot ship again unnoticed.
"""

from __future__ import annotations

import datetime
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport

from foreman.app.routers import all_routers
from foreman.app.routers.chat import router as chat_router
from foreman.lib import auth
from foreman.mcp.auth import Credentials
from foreman.mcp.client import ForemanClient

NOW = datetime.datetime(2026, 5, 17, 22, 0, 0, tzinfo=datetime.UTC)


@dataclass
class FakeResult:
    rows: list[dict] = field(default_factory=list)

    def mappings(self):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def one(self):
        if not self.rows:
            raise LookupError("no rows")
        return self.rows[0]

    def all(self):
        return self.rows

    def scalar_one(self):
        if not self.rows:
            raise LookupError("no rows")
        return list(self.rows[0].values())[0]

    def scalar_one_or_none(self):
        if not self.rows:
            return None
        return list(self.rows[0].values())[0]


@dataclass
class FakeSession:
    workspaces: dict[str, dict] = field(default_factory=dict)
    peers: dict[tuple[str, str], dict] = field(default_factory=dict)
    sessions_: dict[tuple[str, str], dict] = field(default_factory=dict)
    messages: list[dict] = field(default_factory=list)
    summaries: list[dict] = field(default_factory=list)
    webhooks: list[dict] = field(default_factory=list)
    committed: int = 0
    rolled_back: int = 0

    async def execute(self, statement, params=None):  # noqa: C901
        sql = str(statement)
        p = params or {}

        if "FROM workspaces WHERE name" in sql:
            n = p.get("n")
            return FakeResult([self.workspaces[n]] if n in self.workspaces else [])
        if "INSERT INTO workspaces" in sql and "RETURNING" in sql:
            row = {
                "name": p["n"],
                "metadata": json.loads(p.get("m", "{}")),
                "configuration": json.loads(p.get("c", "{}")),
                "created_at": NOW,
            }
            self.workspaces[p["n"]] = row
            return FakeResult([row])
        if "FROM workspaces" in sql and "WHERE name" not in sql:
            return FakeResult(list(self.workspaces.values()))

        if "FROM peers WHERE name" in sql:
            key = (p.get("w"), p.get("n"))
            return FakeResult([self.peers[key]] if key in self.peers else [])
        if "INSERT INTO peers" in sql and "RETURNING" in sql:
            row = {
                "name": p["n"],
                "workspace_name": p["w"],
                "metadata": json.loads(p.get("m", "{}")),
                "configuration": json.loads(p.get("c", "{}")),
                "created_at": NOW,
            }
            self.peers[(p["w"], p["n"])] = row
            return FakeResult([row])
        if "FROM peers" in sql and "workspace_name" in sql:
            ws = p.get("w")
            return FakeResult([v for k, v in self.peers.items() if k[0] == ws])

        if "SELECT 1 FROM sessions WHERE name" in sql:
            key = (p.get("w"), p.get("n"))
            return FakeResult([{"1": 1}] if key in self.sessions_ else [])
        if "FROM sessions" in sql and ("WHERE name" in sql or ":n" in sql):
            key = (p.get("w"), p.get("n"))
            return FakeResult([self.sessions_[key]] if key in self.sessions_ else [])
        if "INSERT INTO sessions" in sql and "RETURNING" in sql:
            row = {
                "name": p["n"],
                "workspace_name": p["w"],
                "metadata": json.loads(p.get("m", "{}")),
                "configuration": json.loads(p.get("c", "{}")),
                "is_active": True,
                "created_at": NOW,
                "internal_metadata": {},
            }
            self.sessions_[(p["w"], p["n"])] = row
            return FakeResult([row])
        if "FROM sessions" in sql and "workspace_name" in sql:
            ws = p.get("w")
            return FakeResult([v for k, v in self.sessions_.items() if k[0] == ws])

        if "INSERT INTO session_peers" in sql or "FROM session_peers" in sql:
            return FakeResult([])

        if "INSERT INTO messages" in sql and "RETURNING" in sql:
            mid = p.get("public_id") or f"msg-{len(self.messages) + 1:03d}"
            md = p.get("metadata")
            if isinstance(md, str):
                try:
                    md = json.loads(md)
                except Exception:
                    md = {}
            row = {
                "public_id": mid,
                "session_name": p.get("session_name", ""),
                "peer_name": p.get("peer_name", ""),
                "workspace_name": p.get("workspace_name", ""),
                "content": p.get("content", ""),
                "token_count": p.get("token_count", 1),
                "metadata": md or {},
                "created_at": NOW,
            }
            self.messages.append(row)
            return FakeResult([row])
        if "FROM messages" in sql and "session_name" in sql:
            s = p.get("s") or p.get("session_name") or p.get("n")
            return FakeResult([m for m in self.messages if m["session_name"] == s])
        if "FROM messages" in sql:
            return FakeResult(self.messages)

        if "session_summaries" in sql or "summaries" in sql:
            return FakeResult(self.summaries)

        if "INSERT INTO webhook_endpoints" in sql:
            row = {
                "id": f"wh-{len(self.webhooks) + 1}",
                "workspace_name": p.get("w", ""),
                "url": p.get("url", ""),
                "event_types": [],
                "metadata": {},
                "created_at": NOW,
            }
            self.webhooks.append(row)
            return FakeResult([row])
        if "FROM webhook_endpoints" in sql:
            return FakeResult(self.webhooks)

        return FakeResult([])

    async def commit(self):
        self.committed += 1

    async def rollback(self):
        self.rolled_back += 1


stub_vs_messages: list[dict] = []


async def stub_embed(text: str):
    return [0.1] * 1024


async def stub_upsert_message(**kwargs):
    stub_vs_messages.append(kwargs)


async def stub_query_messages(**kwargs):
    from foreman.lib.vector_search import MessageHit

    ws = kwargs.get("workspace_name", "")
    peer = kwargs.get("peer_name") or "peer-user"
    sess = kwargs.get("session_name") or "sess-e2e"
    return [
        MessageHit(
            public_id="msg-001",
            workspace_name=ws,
            peer_name=peer,
            session_name=sess,
            content="hello there",
            score=0.93,
        ),
        MessageHit(
            public_id="msg-003",
            workspace_name=ws,
            peer_name=peer,
            session_name=sess,
            content="goodbye",
            score=0.88,
        ),
    ]


async def stub_query_documents(**kwargs):
    return []


async def stub_run_agent_stream(req):
    from foreman.app.dialectic import AgentEvent

    yield AgentEvent(kind="token", data={"delta": "The launch code is "})
    yield AgentEvent(kind="token", data={"delta": "FOXGLOVE-7."})
    yield AgentEvent(kind="done", data={"total_tokens": 42})


def _build_app() -> FastAPI:
    app = FastAPI()
    for r in all_routers():
        app.include_router(r)
    app.include_router(chat_router)
    return app


def _make_creds(token: str, *, databricks_headers: dict[str, str] | None = None) -> Credentials:
    return Credentials(
        base_url="http://test",
        _provider=lambda: token,
        _databricks_headers=(lambda: databricks_headers) if databricks_headers else (lambda: {}),
    )


@pytest.fixture
def fake_db():
    stub_vs_messages.clear()
    fs = FakeSession()

    @asynccontextmanager
    async def _session_ctx():
        yield fs

    patches = [
        patch("foreman.lib.lakebase.session", _session_ctx),
        patch("foreman.app.routers.workspaces.session", _session_ctx),
        patch("foreman.app.routers.peers.session", _session_ctx),
        patch("foreman.app.routers.sessions.session", _session_ctx),
        patch("foreman.app.routers.messages.session", _session_ctx),
        patch("foreman.app.routers.conclusions.session", _session_ctx),
        patch("foreman.app.routers.webhooks.session", _session_ctx),
        patch("foreman.lib.vector_search.embed", stub_embed),
        patch("foreman.lib.vector_search.upsert_message", stub_upsert_message),
        patch("foreman.lib.vector_search.query_messages", stub_query_messages),
        patch("foreman.lib.vector_search.query_documents", stub_query_documents),
        patch("foreman.app.routers.workspaces.vector_search.query_messages", stub_query_messages),
        patch("foreman.app.routers.peers.vector_search.query_messages", stub_query_messages),
        patch("foreman.app.routers.sessions.vector_search.query_messages", stub_query_messages),
        patch("foreman.app.routers.messages.vector_search.embed", stub_embed),
        patch("foreman.app.routers.messages.vector_search.upsert_message", stub_upsert_message),
        patch("foreman.app.routers.chat.run_agent_stream", stub_run_agent_stream),
    ]
    for p in patches:
        p.start()
    auth.signing_key.cache_clear()
    sk_patch = patch.object(auth, "signing_key", return_value="e2e-local-secret-key")
    sk_patch.start()
    try:
        yield fs
    finally:
        sk_patch.stop()
        for p in patches:
            p.stop()


@pytest.fixture
def app(fake_db) -> FastAPI:
    return _build_app()


@pytest.fixture
def admin_token() -> str:
    return auth.issue(admin=True)


@pytest.fixture
def client(app, admin_token) -> ForemanClient:
    transport = ASGITransport(app=app)
    return ForemanClient(credentials=_make_creds(admin_token), transport=transport)


WS = "ws-e2e"
P_A = "peer-assistant"
P_U = "peer-user"
SESSION = "sess-e2e"


async def _seed_full_lifecycle(client: ForemanClient) -> None:
    await client.create_workspace(name=WS, metadata={"e2e": True})
    await client.create_peer(workspace_name=WS, name=P_A)
    await client.create_peer(workspace_name=WS, name=P_U)
    await client.create_session(
        workspace_name=WS,
        name=SESSION,
        peers={
            P_A: {"observe_others": True, "observe_me": True},
            P_U: {"observe_others": True, "observe_me": True},
        },
    )
    await client.send_messages(
        workspace_name=WS,
        session_name=SESSION,
        messages=[
            {"peer_name": P_U, "content": "hi"},
            {"peer_name": P_A, "content": "hello"},
            {"peer_name": P_U, "content": "remember the launch code is FOXGLOVE-7"},
            {"peer_name": P_A, "content": "noted"},
            {"peer_name": P_U, "content": "favorite color is octarine"},
        ],
    )


async def test_create_workspace_idempotent(client):
    a = await client.create_workspace(name=WS, metadata={"e2e": True})
    b = await client.create_workspace(name=WS, metadata={"e2e": True})
    assert a["name"] == b["name"] == WS


async def test_list_workspaces_returns_seeded_workspace(client):
    await client.create_workspace(name=WS)
    res = await client.list_workspaces()
    assert any(i.get("name") == WS for i in res.get("items", []))


async def test_create_and_list_peers(client):
    await client.create_workspace(name=WS)
    a = await client.create_peer(workspace_name=WS, name=P_A)
    u = await client.create_peer(workspace_name=WS, name=P_U)
    assert a["name"] == P_A
    assert u["name"] == P_U
    res = await client.list_peers(workspace_name=WS)
    assert len(res.get("items", [])) == 2


async def test_create_and_list_sessions(client):
    await client.create_workspace(name=WS)
    await client.create_peer(workspace_name=WS, name=P_A)
    await client.create_peer(workspace_name=WS, name=P_U)
    s = await client.create_session(workspace_name=WS, name=SESSION)
    assert s["name"] == SESSION
    res = await client.list_sessions(workspace_name=WS)
    assert any(i.get("name") == SESSION for i in res.get("items", []))


async def test_send_messages_and_vector_upsert(client):
    await _seed_full_lifecycle(client)
    assert len(stub_vs_messages) == 5


async def test_search_workspace_returns_hits(client):
    await _seed_full_lifecycle(client)
    hits = await client.search_workspace(workspace_name=WS, query="FOXGLOVE")
    assert isinstance(hits, list) and len(hits) >= 1


async def test_search_peer_scoped(client):
    await _seed_full_lifecycle(client)
    hits = await client.search_workspace(workspace_name=WS, query="hi", peer_name=P_U, limit=5)
    assert isinstance(hits, list)


async def test_search_session_scoped(client):
    await _seed_full_lifecycle(client)
    hits = await client.search_workspace(
        workspace_name=WS, query="hi", session_name=SESSION, limit=5
    )
    assert isinstance(hits, list)


async def test_session_context_with_summary(client):
    await _seed_full_lifecycle(client)
    res = await client.session_context(
        workspace_name=WS, session_name=SESSION, tokens=4000, include_summary=True
    )
    assert isinstance(res, dict) and res.get("name") == SESSION


async def test_session_context_without_summary(client):
    await _seed_full_lifecycle(client)
    res = await client.session_context(
        workspace_name=WS, session_name=SESSION, tokens=4000, include_summary=False
    )
    assert isinstance(res, dict) and res.get("summary") is None


@pytest.mark.parametrize("reasoning_level", ["minimal", "low", "high"])
async def test_chat_sse(client, reasoning_level):
    await _seed_full_lifecycle(client)
    res = await client.chat(
        workspace_name=WS,
        peer_name=P_A,
        query="What launch code?",
        session_name=SESSION,
        reasoning_level=reasoning_level,
        include_session_history=True,
    )
    assert "FOXGLOVE-7" in res.get("text", "")
    assert len(res.get("events", [])) >= 3


async def test_tampered_jwt_rejected(app, admin_token):
    transport = ASGITransport(app=app)
    bad_client = ForemanClient(credentials=_make_creds(admin_token + "x"), transport=transport)
    with pytest.raises(RuntimeError, match="401"):
        await bad_client.list_workspaces()


async def test_workspace_token_denied_admin_list(app):
    transport = ASGITransport(app=app)
    ws_token = auth.issue(workspace=WS)
    ws_client = ForemanClient(credentials=_make_creds(ws_token), transport=transport)
    with pytest.raises(RuntimeError, match="403"):
        await ws_client.list_workspaces()


async def test_workspace_token_allowed_on_own_peers(app, client):
    await client.create_workspace(name=WS)
    transport = ASGITransport(app=app)
    ws_token = auth.issue(workspace=WS)
    ws_client = ForemanClient(credentials=_make_creds(ws_token), transport=transport)
    res = await ws_client.list_peers(workspace_name=WS)
    assert isinstance(res, dict)


async def test_workspace_token_denied_other_workspace(app):
    transport = ASGITransport(app=app)
    ws_token = auth.issue(workspace=WS)
    ws_client = ForemanClient(credentials=_make_creds(ws_token), transport=transport)
    with pytest.raises(RuntimeError, match="403"):
        await ws_client.list_peers(workspace_name="other-ws")


async def test_session_context_missing_session_returns_404(client):
    # FOR-007 regression: get_session_context must 404 on missing session,
    # not return 200 with empty messages.
    await client.create_workspace(name=WS)
    with pytest.raises(RuntimeError, match="404"):
        await client.session_context(workspace_name=WS, session_name="never-existed")


async def test_dual_header_x_foreman_token_path(app, admin_token):
    transport = ASGITransport(app=app)
    creds = _make_creds(
        admin_token,
        databricks_headers={"Authorization": "Bearer fake-databricks-oauth"},
    )
    client = ForemanClient(credentials=creds, transport=transport)
    # Should succeed even though Authorization is the (irrelevant) Databricks
    # bearer — server reads X-Foreman-Token for scope.
    res = await client.list_workspaces()
    assert isinstance(res, dict)


async def test_search_limit_param_accepted(client):
    await _seed_full_lifecycle(client)
    r1 = await client.search_workspace(workspace_name=WS, query="x", limit=1)
    r50 = await client.search_workspace(workspace_name=WS, query="x", limit=50)
    assert isinstance(r1, list) and isinstance(r50, list)
