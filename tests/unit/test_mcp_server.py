"""Unit tests for the foreman MCP server's tool surface.

Mounts the FastMCP server in-process and exercises each tool via
ForemanClient + an httpx.MockTransport, so no Databricks calls happen.
"""

from __future__ import annotations

import json

import httpx
import pytest

from foreman.mcp import server as mcp_server
from foreman.mcp.auth import Credentials
from foreman.mcp.client import ForemanClient

EXPECTED_TOOLS = {
    "foreman_chat",
    "foreman_send_messages",
    "foreman_search",
    "foreman_session_context",
    "foreman_capture_turn",
    "foreman_list_workspaces",
    "foreman_create_workspace",
    "foreman_list_sessions",
    "foreman_create_session",
    "foreman_list_peers",
    "foreman_create_peer",
}


async def test_all_tools_registered():
    tools = await mcp_server.mcp.list_tools()
    names = {t.name for t in tools}
    assert names == EXPECTED_TOOLS


async def test_tool_inputs_have_schema():
    tools = await mcp_server.mcp.list_tools()
    for t in tools:
        assert t.inputSchema is not None
        assert t.inputSchema.get("type") == "object"


def _stub_creds(transport: httpx.MockTransport) -> Credentials:
    return Credentials(base_url="http://stub", _provider=lambda: "stub-token")


def _client_with(handler) -> ForemanClient:
    transport = httpx.MockTransport(handler)
    return ForemanClient(credentials=_stub_creds(transport), transport=transport)


async def test_list_workspaces_passes_filters(monkeypatch):
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["url"] = req.url.path
        seen["body"] = json.loads(req.content) if req.content else None
        seen["auth"] = req.headers.get("Authorization")
        return httpx.Response(200, json={"items": [], "total": 0})

    monkeypatch.setattr(mcp_server, "_client", _client_with(handler))
    out = await mcp_server.foreman_list_workspaces(filters={"team": "ai"})

    assert out == {"items": [], "total": 0}
    assert seen["url"] == "/workspaces/list"
    assert seen["body"] == {"filters": {"team": "ai"}}
    assert seen["auth"] == "Bearer stub-token"


async def test_create_workspace_posts_name(monkeypatch):
    captured = {}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(req.content)
        return httpx.Response(
            201,
            json={
                "name": "acme",
                "metadata": {},
                "configuration": {},
                "created_at": "2026-01-01T00:00:00Z",
            },
        )

    monkeypatch.setattr(mcp_server, "_client", _client_with(handler))
    out = await mcp_server.foreman_create_workspace(name="acme")
    assert out["name"] == "acme"
    assert captured["body"]["name"] == "acme"


async def test_default_workspace_env_fallback(monkeypatch):
    monkeypatch.setenv("FOREMAN_DEFAULT_WORKSPACE", "default-ws")

    def handler(req: httpx.Request) -> httpx.Response:
        assert "/workspaces/default-ws/sessions/list" in req.url.path
        return httpx.Response(200, json={"items": []})

    monkeypatch.setattr(mcp_server, "_client", _client_with(handler))
    out = await mcp_server.foreman_list_sessions()
    assert out == {"items": []}


async def test_missing_workspace_raises(monkeypatch):
    monkeypatch.delenv("FOREMAN_DEFAULT_WORKSPACE", raising=False)
    # No client call should happen — _workspace() raises before any HTTP.
    monkeypatch.setattr(
        mcp_server,
        "_client",
        _client_with(lambda r: httpx.Response(500, json={})),
    )
    with pytest.raises(ValueError, match="workspace_name is required"):
        await mcp_server.foreman_list_sessions()


async def test_send_messages_batches(monkeypatch):
    captured = {}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(req.content)
        return httpx.Response(
            201,
            json=[
                {
                    "public_id": "m1",
                    "session_name": "s",
                    "peer_name": "p",
                    "workspace_name": "w",
                    "content": "hi",
                    "token_count": 1,
                    "metadata": {},
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ],
        )

    monkeypatch.setattr(mcp_server, "_client", _client_with(handler))
    out = await mcp_server.foreman_send_messages(
        session_name="s",
        workspace_name="w",
        messages=[{"peer_name": "p", "content": "hi"}],
    )
    assert len(out) == 1
    assert captured["body"] == {"messages": [{"peer_name": "p", "content": "hi"}]}


async def test_search_returns_hits(monkeypatch):
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path == "/workspaces/w/search"
        body = json.loads(req.content)
        assert body["query"] == "memory"
        assert body["limit"] == 5
        return httpx.Response(
            200,
            json=[
                {
                    "public_id": "m1",
                    "workspace_name": "w",
                    "peer_name": "p",
                    "session_name": "s",
                    "score": 0.91,
                }
            ],
        )

    monkeypatch.setattr(mcp_server, "_client", _client_with(handler))
    hits = await mcp_server.foreman_search(workspace_name="w", query="memory", limit=5)
    assert len(hits) == 1 and hits[0]["score"] == 0.91


async def test_session_context_uses_query_params(monkeypatch):
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path == "/workspaces/w/sessions/s/context"
        assert req.url.params["tokens"] == "8000"
        assert req.url.params["summary"] == "false"
        return httpx.Response(200, json={"name": "s", "messages": [], "summary": None})

    monkeypatch.setattr(mcp_server, "_client", _client_with(handler))
    out = await mcp_server.foreman_session_context(
        session_name="s",
        workspace_name="w",
        tokens=8000,
        include_summary=False,
    )
    assert out["name"] == "s"


async def test_chat_buffers_sse_stream(monkeypatch):
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path == "/workspaces/w/peers/p/chat"
        assert req.headers["Accept"] == "text/event-stream"
        sse = (
            "event: token\n"
            'data: {"delta": "hello "}\n'
            "\n"
            "event: token\n"
            'data: {"delta": "world"}\n'
            "\n"
            "event: done\n"
            'data: {"finish_reason": "stop"}\n'
            "\n"
        )
        return httpx.Response(
            200,
            content=sse.encode(),
            headers={"Content-Type": "text/event-stream"},
        )

    monkeypatch.setattr(mcp_server, "_client", _client_with(handler))
    out = await mcp_server.foreman_chat(peer_name="p", workspace_name="w", query="say hi")
    assert out["text"] == "hello world"
    assert [e["event"] for e in out["events"]] == ["token", "token", "done"]


async def test_http_error_surfaces_with_body(monkeypatch):
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "workspace not found"})

    monkeypatch.setattr(mcp_server, "_client", _client_with(handler))
    with pytest.raises(RuntimeError, match="404"):
        await mcp_server.foreman_create_peer(name="p", workspace_name="missing")
