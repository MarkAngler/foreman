"""Unit tests for the OBO MCP tools.

The whole peer-isolation invariant is: every tool resolves the peer from the
OBO header on every call, and the value never comes from caller-supplied args.
These tests assert that the downstream calls (`vector_search.query_messages`,
the lakebase SQL) always use the OBO-derived peer + the bound workspace.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

import pytest
from starlette.requests import Request

from foreman.lib.vector_search import MessageHit
from foreman.mcp_http import server as mcp_http_server


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "headers": [(b"x-forwarded-access-token", b"tok")],
            "method": "POST",
            "path": "/",
            "query_string": b"",
        }
    )


@dataclass
class _RequestCtx:
    request: Request


@dataclass
class _Ctx:
    request_context: _RequestCtx


def _ctx() -> _Ctx:
    return _Ctx(request_context=_RequestCtx(request=_request()))


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows
        self.executed: list[tuple[str, dict]] = []

    async def execute(self, statement, params=None):
        self.executed.append((str(statement), params or {}))

        class _R:
            def __init__(self, rows):
                self._rows = rows

            def mappings(self):
                return self

            def first(self):
                return self._rows[0] if self._rows else None

            def all(self):
                return self._rows

        return _R(self._rows)

    async def commit(self):
        pass


@pytest.fixture(autouse=True)
def _bind_workspace():
    with patch.dict(os.environ, {"FOREMAN_MCP_WORKSPACE": "genie-ws"}):
        yield


@pytest.fixture
def _stub_identity():
    with (
        patch.object(mcp_http_server, "current_user_email", AsyncMock(return_value="Mark@x.com")),
        patch.object(mcp_http_server, "ensure_peer", AsyncMock()) as ens,
    ):
        yield ens


async def test_recall_passes_obo_peer_to_vector_search(_stub_identity):
    captured = {}

    async def fake_query(**kwargs):
        captured.update(kwargs)
        return [
            MessageHit(
                public_id="m1",
                workspace_name="genie-ws",
                peer_name="mark-at-x-com",
                session_name="s1",
                content="hi",
                score=0.9,
            )
        ]

    with patch.object(mcp_http_server.vector_search, "query_messages", fake_query):
        out = await mcp_http_server.foreman_recall_about_me(query="anything", ctx=_ctx(), limit=3)

    assert captured["workspace_name"] == "genie-ws"
    assert captured["peer_name"] == "mark-at-x-com"
    assert captured["num_results"] == 3
    assert captured["hybrid"] is True
    assert out == [{"public_id": "m1", "session_name": "s1", "content": "hi", "score": 0.9}]
    _stub_identity.assert_awaited_once_with("genie-ws", "mark-at-x-com")


async def test_recall_clamps_limit(_stub_identity):
    captured = {}

    async def fake_query(**kwargs):
        captured.update(kwargs)
        return []

    with patch.object(mcp_http_server.vector_search, "query_messages", fake_query):
        await mcp_http_server.foreman_recall_about_me(query="q", ctx=_ctx(), limit=10_000)
    assert captured["num_results"] == 50

    captured.clear()
    with patch.object(mcp_http_server.vector_search, "query_messages", fake_query):
        await mcp_http_server.foreman_recall_about_me(query="q", ctx=_ctx(), limit=0)
    assert captured["num_results"] == 1


async def test_peer_card_returns_facts(_stub_identity):
    import datetime as dt

    rows = [{"facts": ["lives in PNW", "uses pgsql"], "updated_at": dt.datetime(2026, 5, 1)}]
    fake = _FakeSession(rows)

    @asynccontextmanager
    async def _ctx_mgr():
        yield fake

    with patch.object(mcp_http_server, "session", _ctx_mgr):
        out = await mcp_http_server.foreman_peer_card(ctx=_ctx())

    sql, params = fake.executed[0]
    assert "peer_cards" in sql
    assert params == {"w": "genie-ws", "p": "mark-at-x-com"}
    assert out["peer_name"] == "mark-at-x-com"
    assert out["facts"] == ["lives in PNW", "uses pgsql"]


async def test_peer_card_empty_when_no_row(_stub_identity):
    fake = _FakeSession([])

    @asynccontextmanager
    async def _ctx_mgr():
        yield fake

    with patch.object(mcp_http_server, "session", _ctx_mgr):
        out = await mcp_http_server.foreman_peer_card(ctx=_ctx())
    assert out == {"peer_name": "mark-at-x-com", "facts": [], "updated_at": None}


async def test_recent_sessions_filters_by_obo_peer(_stub_identity):
    import datetime as dt

    rows = [
        {
            "name": "branch-foo",
            "created_at": dt.datetime(2026, 5, 10),
            "summary": "we discussed X",
        }
    ]
    fake = _FakeSession(rows)

    @asynccontextmanager
    async def _ctx_mgr():
        yield fake

    with patch.object(mcp_http_server, "session", _ctx_mgr):
        out = await mcp_http_server.foreman_recent_sessions(ctx=_ctx(), limit=3)

    sql, params = fake.executed[0]
    assert "session_peers" in sql
    assert params == {"w": "genie-ws", "p": "mark-at-x-com", "n": 3}
    assert out == [
        {
            "session_name": "branch-foo",
            "created_at": "2026-05-10T00:00:00",
            "summary": "we discussed X",
        }
    ]


def test_tools_have_no_identity_args():
    """Signature-level proof: callers cannot pass peer_name or workspace_name."""
    import inspect

    for tool_name in (
        "foreman_recall_about_me",
        "foreman_peer_card",
        "foreman_recent_sessions",
    ):
        tool = getattr(mcp_http_server, tool_name)
        params = inspect.signature(tool).parameters
        assert "peer_name" not in params, f"{tool_name} must not accept peer_name"
        assert "workspace_name" not in params, f"{tool_name} must not accept workspace_name"


def test_workspace_required_at_startup(monkeypatch):
    monkeypatch.delenv("FOREMAN_MCP_WORKSPACE", raising=False)
    with pytest.raises(RuntimeError):
        mcp_http_server._workspace()
