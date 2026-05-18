"""End-to-end dialectic test: query -> tool loop -> SSE event stream.

Lakebase and Vector Search are mocked because the real services live behind
an OAuth-authenticated workspace. The flow exercised is the full agent
orchestration: peer-card load -> prefetch documents -> system prompt assembly
-> LLM round 1 returning a tool call -> tool dispatched against the mocked
spine -> tool result fed back -> LLM round 2 streaming the synthesis.

Run with:
    PYTHONPATH=src .venv/Scripts/python.exe -m pytest tests/integration/ -m integration
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from foreman.app.dialectic import agent, tools
from foreman.app.routers.chat import router as chat_router
from foreman.lib import auth
from foreman.lib.vector_search import DocumentHit

pytestmark = pytest.mark.integration


def _completion(content: str | None = None, tool_calls: list[dict] | None = None) -> dict:
    msg: dict = {"role": "assistant", "content": content or ""}
    if tool_calls:
        msg["tool_calls"] = [
            {
                "id": tc.get("id", f"call-{i}"),
                "type": "function",
                "function": {"name": tc["name"], "arguments": tc["arguments"]},
            }
            for i, tc in enumerate(tool_calls)
        ]
    return {"choices": [{"message": msg}]}


@asynccontextmanager
async def _empty_session():
    s = MagicMock()
    result = MagicMock()
    result.mappings.return_value.first.return_value = None
    result.mappings.return_value.all.return_value = []
    s.execute = AsyncMock(return_value=result)
    yield s


async def test_dialectic_endpoint_streams_sse_frames(monkeypatch):
    """POST /workspaces/{w}/peers/{p}/chat returns an SSE stream of agent events."""
    from fastapi import FastAPI

    # Stub auth signing key so issued JWTs validate.
    auth.signing_key.cache_clear()
    monkeypatch.setattr(auth, "signing_key", lambda: "integration-test-key")

    app = FastAPI()
    app.include_router(chat_router)

    prefetched = DocumentHit(
        id="d1", workspace_name="ws", observer="alice", observed="alice",
        level="explicit", content="alice studied music in college", score=0.9,
    )

    first = _completion(
        tool_calls=[{"name": "search_memory", "arguments": '{"query": "music"}'}]
    )
    second = _completion(content="Alice studied music in college and loves jazz.")
    chat_mock = AsyncMock(side_effect=[first, second])

    fake_search_memory = AsyncMock(
        return_value={"results": [{"content": "alice studied music"}]}
    )

    token = auth.issue(workspace="ws", peer="alice")

    with patch.object(agent.llm_dispatch, "chat", new=chat_mock), patch.object(
        agent.vector_search, "query_documents", new=AsyncMock(return_value=[prefetched])
    ), patch.object(agent, "lakebase_session", _empty_session), patch.dict(
        tools._IMPLEMENTATIONS, {"search_memory": fake_search_memory}
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/workspaces/ws/peers/alice/chat",
                json={
                    "query": "what did alice study?",
                    "reasoning_level": "minimal",
                },
                headers={"Authorization": f"Bearer {token}"},
            )
            assert resp.status_code == 200
            body = resp.text

    assert "event: tool_call" in body
    assert '"name": "search_memory"' in body
    assert "event: tool_result" in body
    assert "event: token" in body
    assert "Alice studied music in college and loves jazz." in body
    assert "event: done" in body

    # The LLM was invoked twice: once with tools (round 1), once for synthesis.
    assert chat_mock.call_count == 2
    fake_search_memory.assert_called_once()


async def test_dialectic_endpoint_rejects_wrong_peer_token(monkeypatch):
    from fastapi import FastAPI

    auth.signing_key.cache_clear()
    monkeypatch.setattr(auth, "signing_key", lambda: "integration-test-key")

    app = FastAPI()
    app.include_router(chat_router)

    # Token scoped to a different peer.
    token = auth.issue(workspace="ws", peer="bob")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/workspaces/ws/peers/alice/chat",
            json={"query": "x", "reasoning_level": "minimal"},
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 403
