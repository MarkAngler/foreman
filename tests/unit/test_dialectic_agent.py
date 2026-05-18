"""Agent loop — tool-call dispatch, iteration budget, SSE event stream."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from foreman.app.dialectic import agent, tools
from foreman.app.dialectic.agent import AgentRequest


def _completion(content: str | None = None, tool_calls: list[dict] | None = None) -> dict:
    """Build a fake non-streaming completion response in OpenAI shape."""
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


def _stream_chunks(parts: list[str]):
    """Build a fake async stream that yields chunk dicts."""

    class _Stream:
        def __aiter__(self):
            self._parts = iter(parts)
            return self

        async def __anext__(self):
            try:
                p = next(self._parts)
            except StopIteration as exc:
                raise StopAsyncIteration from exc
            return {"choices": [{"delta": {"content": p}}]}

    return _Stream()


@asynccontextmanager
async def _empty_session():
    s = MagicMock()
    result = MagicMock()
    result.mappings.return_value.first.return_value = None
    result.mappings.return_value.all.return_value = []
    s.execute = AsyncMock(return_value=result)
    yield s


async def _consume(events_iter):
    return [e async for e in events_iter]


# ---- No tool calls path -----------------------------------------------------


async def test_loop_streams_token_when_model_returns_content_directly():
    final = _completion(content="hello world")
    with patch.object(
        agent.llm_dispatch, "chat", new=AsyncMock(return_value=final)
    ), patch.object(
        agent.vector_search, "query_documents", new=AsyncMock(return_value=[])
    ), patch.object(agent, "lakebase_session", _empty_session):
        events = await _consume(
            agent.run_agent_stream(
                AgentRequest(
                    workspace_name="ws",
                    peer_name="alice",
                    query="hi",
                    reasoning_level="minimal",
                )
            )
        )

    kinds = [e.kind for e in events]
    assert kinds == ["token", "done"]
    assert events[0].data["text"] == "hello world"
    assert events[1].data["iterations"] == 1


# ---- Tool-call dispatch -----------------------------------------------------


async def test_loop_dispatches_tool_call_then_streams_final():
    first = _completion(
        tool_calls=[{"name": "search_memory", "arguments": '{"query": "jazz"}'}]
    )
    second = _completion(content="bob likes jazz")

    fake_impl = AsyncMock(
        return_value={"results": [{"content": "bob likes jazz"}]}
    )
    with patch.object(
        agent.llm_dispatch, "chat", new=AsyncMock(side_effect=[first, second])
    ), patch.object(
        agent.vector_search, "query_documents", new=AsyncMock(return_value=[])
    ), patch.object(agent, "lakebase_session", _empty_session), patch.dict(
        tools._IMPLEMENTATIONS, {"search_memory": fake_impl}
    ):
        events = await _consume(
            agent.run_agent_stream(
                AgentRequest(
                    workspace_name="ws",
                    peer_name="bob",
                    query="what does bob like?",
                    reasoning_level="minimal",
                )
            )
        )

    kinds = [e.kind for e in events]
    assert kinds == ["tool_call", "tool_result", "token", "done"]
    assert events[0].data["name"] == "search_memory"
    assert events[0].data["args"] == {"query": "jazz"}
    assert "1 results" in events[1].data["summary"]
    assert events[2].data["text"] == "bob likes jazz"
    fake_impl.assert_called_once()


async def test_loop_refuses_tool_outside_reasoning_level():
    """At minimal reasoning, only search_memory is enabled. The model trying
    to call grep_messages must result in a tool_result with an error string,
    NOT a crash."""
    first = _completion(
        tool_calls=[{"name": "grep_messages", "arguments": '{"pattern": "x"}'}]
    )
    second = _completion(content="recovered")

    with patch.object(
        agent.llm_dispatch, "chat", new=AsyncMock(side_effect=[first, second])
    ), patch.object(
        agent.vector_search, "query_documents", new=AsyncMock(return_value=[])
    ), patch.object(agent, "lakebase_session", _empty_session):
        events = await _consume(
            agent.run_agent_stream(
                AgentRequest(
                    workspace_name="ws",
                    peer_name="alice",
                    query="search",
                    reasoning_level="minimal",
                )
            )
        )

    summaries = [e.data["summary"] for e in events if e.kind == "tool_result"]
    assert any("not enabled" in s for s in summaries)


# ---- Iteration budget -------------------------------------------------------


async def test_loop_forces_synthesis_when_iterations_exhausted():
    """If the model keeps returning tool calls forever, the loop must hit the
    iteration cap and force a final no-tools synthesis call (streamed)."""
    repeating = _completion(
        tool_calls=[{"name": "search_memory", "arguments": '{"query": "again"}'}]
    )
    final_stream = _stream_chunks(["forced ", "answer"])

    completion_mock = AsyncMock()
    completion_mock.side_effect = [repeating, repeating, repeating, final_stream]

    with patch.object(agent.llm_dispatch, "chat", new=completion_mock), patch.object(
        agent.vector_search, "query_documents", new=AsyncMock(return_value=[])
    ), patch.object(agent, "lakebase_session", _empty_session), patch.object(
        tools, "search_memory", new=AsyncMock(return_value={"results": []})
    ):
        events = await _consume(
            agent.run_agent_stream(
                AgentRequest(
                    workspace_name="ws",
                    peer_name="alice",
                    query="loop",
                    reasoning_level="minimal",  # max_iterations=3
                )
            )
        )

    text = "".join(e.data["text"] for e in events if e.kind == "token")
    assert text == "forced answer"
    done = [e for e in events if e.kind == "done"][-1]
    assert done.data["forced_synthesis"] is True


# ---- Pre-fetch context -----------------------------------------------------


async def test_prefetch_results_inject_into_user_message():
    """The first user message must include the prefetched observation block
    when documents_idx returns hits."""
    captured: dict = {}

    async def fake_chat(**kwargs):
        captured["messages"] = kwargs["messages"]
        return _completion(content="ok")

    from foreman.lib.vector_search import DocumentHit

    hit = DocumentHit(
        id="d1", workspace_name="ws", observer="alice", observed="alice",
        level="explicit", content="alice loves coffee", score=0.9,
    )

    with patch.object(agent.llm_dispatch, "chat", new=fake_chat), patch.object(
        agent.vector_search, "query_documents", new=AsyncMock(return_value=[hit])
    ), patch.object(agent, "lakebase_session", _empty_session):
        await _consume(
            agent.run_agent_stream(
                AgentRequest(
                    workspace_name="ws",
                    peer_name="alice",
                    query="what?",
                    reasoning_level="minimal",
                )
            )
        )

    user_msg = captured["messages"][1]
    assert user_msg["role"] == "user"
    assert "alice loves coffee" in user_msg["content"]
    assert "Query: what?" in user_msg["content"]


# ---- LLM error surface ------------------------------------------------------


async def test_loop_emits_error_event_when_llm_fails():
    with patch.object(
        agent.llm_dispatch, "chat", new=AsyncMock(side_effect=RuntimeError("boom"))
    ), patch.object(
        agent.vector_search, "query_documents", new=AsyncMock(return_value=[])
    ), patch.object(agent, "lakebase_session", _empty_session):
        events = await _consume(
            agent.run_agent_stream(
                AgentRequest(
                    workspace_name="ws",
                    peer_name="alice",
                    query="hi",
                    reasoning_level="minimal",
                )
            )
        )

    assert events[-1].kind == "error"
    assert "boom" in events[-1].data["message"]


# ---- Tool call argument parsing --------------------------------------------


async def test_invalid_tool_arguments_decoded_as_empty_dict():
    """If the model emits malformed JSON in tool arguments, the loop must
    coerce to an empty dict and let the tool's own validation respond."""
    first = _completion(
        tool_calls=[{"name": "search_memory", "arguments": "not valid json {{"}]
    )
    second = _completion(content="done")

    with patch.object(
        agent.llm_dispatch, "chat", new=AsyncMock(side_effect=[first, second])
    ), patch.object(
        agent.vector_search, "query_documents", new=AsyncMock(return_value=[])
    ), patch.object(agent, "lakebase_session", _empty_session):
        events = await _consume(
            agent.run_agent_stream(
                AgentRequest(
                    workspace_name="ws",
                    peer_name="alice",
                    query="hi",
                    reasoning_level="minimal",
                )
            )
        )

    tool_call = next(e for e in events if e.kind == "tool_call")
    assert tool_call.data["args"] == {}
