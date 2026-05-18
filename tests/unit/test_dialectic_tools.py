"""Tool dispatch — gating, argument validation, summary formatting.

We mock the spine helpers (`vector_search`, `lakebase.session`) at the module
level so these are pure unit tests with no I/O.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from foreman.app.dialectic import tools
from foreman.lib.vector_search import DocumentHit, MessageHit


@pytest.fixture
def ctx() -> tools.ToolContext:
    return tools.ToolContext(
        workspace_name="ws", observer="alice", observed="bob", session_name="sess-1"
    )


def _async_session_returning(rows: list[dict]):
    """Build a fake `lakebase.session()` async context manager whose execute()
    returns a result with .mappings().all() / .first() that yield `rows`."""

    @asynccontextmanager
    async def _cm():
        session = MagicMock()
        result = MagicMock()
        mappings = MagicMock()
        mappings.all.return_value = rows
        mappings.first.return_value = rows[0] if rows else None
        result.mappings.return_value = mappings
        session.execute = AsyncMock(return_value=result)
        yield session

    return _cm


# ---- Spec validation --------------------------------------------------------


def test_specs_for_returns_only_requested_tools():
    out = tools.specs_for(["search_memory", "grep_messages"])
    names = [s["function"]["name"] for s in out]
    assert names == ["search_memory", "grep_messages"]


def test_specs_for_unknown_tool_raises():
    with pytest.raises(KeyError):
        tools.specs_for(["search_memory", "nonexistent"])


def test_all_six_tool_specs_have_function_calling_shape():
    for name, spec in tools.TOOL_SPECS.items():
        assert spec["type"] == "function"
        fn = spec["function"]
        assert fn["name"] == name
        assert "description" in fn
        params = fn["parameters"]
        assert params["type"] == "object"
        assert "properties" in params


# ---- Reasoning-level gating -------------------------------------------------


async def test_dispatch_refuses_disabled_tool(ctx: tools.ToolContext):
    result = await tools.dispatch(
        "grep_messages",
        {"pattern": "x"},
        ctx=ctx,
        enabled=("search_memory",),
    )
    assert "error" in result
    assert "not enabled" in result["error"]


async def test_dispatch_refuses_unknown_tool(ctx: tools.ToolContext):
    result = await tools.dispatch(
        "imaginary_tool",
        {},
        ctx=ctx,
        enabled=("imaginary_tool",),
    )
    assert "error" in result
    assert "unknown tool" in result["error"]


async def test_dispatch_returns_tool_errors_as_dict(ctx: tools.ToolContext):
    # search_memory requires a "query" string — sending an int triggers ValueError
    result = await tools.dispatch(
        "search_memory", {"query": 42}, ctx=ctx, enabled=("search_memory",)
    )
    assert "error" in result
    assert "query" in result["error"]


# ---- Tool implementations (mocked spine) ------------------------------------


async def test_search_memory_calls_vector_search_with_scope(ctx: tools.ToolContext):
    fake_hit = DocumentHit(
        id="d1", workspace_name="ws", observer="alice", observed="bob",
        level="explicit", content="bob likes jazz", score=0.91,
    )
    with patch.object(
        tools.vector_search,
        "query_documents",
        new=AsyncMock(return_value=[fake_hit]),
    ) as mock_q:
        result = await tools.search_memory(ctx, {"query": "jazz", "num_results": 5})
    mock_q.assert_called_once()
    kwargs = mock_q.call_args.kwargs
    assert kwargs["workspace_name"] == "ws"
    assert kwargs["observer"] == "alice"
    assert kwargs["observed"] == "bob"
    assert kwargs["num_results"] == 5
    assert result["results"][0]["id"] == "d1"
    assert result["results"][0]["content"] == "bob likes jazz"


async def test_search_memory_clamps_num_results(ctx: tools.ToolContext):
    with patch.object(
        tools.vector_search, "query_documents", new=AsyncMock(return_value=[])
    ) as mock_q:
        await tools.search_memory(ctx, {"query": "x", "num_results": 9999})
    assert mock_q.call_args.kwargs["num_results"] == tools.SEARCH_MAX_RESULTS


async def test_search_memory_argument_overrides_default_observer(ctx: tools.ToolContext):
    with patch.object(
        tools.vector_search, "query_documents", new=AsyncMock(return_value=[])
    ) as mock_q:
        await tools.search_memory(ctx, {"query": "x", "observer": "carol"})
    assert mock_q.call_args.kwargs["observer"] == "carol"


async def test_search_messages_hydrates_from_lakebase(ctx: tools.ToolContext):
    fake_hit = MessageHit(
        public_id="m1", workspace_name="ws", peer_name="bob",
        session_name="sess-1", score=0.88,
    )
    rows = [{"public_id": "m1", "content": "hi", "created_at": datetime(2024, 1, 1)}]
    with patch.object(
        tools.vector_search, "query_messages", new=AsyncMock(return_value=[fake_hit])
    ), patch.object(tools, "lakebase_session", _async_session_returning(rows)):
        result = await tools.search_messages(ctx, {"query": "hi"})
    assert result["results"][0]["public_id"] == "m1"
    assert result["results"][0]["content"] == "hi"
    assert result["results"][0]["created_at"] == "2024-01-01T00:00:00"


async def test_search_messages_returns_empty_when_no_hits(ctx: tools.ToolContext):
    with patch.object(
        tools.vector_search, "query_messages", new=AsyncMock(return_value=[])
    ):
        result = await tools.search_messages(ctx, {"query": "nothing"})
    assert result == {"results": []}


async def test_get_observation_context_caps_facts(ctx: tools.ToolContext):
    overflow_facts = [f"fact-{i}" for i in range(60)]
    rows = [{"facts": overflow_facts}]  # peer_cards row
    # observation rows used in second execute() — replace with empty
    @asynccontextmanager
    async def _cm():
        session = MagicMock()
        # First call: peer_cards. Second: documents_mirror.
        result_card = MagicMock()
        result_card.mappings.return_value.first.return_value = rows[0]
        result_card.mappings.return_value.all.return_value = rows
        result_docs = MagicMock()
        result_docs.mappings.return_value.first.return_value = None
        result_docs.mappings.return_value.all.return_value = []
        session.execute = AsyncMock(side_effect=[result_card, result_docs])
        yield session

    with patch.object(tools, "lakebase_session", _cm):
        result = await tools.get_observation_context(ctx, {})

    assert len(result["facts"]) == tools.PEER_CARD_FACT_CAP
    assert result["facts"][0] == "fact-0"
    assert result["recent_observations"] == []


async def test_grep_messages_returns_matches(ctx: tools.ToolContext):
    rows = [
        {
            "public_id": "m1",
            "peer_name": "bob",
            "session_name": "sess-1",
            "content": "hello world",
            "created_at": datetime(2024, 1, 2),
        }
    ]
    with patch.object(tools, "lakebase_session", _async_session_returning(rows)):
        result = await tools.grep_messages(ctx, {"pattern": "hello", "limit": 10})
    assert len(result["matches"]) == 1
    assert result["matches"][0]["content"] == "hello world"


async def test_get_messages_by_date_range_orders_ascending(ctx: tools.ToolContext):
    rows = [
        {
            "public_id": "m1",
            "peer_name": "bob",
            "session_name": "sess-1",
            "content": "earlier",
            "created_at": datetime(2024, 1, 1),
        },
        {
            "public_id": "m2",
            "peer_name": "bob",
            "session_name": "sess-1",
            "content": "later",
            "created_at": datetime(2024, 1, 3),
        },
    ]
    with patch.object(tools, "lakebase_session", _async_session_returning(rows)):
        result = await tools.get_messages_by_date_range(
            ctx, {"start": "2024-01-01", "end": "2024-01-04"}
        )
    assert [m["public_id"] for m in result["messages"]] == ["m1", "m2"]


async def test_get_reasoning_chain_returns_chain_rows(ctx: tools.ToolContext):
    rows = [
        {
            "id": "d-root",
            "observer": "alice",
            "observed": "bob",
            "level": "explicit",
            "content": "root fact",
            "source_ids": [],
            "created_at": datetime(2024, 1, 1),
            "depth": 1,
        },
        {
            "id": "d-derived",
            "observer": "alice",
            "observed": "bob",
            "level": "deductive",
            "content": "follows from root",
            "source_ids": ["d-root"],
            "created_at": datetime(2024, 1, 2),
            "depth": 0,
        },
    ]
    with patch.object(tools, "lakebase_session", _async_session_returning(rows)):
        result = await tools.get_reasoning_chain(
            ctx, {"document_id": "d-derived", "max_depth": 5}
        )
    assert result["document_id"] == "d-derived"
    assert {item["id"] for item in result["chain"]} == {"d-root", "d-derived"}


# ---- Result summarization ---------------------------------------------------


@pytest.mark.parametrize(
    "result,expected_substring",
    [
        ({"error": "boom"}, "error: boom"),
        ({"results": [1, 2, 3]}, "3 results"),
        ({"matches": [1, 2]}, "2 matches"),
        ({"messages": []}, "0 messages"),
        ({"chain": [1]}, "chain length 1"),
        ({"facts": [1, 2], "recent_observations": [1]}, "2 facts"),
        ({}, "ok"),
    ],
)
def test_summarize_result_shape(result, expected_substring):
    assert expected_substring in tools.summarize_result(result)


def test_encode_result_handles_datetimes():
    encoded = tools.encode_result({"when": datetime(2024, 1, 1)})
    assert "2024-01-01" in encoded
