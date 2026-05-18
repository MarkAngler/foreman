"""Dialectic agent — the tool-calling loop.

Drives an LLM through repeated rounds of tool calls until it stops asking for
more tools, then streams the final response. Emits structured `AgentEvent`
objects for the router to translate into SSE frames.

The loop is a straightforward implementation of OpenAI/Databricks
function-calling chat completions:

    1. Build the initial messages: [system prompt, optional preliminary
       observation context, user query].
    2. Round N: ask the model for a completion with tools attached. If the
       model returns tool calls, execute each one, append the tool results,
       and loop.
    3. Once the model returns content without tool calls, stream the chunks
       as they arrive.

Spine constraint we accept (see worktree_notes_dialectic.md): tool calling
is only supported through the OpenAI-compatible provider path inside
`llm_dispatch`. The Databricks SDK serving-endpoints client used by the
default `fmapi` provider does not propagate `tools=` to the endpoint. For the
dialectic agent to function the workspace must configure
`workspace_llm_config(role='dialectic', provider='openai-compatible', ...)`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy import text

from foreman.app.dialectic import prompts, tools
from foreman.app.dialectic.reasoning_levels import LevelConfig, resolve
from foreman.lib import llm_dispatch, vector_search
from foreman.lib.lakebase import session as lakebase_session
from foreman.lib.tracing import trace

log = logging.getLogger(__name__)

PREFETCH_RESULTS = 5
EventKind = Literal["tool_call", "tool_result", "token", "done", "error"]


@dataclass(frozen=True)
class AgentEvent:
    kind: EventKind
    data: dict[str, Any]


@dataclass
class AgentRequest:
    workspace_name: str
    peer_name: str
    query: str
    session_name: str | None = None
    reasoning_level: str = "low"
    include_session_history: bool = False
    session_history_max_tokens: int = 2000


# ---- Public entrypoint ------------------------------------------------------


@trace
async def run_agent_stream(req: AgentRequest) -> AsyncIterator[AgentEvent]:
    """Run the dialectic loop, yielding SSE-shaped events as it progresses."""
    level = resolve(req.reasoning_level)
    ctx = tools.ToolContext(
        workspace_name=req.workspace_name,
        observer=req.peer_name,
        observed=req.peer_name,
        session_name=req.session_name,
    )

    observer_card, observed_card = await _load_peer_cards(
        req.workspace_name, observer=ctx.observer, observed=ctx.observed
    )

    prefetched = await _prefetch_observations(req.query, ctx)
    preliminary = prompts.preliminary_context_block(prefetched) if prefetched else None

    system_prompt = prompts.system_prompt_for_level(
        level=level,
        observer=ctx.observer,
        observed=ctx.observed,
        observer_peer_card=observer_card,
        observed_peer_card=observed_card,
    )
    if req.include_session_history and req.session_name:
        history = await _load_session_history(
            req.workspace_name, req.session_name, req.session_history_max_tokens
        )
        if history:
            system_prompt = f"{system_prompt}\n\n## SESSION HISTORY\n\n{history}"

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": prompts.query_message(req.query, preliminary)},
    ]
    tool_specs = tools.specs_for(list(level.tools))

    async for event in _run_loop(req, level, ctx, messages, tool_specs):
        yield event


# ---- Loop -------------------------------------------------------------------


async def _run_loop(
    req: AgentRequest,
    level: LevelConfig,
    ctx: tools.ToolContext,
    messages: list[dict[str, Any]],
    tool_specs: list[dict[str, Any]],
) -> AsyncIterator[AgentEvent]:
    for iteration in range(level.max_iterations):
        try:
            response = await _completion(
                req, level, messages, tool_specs, stream=False
            )
        except Exception as exc:  # noqa: BLE001 — surface to client as event
            log.exception("dialectic completion failed")
            yield AgentEvent("error", {"message": f"{type(exc).__name__}: {exc}"})
            return

        choice = _first_choice(response)
        if choice is None:
            yield AgentEvent("error", {"message": "empty completion response"})
            return

        tool_calls = _extract_tool_calls(choice)
        if not tool_calls:
            content = _extract_content(choice)
            if content:
                yield AgentEvent("token", {"text": content})
            yield AgentEvent("done", {"iterations": iteration + 1})
            return

        messages.append(_assistant_message_with_tools(choice, tool_calls))
        for tc in tool_calls:
            yield AgentEvent("tool_call", {"name": tc["name"], "args": tc["args"]})
            result = await tools.dispatch(
                tc["name"], tc["args"], ctx=ctx, enabled=level.tools
            )
            yield AgentEvent(
                "tool_result",
                {"name": tc["name"], "summary": tools.summarize_result(result)},
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "name": tc["name"],
                    "content": tools.encode_result(result),
                }
            )

    # Iteration budget exhausted — make one final no-tools call to force a
    # synthesis from whatever has been gathered.
    try:
        stream = await _completion(req, level, messages, tool_specs=None, stream=True)
    except Exception as exc:  # noqa: BLE001
        log.exception("final dialectic synthesis failed")
        yield AgentEvent("error", {"message": f"{type(exc).__name__}: {exc}"})
        return

    async for chunk_text in _iter_stream_tokens(stream):
        if chunk_text:
            yield AgentEvent("token", {"text": chunk_text})
    yield AgentEvent("done", {"iterations": level.max_iterations, "forced_synthesis": True})


# ---- LLM dispatch wrapper ---------------------------------------------------


async def _completion(
    req: AgentRequest,
    level: LevelConfig,
    messages: list[dict[str, Any]],
    tool_specs: list[dict[str, Any]] | None,
    *,
    stream: bool,
) -> Any:
    kwargs: dict[str, Any] = {"max_tokens": level.max_output_tokens}
    if tool_specs:
        kwargs["tools"] = tool_specs
        kwargs["tool_choice"] = "auto"
    return await llm_dispatch.chat(
        workspace_name=req.workspace_name,
        role=level.role,  # type: ignore[arg-type]
        messages=messages,
        stream=stream,
        **kwargs,
    )


# ---- Response parsing -------------------------------------------------------


def _first_choice(response: Any) -> dict[str, Any] | None:
    """Coerce a non-streaming completion into a dict choice.

    The dispatcher returns either a Databricks SDK QueryEndpointResponse (for
    the SDK provider) or an OpenAI ChatCompletion (for openai-compatible).
    Both expose `choices[0]` with similar shape; we normalize to a dict.
    """
    if response is None:
        return None
    if hasattr(response, "model_dump"):
        d = response.model_dump()
    elif hasattr(response, "as_dict"):
        d = response.as_dict()
    elif isinstance(response, dict):
        d = response
    else:
        d = getattr(response, "__dict__", {})
    choices = d.get("choices") or []
    if not choices:
        return None
    first = choices[0]
    if hasattr(first, "model_dump"):
        first = first.model_dump()
    elif hasattr(first, "as_dict"):
        first = first.as_dict()
    return first if isinstance(first, dict) else None


def _extract_tool_calls(choice: dict[str, Any]) -> list[dict[str, Any]]:
    msg = choice.get("message") or {}
    raw_calls = msg.get("tool_calls") or []
    out: list[dict[str, Any]] = []
    for tc in raw_calls:
        tc = tc if isinstance(tc, dict) else getattr(tc, "model_dump", lambda: {})()
        fn = tc.get("function") or {}
        name = fn.get("name")
        if not name:
            continue
        raw_args = fn.get("arguments")
        try:
            args = json.loads(raw_args) if isinstance(raw_args, str) else (raw_args or {})
        except json.JSONDecodeError:
            args = {}
        out.append({"id": tc.get("id") or name, "name": name, "args": args})
    return out


def _extract_content(choice: dict[str, Any]) -> str:
    msg = choice.get("message") or {}
    content = msg.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        # OpenAI v1 sometimes returns a list of {type: 'text', text: ...} parts.
        return "".join(p.get("text", "") for p in content if isinstance(p, dict))
    return ""


def _assistant_message_with_tools(
    choice: dict[str, Any], tool_calls: list[dict[str, Any]]
) -> dict[str, Any]:
    msg = choice.get("message") or {}
    return {
        "role": "assistant",
        "content": msg.get("content") or "",
        "tool_calls": [
            {
                "id": tc["id"],
                "type": "function",
                "function": {
                    "name": tc["name"],
                    "arguments": json.dumps(tc["args"]),
                },
            }
            for tc in tool_calls
        ],
    }


async def _iter_stream_tokens(stream: Any) -> AsyncIterator[str]:
    """Yield text deltas from a streaming completion regardless of provider."""
    async for chunk in _aiter(stream):
        text_delta = _chunk_text(chunk)
        if text_delta:
            yield text_delta


async def _aiter(stream: Any) -> AsyncIterator[Any]:
    if hasattr(stream, "__aiter__"):
        async for c in stream:
            yield c
        return
    # Sync iterator (SDK path) — pump it on a thread.
    loop = asyncio.get_running_loop()
    iterator = iter(stream)
    while True:
        try:
            chunk = await loop.run_in_executor(None, next, iterator)
        except StopIteration:
            return
        yield chunk


def _chunk_text(chunk: Any) -> str:
    if chunk is None:
        return ""
    if hasattr(chunk, "model_dump"):
        d = chunk.model_dump()
    elif hasattr(chunk, "as_dict"):
        d = chunk.as_dict()
    elif isinstance(chunk, dict):
        d = chunk
    else:
        d = getattr(chunk, "__dict__", {})
    choices = d.get("choices") or []
    if not choices:
        return ""
    first = choices[0]
    if hasattr(first, "model_dump"):
        first = first.model_dump()
    elif hasattr(first, "as_dict"):
        first = first.as_dict()
    delta = (first or {}).get("delta") or (first or {}).get("message") or {}
    content = delta.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(p.get("text", "") for p in content if isinstance(p, dict))
    return ""


# ---- Pre-fetch and history --------------------------------------------------


@trace
async def _prefetch_observations(
    query: str, ctx: tools.ToolContext
) -> list[str]:
    """Run a cheap self-collection vector search for preliminary context."""
    try:
        hits = await vector_search.query_documents(
            query_text=query,
            workspace_name=ctx.workspace_name,
            observer=ctx.observed,
            observed=ctx.observed,
            num_results=PREFETCH_RESULTS,
        )
    except Exception as exc:  # noqa: BLE001 — prefetch is best-effort
        log.warning("dialectic prefetch failed: %s", exc)
        return []
    return [h.content for h in hits if h.content]


async def _load_peer_cards(
    workspace_name: str, *, observer: str, observed: str
) -> tuple[list[str] | None, list[str] | None]:
    names = {observer, observed}
    async with lakebase_session() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT peer_name, facts FROM peer_cards "
                    "WHERE workspace_name = :w AND peer_name = ANY(:names)"
                ),
                {"w": workspace_name, "names": list(names)},
            )
        ).mappings().all()
    by_peer: dict[str, list[str]] = {r["peer_name"]: list(r["facts"] or []) for r in rows}
    return by_peer.get(observer), by_peer.get(observed)


async def _load_session_history(
    workspace_name: str, session_name: str, max_tokens: int
) -> str | None:
    if max_tokens <= 0:
        return None
    # Approximate cap: 4 chars/token. Pull recent rows DESC, accumulate, reverse.
    char_budget = max_tokens * 4
    async with lakebase_session() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT peer_name, content, created_at FROM messages "
                    "WHERE workspace_name = :w AND session_name = :s "
                    "ORDER BY created_at DESC LIMIT 200"
                ),
                {"w": workspace_name, "s": session_name},
            )
        ).mappings().all()
    if not rows:
        return None

    selected: list[dict[str, Any]] = []
    used = 0
    for r in rows:
        line = f"[{r['created_at']}] {r['peer_name']}: {r['content']}"
        if used + len(line) > char_budget and selected:
            break
        selected.append({"created_at": r["created_at"], "line": line})
        used += len(line) + 1
    selected.sort(key=lambda x: x["created_at"])  # chronological
    return "\n".join(item["line"] for item in selected)
