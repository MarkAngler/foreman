"""Six tools the dialectic agent can call.

Each tool has:
  - a JSON-schema function-calling spec (OpenAI / Databricks compatible)
  - an async implementation that receives a `ToolContext` carrying scope
    (workspace_name, observer, observed, session_name) and the LLM-supplied
    arguments dict.

The implementations sit on top of the spine helpers (`vector_search` and
`lakebase`) and never call them through anything other than the documented
public API.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from sqlalchemy import bindparam, text

from foreman.lib import vector_search
from foreman.lib.lakebase import session as lakebase_session
from foreman.lib.tracing import trace

PEER_CARD_FACT_CAP = 40
RECENT_DOCS_CAP = 50
GREP_DEFAULT_LIMIT = 50
GREP_MAX_LIMIT = 200
DATE_RANGE_DEFAULT_LIMIT = 200
DATE_RANGE_MAX_LIMIT = 500
REASONING_DEFAULT_DEPTH = 10
REASONING_MAX_DEPTH = 25
SEARCH_DEFAULT_RESULTS = 10
SEARCH_MAX_RESULTS = 40


@dataclass(frozen=True)
class ToolContext:
    workspace_name: str
    observer: str
    observed: str
    session_name: str | None


# ---- Tool spec (OpenAI / Databricks function-calling format) ----------------

TOOL_SPECS: dict[str, dict[str, Any]] = {
    "search_memory": {
        "type": "function",
        "function": {
            "name": "search_memory",
            "description": (
                "Semantic search over stored observations (documents) about "
                "the peer. Returns relevant fact snippets."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query text."},
                    "observer": {
                        "type": "string",
                        "description": "Observer peer name. Defaults to the agent's observer.",
                    },
                    "observed": {
                        "type": "string",
                        "description": "Observed peer name. Defaults to the agent's observed.",
                    },
                    "num_results": {
                        "type": "integer",
                        "description": f"Number of results (1..{SEARCH_MAX_RESULTS}). Default {SEARCH_DEFAULT_RESULTS}.",
                    },
                },
                "required": ["query"],
            },
        },
    },
    "search_messages": {
        "type": "function",
        "function": {
            "name": "search_messages",
            "description": (
                "Semantic search over raw messages in the workspace. "
                "Returns matching messages with peer/session metadata."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "peer_name": {
                        "type": "string",
                        "description": "Restrict to messages authored by this peer.",
                    },
                    "session_name": {
                        "type": "string",
                        "description": "Restrict to a specific session.",
                    },
                    "num_results": {"type": "integer"},
                },
                "required": ["query"],
            },
        },
    },
    "get_observation_context": {
        "type": "function",
        "function": {
            "name": "get_observation_context",
            "description": (
                "Return the peer's compact identity card (≤40 facts) plus the "
                "most recent observations about that peer."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "peer_name": {
                        "type": "string",
                        "description": "Peer to load context for. Defaults to the agent's observed peer.",
                    },
                },
            },
        },
    },
    "grep_messages": {
        "type": "function",
        "function": {
            "name": "grep_messages",
            "description": (
                "Case-insensitive substring search over message bodies. Use "
                "for exact names, dates, or keywords."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Substring to match (ILIKE %pattern%).",
                    },
                    "session_name": {"type": "string"},
                    "limit": {"type": "integer"},
                },
                "required": ["pattern"],
            },
        },
    },
    "get_messages_by_date_range": {
        "type": "function",
        "function": {
            "name": "get_messages_by_date_range",
            "description": "Return messages whose created_at falls in [start, end].",
            "parameters": {
                "type": "object",
                "properties": {
                    "start": {
                        "type": "string",
                        "description": "ISO-8601 timestamp. Inclusive lower bound.",
                    },
                    "end": {
                        "type": "string",
                        "description": "ISO-8601 timestamp. Inclusive upper bound.",
                    },
                    "session_name": {"type": "string"},
                    "limit": {"type": "integer"},
                },
                "required": ["start", "end"],
            },
        },
    },
    "get_reasoning_chain": {
        "type": "function",
        "function": {
            "name": "get_reasoning_chain",
            "description": (
                "Walk a derived document's source_ids back to explicit roots. "
                "Returns the chain of premise documents in DAG order."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "document_id": {"type": "string"},
                    "max_depth": {"type": "integer"},
                },
                "required": ["document_id"],
            },
        },
    },
}


def specs_for(tool_names: list[str]) -> list[dict[str, Any]]:
    missing = [n for n in tool_names if n not in TOOL_SPECS]
    if missing:
        raise KeyError(f"unknown tool name(s): {missing}")
    return [TOOL_SPECS[n] for n in tool_names]


# ---- Tool implementations ---------------------------------------------------


@trace
async def search_memory(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    query = _require_str(args, "query")
    observer = args.get("observer") or ctx.observer
    observed = args.get("observed") or ctx.observed
    num_results = _clamp_int(
        args.get("num_results"), default=SEARCH_DEFAULT_RESULTS, lo=1, hi=SEARCH_MAX_RESULTS
    )
    hits = await vector_search.query_documents(
        query_text=query,
        workspace_name=ctx.workspace_name,
        observer=observer,
        observed=observed,
        num_results=num_results,
    )
    return {
        "results": [
            {
                "id": h.id,
                "level": h.level,
                "content": h.content,
                "score": h.score,
                "observer": h.observer,
                "observed": h.observed,
            }
            for h in hits
        ]
    }


@trace
async def search_messages(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    query = _require_str(args, "query")
    peer_name = args.get("peer_name")
    session_name = args.get("session_name")
    num_results = _clamp_int(
        args.get("num_results"), default=SEARCH_DEFAULT_RESULTS, lo=1, hi=SEARCH_MAX_RESULTS
    )
    hits = await vector_search.query_messages(
        query_text=query,
        workspace_name=ctx.workspace_name,
        peer_name=peer_name,
        session_name=session_name,
        num_results=num_results,
    )
    if not hits:
        return {"results": []}

    public_ids = [h.public_id for h in hits]
    rows_by_id = await _hydrate_messages(ctx.workspace_name, public_ids)
    results: list[dict[str, Any]] = []
    for h in hits:
        row = rows_by_id.get(h.public_id)
        results.append(
            {
                "public_id": h.public_id,
                "peer_name": h.peer_name,
                "session_name": h.session_name,
                "score": h.score,
                "content": row["content"] if row else None,
                "created_at": _iso(row["created_at"]) if row else None,
            }
        )
    return {"results": results}


@trace
async def get_observation_context(
    ctx: ToolContext, args: dict[str, Any]
) -> dict[str, Any]:
    peer = args.get("peer_name") or ctx.observed
    async with lakebase_session() as s:
        card_row = (
            await s.execute(
                text(
                    "SELECT facts FROM peer_cards "
                    "WHERE workspace_name = :w AND peer_name = :p"
                ),
                {"w": ctx.workspace_name, "p": peer},
            )
        ).mappings().first()
        facts = list((card_row or {}).get("facts") or [])[:PEER_CARD_FACT_CAP]

        doc_rows = (
            await s.execute(
                text(
                    "SELECT id, observer, observed, level, content, created_at "
                    "FROM documents_mirror "
                    "WHERE workspace_name = :w "
                    "AND (observer = :p OR observed = :p) "
                    "ORDER BY created_at DESC LIMIT :n"
                ),
                {"w": ctx.workspace_name, "p": peer, "n": RECENT_DOCS_CAP},
            )
        ).mappings().all()

    return {
        "peer_name": peer,
        "facts": facts,
        "recent_observations": [
            {
                "id": r["id"],
                "observer": r["observer"],
                "observed": r["observed"],
                "level": r["level"],
                "content": r["content"],
                "created_at": _iso(r["created_at"]),
            }
            for r in doc_rows
        ],
    }


@trace
async def grep_messages(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    pattern = _require_str(args, "pattern")
    session_name = args.get("session_name")
    limit = _clamp_int(
        args.get("limit"), default=GREP_DEFAULT_LIMIT, lo=1, hi=GREP_MAX_LIMIT
    )

    sql = (
        "SELECT public_id, peer_name, session_name, content, created_at "
        "FROM messages "
        "WHERE workspace_name = :w AND content ILIKE :pat"
    )
    params: dict[str, Any] = {"w": ctx.workspace_name, "pat": f"%{pattern}%"}
    if session_name:
        sql += " AND session_name = :s"
        params["s"] = session_name
    sql += " ORDER BY created_at DESC LIMIT :n"
    params["n"] = limit

    async with lakebase_session() as s:
        rows = (await s.execute(text(sql), params)).mappings().all()

    return {
        "pattern": pattern,
        "matches": [
            {
                "public_id": r["public_id"],
                "peer_name": r["peer_name"],
                "session_name": r["session_name"],
                "content": r["content"],
                "created_at": _iso(r["created_at"]),
            }
            for r in rows
        ],
    }


@trace
async def get_messages_by_date_range(
    ctx: ToolContext, args: dict[str, Any]
) -> dict[str, Any]:
    start = _require_str(args, "start")
    end = _require_str(args, "end")
    session_name = args.get("session_name")
    limit = _clamp_int(
        args.get("limit"), default=DATE_RANGE_DEFAULT_LIMIT, lo=1, hi=DATE_RANGE_MAX_LIMIT
    )

    sql = (
        "SELECT public_id, peer_name, session_name, content, created_at "
        "FROM messages "
        "WHERE workspace_name = :w "
        "AND created_at >= :start AND created_at <= :end"
    )
    params: dict[str, Any] = {"w": ctx.workspace_name, "start": start, "end": end}
    if session_name:
        sql += " AND session_name = :s"
        params["s"] = session_name
    sql += " ORDER BY created_at ASC LIMIT :n"
    params["n"] = limit

    async with lakebase_session() as s:
        rows = (await s.execute(text(sql), params)).mappings().all()

    return {
        "start": start,
        "end": end,
        "messages": [
            {
                "public_id": r["public_id"],
                "peer_name": r["peer_name"],
                "session_name": r["session_name"],
                "content": r["content"],
                "created_at": _iso(r["created_at"]),
            }
            for r in rows
        ],
    }


@trace
async def get_reasoning_chain(
    ctx: ToolContext, args: dict[str, Any]
) -> dict[str, Any]:
    document_id = _require_str(args, "document_id")
    max_depth = _clamp_int(
        args.get("max_depth"), default=REASONING_DEFAULT_DEPTH, lo=1, hi=REASONING_MAX_DEPTH
    )

    # Recursive CTE: starting from the requested doc, follow source_ids until
    # explicit roots (source_ids = '{}') or max_depth is reached.
    sql = """
        WITH RECURSIVE chain (id, observer, observed, level, content,
                              source_ids, created_at, depth) AS (
            SELECT id, observer, observed, level, content,
                   source_ids, created_at, 0
            FROM documents_mirror
            WHERE workspace_name = :w AND id = :doc_id
            UNION ALL
            SELECT d.id, d.observer, d.observed, d.level, d.content,
                   d.source_ids, d.created_at, c.depth + 1
            FROM documents_mirror d
            JOIN chain c ON d.id = ANY(c.source_ids)
            WHERE d.workspace_name = :w AND c.depth < :depth
        )
        SELECT DISTINCT ON (id) id, observer, observed, level, content,
                                source_ids, created_at, depth
        FROM chain
        ORDER BY id, depth ASC
    """
    async with lakebase_session() as s:
        rows = (
            await s.execute(
                text(sql),
                {"w": ctx.workspace_name, "doc_id": document_id, "depth": max_depth},
            )
        ).mappings().all()

    return {
        "document_id": document_id,
        "chain": [
            {
                "id": r["id"],
                "observer": r["observer"],
                "observed": r["observed"],
                "level": r["level"],
                "content": r["content"],
                "source_ids": list(r["source_ids"] or []),
                "created_at": _iso(r["created_at"]),
                "depth": r["depth"],
            }
            for r in rows
        ],
    }


# ---- Dispatch ---------------------------------------------------------------

ToolImpl = Callable[[ToolContext, dict[str, Any]], Awaitable[dict[str, Any]]]

_IMPLEMENTATIONS: dict[str, ToolImpl] = {
    "search_memory": search_memory,
    "search_messages": search_messages,
    "get_observation_context": get_observation_context,
    "grep_messages": grep_messages,
    "get_messages_by_date_range": get_messages_by_date_range,
    "get_reasoning_chain": get_reasoning_chain,
}


async def dispatch(
    name: str,
    args: dict[str, Any],
    *,
    ctx: ToolContext,
    enabled: tuple[str, ...],
) -> dict[str, Any]:
    """Run the named tool against `ctx`. Refuses tools not in `enabled`.

    Wraps tool errors so the loop can return them to the model rather than
    crashing the request — the model decides whether to recover.
    """
    if name not in enabled:
        return {"error": f"tool {name!r} is not enabled at the current reasoning level"}
    impl = _IMPLEMENTATIONS.get(name)
    if impl is None:
        return {"error": f"unknown tool {name!r}"}
    try:
        return await impl(ctx, args or {})
    except Exception as exc:  # noqa: BLE001 — errors must be returned to the model
        return {"error": f"{type(exc).__name__}: {exc}"}


def summarize_result(result: dict[str, Any]) -> str:
    """Compact one-line description of a tool result for the SSE stream."""
    if "error" in result:
        return f"error: {result['error']}"
    if "results" in result:
        return f"{len(result['results'])} results"
    if "matches" in result:
        return f"{len(result['matches'])} matches"
    if "messages" in result:
        return f"{len(result['messages'])} messages"
    if "chain" in result:
        return f"chain length {len(result['chain'])}"
    if "facts" in result:
        return f"{len(result['facts'])} facts, {len(result.get('recent_observations', []))} obs"
    return "ok"


# ---- Helpers ----------------------------------------------------------------


async def _hydrate_messages(
    workspace_name: str, public_ids: list[str]
) -> dict[str, dict[str, Any]]:
    if not public_ids:
        return {}
    stmt = text(
        "SELECT public_id, content, created_at FROM messages "
        "WHERE workspace_name = :w AND public_id IN :ids"
    ).bindparams(bindparam("ids", expanding=True))
    async with lakebase_session() as s:
        rows = (
            await s.execute(stmt, {"w": workspace_name, "ids": public_ids})
        ).mappings().all()
    return {r["public_id"]: dict(r) for r in rows}


def _require_str(args: dict[str, Any], key: str) -> str:
    val = args.get(key)
    if not isinstance(val, str) or not val.strip():
        raise ValueError(f"missing required string argument {key!r}")
    return val


def _clamp_int(raw: Any, *, default: int, lo: int, hi: int) -> int:
    if raw is None:
        return default
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return default
    return max(lo, min(n, hi))


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


# Convenience: JSON-encode a tool result so callers can drop it straight into
# the chat-completions tool message payload.
def encode_result(result: dict[str, Any]) -> str:
    return json.dumps(result, default=str)
