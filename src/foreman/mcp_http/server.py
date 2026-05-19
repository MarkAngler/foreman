"""FastMCP server exposing three read-only memory tools, peer-pinned by OBO.

The peer is derived server-side from the OBO token in each request. Tool
arguments NEVER include peer_name or workspace_name — the workspace is bound
once at startup via FOREMAN_MCP_WORKSPACE, and the peer is recomputed from
the forwarded identity for every call. A caller cannot retrieve memories
about anyone other than themselves.
"""

from __future__ import annotations

import os
from typing import Any

from mcp.server.fastmcp import Context, FastMCP
from sqlalchemy import text
from starlette.requests import Request

from foreman.lib import vector_search
from foreman.lib.lakebase import session
from foreman.mcp_http.identity import current_user_email, peer_name_for
from foreman.mcp_http.peers import ensure_peer

mcp = FastMCP("foreman-genie")


def _workspace() -> str:
    ws = os.environ.get("FOREMAN_MCP_WORKSPACE")
    if not ws:
        raise RuntimeError(
            "FOREMAN_MCP_WORKSPACE is not set; the OBO MCP refuses to start "
            "without a pre-bound Foreman workspace."
        )
    return ws


async def resolve_peer(request: Request) -> tuple[str, str]:
    email = await current_user_email(request)
    peer = peer_name_for(email)
    workspace = _workspace()
    await ensure_peer(workspace, peer)
    return workspace, peer


def _request_from_ctx(ctx: Context) -> Request:
    return ctx.request_context.request


@mcp.tool()
async def foreman_recall_about_me(
    query: str, ctx: Context, limit: int = 10
) -> list[dict[str, Any]]:
    """Search Foreman's memory for content involving the calling user.

    Hybrid semantic + lexical search over messages where the calling user is
    the speaker. Returns up to `limit` hits with content + score. The peer is
    derived from the OBO token on every call and cannot be overridden.
    """
    workspace, peer = await resolve_peer(_request_from_ctx(ctx))
    hits = await vector_search.query_messages(
        query_text=query,
        workspace_name=workspace,
        peer_name=peer,
        num_results=max(1, min(limit, 50)),
        hybrid=True,
    )
    return [
        {
            "public_id": h.public_id,
            "session_name": h.session_name,
            "content": h.content,
            "score": h.score,
        }
        for h in hits
    ]


@mcp.tool()
async def foreman_peer_card(ctx: Context) -> dict[str, Any]:
    """Return the synthesized facts Foreman has learned about the calling user.

    Reads from peer_cards for the OBO-derived peer in the bound workspace.
    Returns an empty list of facts if no card exists yet.
    """
    workspace, peer = await resolve_peer(_request_from_ctx(ctx))
    async with session() as s:
        row = (
            (
                await s.execute(
                    text(
                        "SELECT facts, updated_at FROM peer_cards "
                        "WHERE workspace_name = :w AND peer_name = :p"
                    ),
                    {"w": workspace, "p": peer},
                )
            )
            .mappings()
            .first()
        )
    if row is None:
        return {"peer_name": peer, "facts": [], "updated_at": None}
    return {
        "peer_name": peer,
        "facts": list(row["facts"] or []),
        "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
    }


@mcp.tool()
async def foreman_recent_sessions(ctx: Context, limit: int = 5) -> list[dict[str, Any]]:
    """List the calling user's recent sessions with their rolling short summary.

    Joins sessions and session_peers filtered to the OBO-derived peer; returns
    name + short_summary text (from sessions.internal_metadata) when present.
    """
    workspace, peer = await resolve_peer(_request_from_ctx(ctx))
    n = max(1, min(limit, 25))
    async with session() as s:
        rows = (
            (
                await s.execute(
                    text(
                        "SELECT s.name, s.created_at, "
                        "s.internal_metadata->'short_summary'->>'content' AS summary "
                        "FROM sessions s "
                        "JOIN session_peers sp "
                        "  ON sp.session_name = s.name AND sp.workspace_name = s.workspace_name "
                        "WHERE s.workspace_name = :w "
                        "  AND sp.peer_name = :p "
                        "  AND s.deleted_at IS NULL "
                        "ORDER BY s.created_at DESC "
                        "LIMIT :n"
                    ),
                    {"w": workspace, "p": peer, "n": n},
                )
            )
            .mappings()
            .all()
        )
    return [
        {
            "session_name": r["name"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "summary": r["summary"],
        }
        for r in rows
    ]
