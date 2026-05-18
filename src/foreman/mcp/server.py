"""FastMCP server exposing foreman's REST surface as tools.

Ten coarse-grained tools — workspace/peer/session lifecycle, message create,
semantic search, session context, and the dialectic chat agent. Webhooks, key
issuance, and per-row CRUD are intentionally omitted; add later if needed.

Optional env: FOREMAN_DEFAULT_WORKSPACE lets tools fall back to a default
workspace when callers omit it.
"""

from __future__ import annotations

import os
from typing import Any, Literal

from mcp.server.fastmcp import FastMCP

from foreman.mcp.capture import capture_turn as _capture_turn
from foreman.mcp.client import ForemanClient

mcp = FastMCP("foreman")
_client = ForemanClient()


def _workspace(ws: str | None) -> str:
    resolved = ws or os.environ.get("FOREMAN_DEFAULT_WORKSPACE")
    if not resolved:
        raise ValueError("workspace_name is required (or set FOREMAN_DEFAULT_WORKSPACE)")
    return resolved


@mcp.tool()
async def foreman_list_workspaces(filters: dict | None = None) -> dict:
    """List foreman workspaces. Admin-only; workspace-scoped tokens cannot enumerate.

    Returns a paginated page of workspaces matching the optional metadata filter.
    """
    return await _client.list_workspaces(filters=filters)


@mcp.tool()
async def foreman_create_workspace(
    name: str,
    metadata: dict | None = None,
    configuration: dict | None = None,
) -> dict:
    """Get-or-create a workspace by name. Returns the workspace record."""
    return await _client.create_workspace(name=name, metadata=metadata, configuration=configuration)


@mcp.tool()
async def foreman_list_peers(
    workspace_name: str | None = None, filters: dict | None = None
) -> dict:
    """List peers in a workspace. Falls back to FOREMAN_DEFAULT_WORKSPACE if unset."""
    return await _client.list_peers(_workspace(workspace_name), filters=filters)


@mcp.tool()
async def foreman_create_peer(
    name: str,
    workspace_name: str | None = None,
    metadata: dict | None = None,
    configuration: dict | None = None,
) -> dict:
    """Get-or-create a peer in a workspace."""
    return await _client.create_peer(
        workspace_name=_workspace(workspace_name),
        name=name,
        metadata=metadata,
        configuration=configuration,
    )


@mcp.tool()
async def foreman_list_sessions(
    workspace_name: str | None = None, filters: dict | None = None
) -> dict:
    """List sessions in a workspace."""
    return await _client.list_sessions(_workspace(workspace_name), filters=filters)


@mcp.tool()
async def foreman_create_session(
    name: str,
    workspace_name: str | None = None,
    peers: dict[str, dict] | None = None,
    metadata: dict | None = None,
    configuration: dict | None = None,
) -> dict:
    """Get-or-create a session. Optional `peers` maps peer_name → {observe_others, observe_me}."""
    return await _client.create_session(
        workspace_name=_workspace(workspace_name),
        name=name,
        peers=peers,
        metadata=metadata,
        configuration=configuration,
    )


@mcp.tool()
async def foreman_send_messages(
    session_name: str,
    messages: list[dict],
    workspace_name: str | None = None,
) -> list[dict]:
    """Append messages to a session. Each message: {peer_name, content, metadata?, token_count?}.

    Messages are embedded and indexed inline so search results reflect them on
    the next request. Max 100 messages per batch.
    """
    return await _client.send_messages(
        workspace_name=_workspace(workspace_name),
        session_name=session_name,
        messages=messages,
    )


@mcp.tool()
async def foreman_search(
    query: str,
    workspace_name: str | None = None,
    peer_name: str | None = None,
    session_name: str | None = None,
    limit: int = 10,
) -> list[dict]:
    """Hybrid semantic + lexical search over messages in a workspace.

    Optional peer_name / session_name narrow the scope. Returns up to `limit`
    MessageHit records with public_id + score.
    """
    return await _client.search_workspace(
        workspace_name=_workspace(workspace_name),
        query=query,
        peer_name=peer_name,
        session_name=session_name,
        limit=limit,
    )


@mcp.tool()
async def foreman_session_context(
    session_name: str,
    workspace_name: str | None = None,
    tokens: int = 4000,
    include_summary: bool = True,
) -> dict:
    """Return a prompt-ready window for a session: recent messages + optional rolling summary.

    `tokens` budgets the total response size. `include_summary` toggles whether
    the latest short summary is prepended (summary tokens count against budget).
    """
    return await _client.session_context(
        workspace_name=_workspace(workspace_name),
        session_name=session_name,
        tokens=tokens,
        include_summary=include_summary,
    )


@mcp.tool()
async def foreman_capture_turn(
    user_text: str,
    assistant_text: str,
    branch: str,
    metadata: dict | None = None,
) -> dict:
    """Append a user+assistant turn to a branch-scoped session.

    Workspace and peer attribution come from server env: FOREMAN_DEFAULT_WORKSPACE,
    FOREMAN_USER_PEER, FOREMAN_ASSISTANT_PEER. Session name is derived from
    `branch` (slash → dash, lowercased). Get-or-creates the session.
    """
    return await _capture_turn(
        user_text=user_text,
        assistant_text=assistant_text,
        branch=branch,
        metadata=metadata,
        client=_client,
    )


@mcp.tool()
async def foreman_chat(
    peer_name: str,
    query: str,
    workspace_name: str | None = None,
    session_name: str | None = None,
    reasoning_level: Literal["minimal", "low", "medium", "high", "max"] = "low",
    include_session_history: bool = False,
    session_history_max_tokens: int = 2000,
) -> dict[str, Any]:
    """Invoke the foreman dialectic agent for a peer; returns buffered SSE result.

    The agent reasons over the peer's accumulated knowledge (and optional
    session history) to answer `query`. Response shape:
        {"text": str, "events": [{"event": str, "data": Any}, ...]}
    `text` is the concatenated assistant output; `events` is the raw SSE stream
    for inspection.
    """
    return await _client.chat(
        workspace_name=_workspace(workspace_name),
        peer_name=peer_name,
        query=query,
        session_name=session_name,
        reasoning_level=reasoning_level,
        include_session_history=include_session_history,
        session_history_max_tokens=session_history_max_tokens,
    )
