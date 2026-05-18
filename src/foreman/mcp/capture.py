"""Turn capture: the shared implementation behind the `foreman_capture_turn`
MCP tool.

Workspace and peer attribution come from the foreman MCP server's env
config (`FOREMAN_DEFAULT_WORKSPACE`, `FOREMAN_USER_PEER`,
`FOREMAN_ASSISTANT_PEER`). Callers may set those in the process env, or rely
on the `mcpServers.foreman.env` block of a discoverable `.mcp.json`.

Capture is agent-driven: any agent decides when to call the MCP tool. There
is no transcript-parsing or host-specific hook in this module.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from foreman.mcp.client import ForemanClient

_mcp_env_cache: dict[str, str] | None = None


def _load_mcp_foreman_env() -> dict[str, str]:
    for root in (Path.cwd(), *Path(__file__).resolve().parents):
        mcp_path = root / ".mcp.json"
        if not mcp_path.exists():
            continue
        try:
            data = json.loads(mcp_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        env = data.get("mcpServers", {}).get("foreman", {}).get("env") or {}
        if env:
            return env
    return {}


def _config(key: str) -> str | None:
    val = os.environ.get(key)
    if val:
        return val
    global _mcp_env_cache
    if _mcp_env_cache is None:
        _mcp_env_cache = _load_mcp_foreman_env()
    return _mcp_env_cache.get(key)


def _require(key: str) -> str:
    val = _config(key)
    if not val:
        raise RuntimeError(f"{key} not set (mcpServers.foreman.env in .mcp.json, or process env)")
    return val


def normalize_session(branch: str) -> str:
    return branch.replace("/", "-").replace(" ", "-").lower()


async def capture_turn(
    user_text: str,
    assistant_text: str,
    branch: str,
    metadata: dict[str, Any] | None = None,
    client: ForemanClient | None = None,
) -> dict[str, Any]:
    workspace = _require("FOREMAN_DEFAULT_WORKSPACE")
    user_peer = _require("FOREMAN_USER_PEER")
    assistant_peer = _require("FOREMAN_ASSISTANT_PEER")
    session_name = normalize_session(branch)

    c = client or ForemanClient()
    await c.create_session(
        workspace_name=workspace,
        name=session_name,
        peers={user_peer: {}, assistant_peer: {}},
    )
    msgs = await c.send_messages(
        workspace_name=workspace,
        session_name=session_name,
        messages=[
            {"peer_name": user_peer, "content": user_text, "metadata": metadata or {}},
            {"peer_name": assistant_peer, "content": assistant_text, "metadata": metadata or {}},
        ],
    )
    return {"workspace": workspace, "session_name": session_name, "messages": msgs}
