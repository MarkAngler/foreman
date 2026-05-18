"""Turn capture: shared by the `foreman_capture_turn` MCP tool and the Claude
Code Stop-hook CLI.

Workspace and peer attribution come from the foreman MCP server's env config
(`FOREMAN_DEFAULT_WORKSPACE`, `FOREMAN_USER_PEER`, `FOREMAN_ASSISTANT_PEER`).
The sibling-process CLI (run from a Stop hook) reads the same env block out of
`.mcp.json` so callers only ever configure one file. Failures soft-fail so a
foreman outage never blocks a turn.

CLI: `python -m foreman.mcp.capture` (reads Stop-hook payload from stdin).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from foreman.mcp.client import ForemanClient

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
MIN_ASSISTANT_CHARS_DEFAULT = 200
MAX_CONTENT_CHARS_DEFAULT = 100_000

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


# ---- Stop-hook CLI -----------------------------------------------------------


def _log(msg: str) -> None:
    sys.stderr.write(f"[foreman.capture] {msg}\n")


def _git(*args: str, default: str | None = None) -> str | None:
    try:
        out = subprocess.run(["git", *args], capture_output=True, text=True, timeout=2, check=False)
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
    return default


def _branch_or_sha() -> str:
    branch = _git("symbolic-ref", "--short", "HEAD")
    if branch:
        return branch
    sha = _git("rev-parse", "--short", "HEAD")
    return f"detached-{sha}" if sha else "main"


def _strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s)


def _truncate(s: str, max_chars: int) -> str:
    if len(s) <= max_chars:
        return s
    return s[:max_chars] + f"\n\n[truncated {len(s) - max_chars} chars]"


def _read_transcript(path: Path) -> list[dict]:
    entries: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def _is_real_user_turn(entry: dict) -> bool:
    if entry.get("type") != "user":
        return False
    msg = entry.get("message") or {}
    content = msg.get("content")
    if isinstance(content, str):
        return content.strip() != ""
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                return False
        return any(isinstance(b, dict) and b.get("type") == "text" for b in content)
    return False


def _extract_user_text(entry: dict) -> str:
    msg = entry.get("message") or {}
    content = msg.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"
        )
    return ""


def _extract_assistant_blocks(entries: list[dict]) -> tuple[str, list[str], bool]:
    text_parts: list[str] = []
    tool_names: list[str] = []
    committed = False
    for entry in entries:
        if entry.get("type") != "assistant":
            continue
        content = (entry.get("message") or {}).get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            btype = block.get("type")
            if btype == "text":
                text_parts.append(block.get("text", ""))
            elif btype == "tool_use":
                name = block.get("name", "")
                tool_names.append(name)
                if name == "Bash":
                    cmd = (block.get("input") or {}).get("command", "")
                    if isinstance(cmd, str) and "git commit" in cmd:
                        committed = True
    return "\n".join(text_parts).strip(), tool_names, committed


def _should_capture(
    user_text: str,
    assistant_text: str,
    tool_names: list[str],
    committed: bool,
    min_chars: int,
) -> bool:
    if committed or tool_names:
        return True
    if user_text.lstrip().startswith("/"):
        return True
    return len(assistant_text) >= min_chars


async def _cli_async() -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError) as e:
        _log(f"no/invalid stdin payload: {e}")
        return 0

    tp = payload.get("transcript_path")
    if not tp:
        _log("no transcript_path in payload")
        return 0
    path = Path(tp)
    if not path.exists():
        _log(f"transcript not found: {path}")
        return 0

    entries = _read_transcript(path)
    if not entries:
        return 0

    last_user_idx = next(
        (i for i in range(len(entries) - 1, -1, -1) if _is_real_user_turn(entries[i])),
        None,
    )
    if last_user_idx is None:
        return 0

    user_text = _extract_user_text(entries[last_user_idx]).strip()
    assistant_text, tool_names, committed = _extract_assistant_blocks(entries[last_user_idx + 1 :])
    assistant_text = assistant_text.strip()
    if not user_text or not assistant_text:
        return 0

    min_chars = int(_config("FOREMAN_MIN_ASSISTANT_CHARS") or MIN_ASSISTANT_CHARS_DEFAULT)
    max_chars = int(_config("FOREMAN_MAX_CONTENT_CHARS") or MAX_CONTENT_CHARS_DEFAULT)

    if not _should_capture(user_text, assistant_text, tool_names, committed, min_chars):
        return 0

    branch = _branch_or_sha()
    metadata = {
        "branch": branch,
        "commit": _git("rev-parse", "HEAD"),
        "tool_uses": tool_names,
        "git_committed": committed,
        "session_id": payload.get("session_id"),
    }
    try:
        await capture_turn(
            user_text=_truncate(_strip_ansi(user_text), max_chars),
            assistant_text=_truncate(_strip_ansi(assistant_text), max_chars),
            branch=branch,
            metadata=metadata,
        )
    except Exception as e:
        _log(f"capture failed (soft): {type(e).__name__}: {e}")
    return 0


def main() -> int:
    return asyncio.run(_cli_async())


if __name__ == "__main__":
    sys.exit(main())
