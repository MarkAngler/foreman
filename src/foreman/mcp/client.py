"""Async HTTP client wrapping the foreman REST API for MCP tool use.

Tools delegate here so server.py stays declarative. Each method returns parsed
JSON (or a buffered SSE result for chat). HTTPStatusError surfaces with the
response body as a RuntimeError so MCP clients see actionable messages.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from foreman.mcp.auth import Credentials, resolve_credentials

DEFAULT_TIMEOUT = httpx.Timeout(30.0, read=120.0)
CHAT_TIMEOUT = httpx.Timeout(60.0, read=600.0)


class ForemanClient:
    def __init__(
        self,
        credentials: Credentials | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self._credentials = credentials
        self._transport = transport

    def _creds(self) -> Credentials:
        if self._credentials is None:
            self._credentials = resolve_credentials()
        return self._credentials

    def _headers(self) -> dict[str, str]:
        creds = self._creds()
        # Databricks Apps OAuth proxy gates on Authorization; when present, use
        # its Bearer there and shuttle the foreman JWT under X-Foreman-Token.
        # In-process tests / pure-foreman deployments pass an empty dict here
        # and fall back to Authorization carrying the foreman JWT directly.
        databricks_headers = creds.databricks_auth_headers()
        foreman_jwt = creds.bearer_token()
        headers: dict[str, str] = {
            "X-Foreman-Token": foreman_jwt,
            "Content-Type": "application/json",
        }
        if databricks_headers:
            headers.update(databricks_headers)
        else:
            headers["Authorization"] = f"Bearer {foreman_jwt}"
        return headers

    def _client(self, timeout: httpx.Timeout = DEFAULT_TIMEOUT) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._creds().base_url,
            timeout=timeout,
            transport=self._transport,
            follow_redirects=True,
        )

    async def _request(self, method: str, path: str, **kwargs) -> Any:
        async with self._client() as c:
            resp = await c.request(method, path, headers=self._headers(), **kwargs)
        _raise_for_status(resp)
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

    # ---- Workspaces ----------------------------------------------------------

    async def list_workspaces(self, filters: dict | None = None) -> dict:
        return await self._request(
            "POST", "/workspaces/list", json={"filters": filters} if filters else {}
        )

    async def create_workspace(
        self,
        name: str,
        metadata: dict | None = None,
        configuration: dict | None = None,
    ) -> dict:
        body = {"name": name, "metadata": metadata or {}, "configuration": configuration or {}}
        return await self._request("POST", "/workspaces", json=body)

    async def search_workspace(
        self,
        workspace_name: str,
        query: str,
        peer_name: str | None = None,
        session_name: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        body: dict[str, Any] = {"query": query, "limit": limit}
        if peer_name:
            body["peer_name"] = peer_name
        if session_name:
            body["session_name"] = session_name
        return await self._request("POST", f"/workspaces/{workspace_name}/search", json=body)

    # ---- Peers ---------------------------------------------------------------

    async def list_peers(self, workspace_name: str, filters: dict | None = None) -> dict:
        return await self._request(
            "POST",
            f"/workspaces/{workspace_name}/peers/list",
            json={"filters": filters} if filters else {},
        )

    async def create_peer(
        self,
        workspace_name: str,
        name: str,
        metadata: dict | None = None,
        configuration: dict | None = None,
    ) -> dict:
        body = {"name": name, "metadata": metadata or {}, "configuration": configuration or {}}
        return await self._request("POST", f"/workspaces/{workspace_name}/peers", json=body)

    # ---- Sessions ------------------------------------------------------------

    async def list_sessions(self, workspace_name: str, filters: dict | None = None) -> dict:
        return await self._request(
            "POST",
            f"/workspaces/{workspace_name}/sessions/list",
            json={"filters": filters} if filters else {},
        )

    async def create_session(
        self,
        workspace_name: str,
        name: str,
        peers: dict[str, dict] | None = None,
        metadata: dict | None = None,
        configuration: dict | None = None,
    ) -> dict:
        body: dict[str, Any] = {
            "name": name,
            "metadata": metadata or {},
            "configuration": configuration or {},
        }
        if peers:
            body["peers"] = peers
        return await self._request("POST", f"/workspaces/{workspace_name}/sessions", json=body)

    async def session_context(
        self,
        workspace_name: str,
        session_name: str,
        tokens: int = 4000,
        include_summary: bool = True,
    ) -> dict:
        params = {"tokens": tokens, "summary": "true" if include_summary else "false"}
        return await self._request(
            "GET",
            f"/workspaces/{workspace_name}/sessions/{session_name}/context",
            params=params,
        )

    async def session_summaries(self, workspace_name: str, session_name: str) -> dict:
        return await self._request(
            "GET", f"/workspaces/{workspace_name}/sessions/{session_name}/summaries"
        )

    # ---- Messages ------------------------------------------------------------

    async def send_messages(
        self,
        workspace_name: str,
        session_name: str,
        messages: list[dict],
    ) -> list[dict]:
        return await self._request(
            "POST",
            f"/workspaces/{workspace_name}/sessions/{session_name}/messages",
            json={"messages": messages},
        )

    # ---- Chat (SSE) ----------------------------------------------------------

    async def chat(
        self,
        workspace_name: str,
        peer_name: str,
        query: str,
        session_name: str | None = None,
        reasoning_level: str = "low",
        include_session_history: bool = False,
        session_history_max_tokens: int = 2000,
    ) -> dict:
        body: dict[str, Any] = {
            "query": query,
            "reasoning_level": reasoning_level,
            "include_session_history": include_session_history,
            "session_history_max_tokens": session_history_max_tokens,
        }
        if session_name:
            body["session_name"] = session_name

        events: list[dict] = []
        text_parts: list[str] = []
        path = f"/workspaces/{workspace_name}/peers/{peer_name}/chat"

        async with self._client(CHAT_TIMEOUT) as c:
            async with c.stream(
                "POST",
                path,
                headers={**self._headers(), "Accept": "text/event-stream"},
                json=body,
            ) as resp:
                if resp.status_code >= 400:
                    body_text = (await resp.aread()).decode("utf-8", errors="replace")
                    raise RuntimeError(f"foreman chat returned {resp.status_code}: {body_text}")
                async for event in _aiter_sse(resp):
                    events.append(event)
                    chunk = _extract_text(event)
                    if chunk:
                        text_parts.append(chunk)

        return {"text": "".join(text_parts), "events": events}


def _raise_for_status(resp: httpx.Response) -> None:
    if 300 <= resp.status_code < 400:
        # follow_redirects=True normally absorbs these; if one leaks (e.g. tests
        # disable redirects) fail loudly with the Location so the proxy hop is
        # visible instead of swallowed as an empty 3xx body.
        location = resp.headers.get("location", "<no Location header>")
        raise RuntimeError(
            f"foreman {resp.request.method} {resp.request.url.path} returned "
            f"{resp.status_code} redirect to {location}"
        )
    if resp.status_code < 400:
        return
    body = resp.text
    raise RuntimeError(
        f"foreman {resp.request.method} {resp.request.url.path} returned {resp.status_code}: {body}"
    )


async def _aiter_sse(resp: httpx.Response):
    event_name: str | None = None
    data_lines: list[str] = []
    async for line in resp.aiter_lines():
        if line == "":
            if data_lines:
                payload = "\n".join(data_lines)
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    data = payload
                yield {"event": event_name or "message", "data": data}
            event_name = None
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event_name = line[len("event:") :].strip()
        elif line.startswith("data:"):
            data_lines.append(line[len("data:") :].lstrip())


def _extract_text(event: dict) -> str:
    data = event.get("data")
    if not isinstance(data, dict):
        return ""
    for key in ("delta", "text", "content", "chunk"):
        v = data.get(key)
        if isinstance(v, str):
            return v
    return ""
