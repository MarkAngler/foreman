"""Unit tests for ForemanClient header bridging and redirect handling.

Regression coverage for FOR-002 (Databricks Apps OAuth proxy needs the
Databricks Bearer in `Authorization` while the foreman JWT rides in
`X-Foreman-Token`) and FOR-003 (3xx must not be silently swallowed as
None — either the redirect is followed or it raises with the Location).
"""

from __future__ import annotations

import httpx
import pytest

from foreman.mcp.auth import Credentials
from foreman.mcp.client import ForemanClient


def _creds_with_databricks_bearer(databricks_token: str = "dbx-oauth") -> Credentials:
    return Credentials(
        base_url="http://stub",
        _provider=lambda: "foreman-jwt",
        _databricks_headers=lambda: {"Authorization": f"Bearer {databricks_token}"},
    )


def _creds_pure_foreman() -> Credentials:
    return Credentials(
        base_url="http://stub",
        _provider=lambda: "foreman-jwt",
    )


async def test_databricks_bearer_in_authorization_foreman_jwt_in_custom_header():
    seen: dict[str, str] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["authorization"] = req.headers.get("authorization", "")
        seen["x_foreman_token"] = req.headers.get("x-foreman-token", "")
        return httpx.Response(200, json={"items": []})

    transport = httpx.MockTransport(handler)
    client = ForemanClient(credentials=_creds_with_databricks_bearer(), transport=transport)
    await client.list_workspaces()

    assert seen["authorization"] == "Bearer dbx-oauth"
    assert seen["x_foreman_token"] == "foreman-jwt"


async def test_pure_foreman_mode_carries_jwt_in_authorization():
    seen: dict[str, str] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["authorization"] = req.headers.get("authorization", "")
        seen["x_foreman_token"] = req.headers.get("x-foreman-token", "")
        return httpx.Response(200, json={"items": []})

    transport = httpx.MockTransport(handler)
    client = ForemanClient(credentials=_creds_pure_foreman(), transport=transport)
    await client.list_workspaces()

    # When no Databricks proxy headers are available the foreman JWT also
    # rides in Authorization so the legacy / in-process auth path keeps working.
    assert seen["authorization"] == "Bearer foreman-jwt"
    assert seen["x_foreman_token"] == "foreman-jwt"


async def test_follow_redirects_lands_on_final_response():
    hops: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        hops.append(req.url.path)
        if req.url.path == "/workspaces/list":
            return httpx.Response(307, headers={"location": "http://stub/workspaces/list/v2"})
        return httpx.Response(200, json={"items": ["ok"]})

    transport = httpx.MockTransport(handler)
    client = ForemanClient(credentials=_creds_pure_foreman(), transport=transport)
    out = await client.list_workspaces()

    assert hops == ["/workspaces/list", "/workspaces/list/v2"]
    assert out == {"items": ["ok"]}


async def test_unfollowed_3xx_raises_with_location():
    # Simulate the Databricks Apps OAuth proxy 302 by short-circuiting the
    # transport before the follow can complete. httpx still treats the
    # response as redirectable, but if the redirect target itself returns 3xx
    # without a Location we want a clear RuntimeError, not a silent None.
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": ""})

    transport = httpx.MockTransport(handler)
    client = ForemanClient(credentials=_creds_pure_foreman(), transport=transport)

    with pytest.raises((RuntimeError, httpx.HTTPError)):
        await client.list_workspaces()
