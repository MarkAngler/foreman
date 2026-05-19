"""Unit tests for the OBO identity resolver used by the Genie-facing MCP."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import patch

import httpx
import pytest
from fastapi import HTTPException
from starlette.requests import Request

from foreman.mcp_http import identity


def _request_with(headers: dict[str, str]) -> Request:
    raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    scope = {
        "type": "http",
        "headers": raw,
        "method": "POST",
        "path": "/",
        "query_string": b"",
    }
    return Request(scope)


@contextmanager
def _scim_transport(handler):
    orig = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return orig(*args, **kwargs)

    with patch.object(httpx, "AsyncClient", factory):
        yield


@pytest.fixture(autouse=True)
def _clear_cache():
    identity._email_cache.clear()
    yield
    identity._email_cache.clear()


@pytest.fixture(autouse=True)
def _stub_host():
    with patch.object(
        identity, "_workspace_host", return_value="https://example.cloud.databricks.com"
    ):
        yield


def test_peer_name_for_lowercases_and_substitutes():
    assert identity.peer_name_for("Mark.Angler@outlook.com") == "mark-angler-at-outlook-com"


def test_peer_name_for_collapses_unsafe_chars():
    assert identity.peer_name_for("a+b@c.d") == "a-b-at-c-d"


def test_peer_name_for_truncates_and_strips_edges():
    long = "a" * 300 + "@e.f"
    derived = identity.peer_name_for(long)
    assert len(derived) <= 256
    assert not derived.startswith("-") and not derived.endswith("-")


def test_peer_name_for_rejects_empty():
    with pytest.raises(ValueError):
        identity.peer_name_for("...")


async def test_missing_header_returns_401():
    req = _request_with({})
    with pytest.raises(HTTPException) as exc:
        await identity.current_user_email(req)
    assert exc.value.status_code == 401


async def test_scim_rejection_surfaces_as_401():
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"detail": "nope"})

    with _scim_transport(handler):
        req = _request_with({identity.HEADER_NAME: "tok"})
        with pytest.raises(HTTPException) as exc:
            await identity.current_user_email(req)
    assert exc.value.status_code == 401


async def test_scim_success_extracts_first_email():
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.headers["Authorization"] == "Bearer tok"
        assert req.url.path == "/api/2.0/preview/scim/v2/Me"
        return httpx.Response(
            200,
            json={
                "userName": "ignored",
                "emails": [{"value": "alice@example.com", "primary": True}],
            },
        )

    with _scim_transport(handler):
        req = _request_with({identity.HEADER_NAME: "tok"})
        email = await identity.current_user_email(req)
    assert email == "alice@example.com"


async def test_scim_fallback_to_username_when_no_email():
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"userName": "svc-account", "emails": []})

    with _scim_transport(handler):
        req = _request_with({identity.HEADER_NAME: "tok"})
        email = await identity.current_user_email(req)
    assert email == "svc-account"


async def test_cache_hit_skips_second_scim_call():
    calls = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json={"emails": [{"value": "bob@example.com"}]})

    with _scim_transport(handler):
        req = _request_with({identity.HEADER_NAME: "tok"})
        a = await identity.current_user_email(req)
        b = await identity.current_user_email(req)
    assert a == b == "bob@example.com"
    assert calls["n"] == 1
