"""Integration test for webhook delivery using a real httpx MockTransport.

Skipped in `pytest tests/unit` runs. Run with:
    PYTHONPATH=src .venv/Scripts/python.exe -m pytest tests/integration/ -m integration
"""

from __future__ import annotations

import hashlib
import hmac
import json

import httpx
import pytest

from foreman.jobs.webhooks.delivery import (
    EVENT_HEADER,
    SIGNATURE_HEADER,
    build_headers,
    build_payload,
    deliver,
    serialize_body,
)

pytestmark = pytest.mark.integration


def test_signed_post_round_trip_through_mock_transport():
    secret = "integration-secret"
    received: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received["url"] = str(request.url)
        received["headers"] = dict(request.headers)
        received["body"] = request.content
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)

    payload = build_payload(
        event_type="message.created",
        workspace_name="ws-integration",
        data={"message_id": "m_xyz"},
    )
    body = serialize_body(payload)
    headers = build_headers("message.created", body, secret)

    result = deliver("https://hook.example.com/incoming", body, headers, client=client)

    assert result.success is True
    assert result.status_code == 200
    assert received["url"] == "https://hook.example.com/incoming"
    assert received["body"] == body
    assert received["headers"][EVENT_HEADER.lower()] == "message.created"

    sig = received["headers"][SIGNATURE_HEADER.lower()]
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert hmac.compare_digest(sig, expected)

    decoded = json.loads(received["body"])
    assert decoded["event_type"] == "message.created"
    assert decoded["workspace_name"] == "ws-integration"
    assert decoded["data"] == {"message_id": "m_xyz"}


def test_5xx_response_classified_as_retryable():
    transport = httpx.MockTransport(lambda req: httpx.Response(503))
    client = httpx.Client(transport=transport)

    result = deliver("https://hook.example.com/x", b"{}", {"Content-Type": "application/json"}, client=client)

    assert result.success is False
    assert result.status_code == 503
    assert result.retryable is True


def test_4xx_response_classified_as_permanent():
    transport = httpx.MockTransport(lambda req: httpx.Response(401))
    client = httpx.Client(transport=transport)

    result = deliver("https://hook.example.com/x", b"{}", {}, client=client)

    assert result.success is False
    assert result.status_code == 401
    assert result.retryable is False
