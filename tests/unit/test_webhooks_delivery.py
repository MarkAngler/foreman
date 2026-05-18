"""Unit tests for webhook delivery primitives. No Spark, no network."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import httpx
import pytest

from foreman.jobs.webhooks.delivery import (
    EVENT_HEADER,
    MAX_ATTEMPTS,
    SIGNATURE_HEADER,
    build_headers,
    build_payload,
    classify_status,
    deliver,
    event_matches,
    serialize_body,
    should_retry,
    sign,
)


def test_build_payload_shape():
    occurred = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
    payload = build_payload(
        event_type="message.created",
        workspace_name="ws-a",
        data={"message_id": "m_123"},
        occurred_at=occurred,
    )
    assert payload == {
        "event_type": "message.created",
        "workspace_name": "ws-a",
        "occurred_at": "2026-05-15T12:00:00+00:00",
        "data": {"message_id": "m_123"},
    }


def test_build_payload_defaults_occurred_at_to_now_utc():
    payload = build_payload("test.event", "ws-a", {})
    parsed = datetime.fromisoformat(payload["occurred_at"])
    assert parsed.tzinfo is not None


def test_serialize_body_is_stable_under_key_order():
    a = {"event_type": "x", "workspace_name": "w", "data": {"b": 1, "a": 2}}
    b = {"data": {"a": 2, "b": 1}, "workspace_name": "w", "event_type": "x"}
    assert serialize_body(a) == serialize_body(b)


def test_sign_matches_independent_hmac():
    secret = "shh"
    body = b'{"hello":"world"}'
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert sign(body, secret) == expected


def test_build_headers_includes_signature_when_secret_present():
    body = b'{"x":1}'
    headers = build_headers("message.created", body, secret="s3cret")
    assert headers["Content-Type"] == "application/json"
    assert headers[EVENT_HEADER] == "message.created"
    assert headers[SIGNATURE_HEADER] == sign(body, "s3cret")


def test_build_headers_omits_signature_when_no_secret():
    headers = build_headers("message.created", b"{}", secret=None)
    assert SIGNATURE_HEADER not in headers


@pytest.mark.parametrize(
    ("status", "success", "retryable"),
    [
        (200, True, False),
        (201, True, False),
        (204, True, False),
        (299, True, False),
        (400, False, False),
        (401, False, False),
        (403, False, False),
        (404, False, False),
        (408, False, True),
        (422, False, False),
        (429, False, True),
        (500, False, True),
        (502, False, True),
        (503, False, True),
        (504, False, True),
    ],
)
def test_classify_status(status, success, retryable):
    assert classify_status(status) == (success, retryable)


def test_event_matches_empty_subscribes_to_all():
    assert event_matches(None, "any.event")
    assert event_matches([], "any.event")


def test_event_matches_specific_filter():
    assert event_matches(["message.created"], "message.created")
    assert not event_matches(["message.created"], "session.summarized")


def _mock_client_with_response(status_code: int) -> MagicMock:
    client = MagicMock(spec=httpx.Client)
    response = MagicMock()
    response.status_code = status_code
    client.post.return_value = response
    return client


def test_deliver_success_2xx():
    client = _mock_client_with_response(200)
    result = deliver("https://example.com/hook", b"{}", {}, client=client)
    assert result.success is True
    assert result.status_code == 200
    assert result.error is None
    assert result.retryable is False


def test_deliver_4xx_marks_failure_not_retryable():
    client = _mock_client_with_response(404)
    result = deliver("https://example.com/hook", b"{}", {}, client=client)
    assert result.success is False
    assert result.status_code == 404
    assert result.retryable is False
    assert result.error == "HTTP 404"


def test_deliver_5xx_marks_failure_retryable():
    client = _mock_client_with_response(503)
    result = deliver("https://example.com/hook", b"{}", {}, client=client)
    assert result.success is False
    assert result.retryable is True


def test_deliver_network_error_is_retryable():
    client = MagicMock(spec=httpx.Client)
    client.post.side_effect = httpx.ConnectError("connection refused")
    result = deliver("https://example.com/hook", b"{}", {}, client=client)
    assert result.success is False
    assert result.status_code is None
    assert result.retryable is True
    assert "ConnectError" in (result.error or "")


def test_deliver_passes_headers_and_body_to_client():
    client = _mock_client_with_response(200)
    headers = {"X-Test": "1", "Content-Type": "application/json"}
    body = b'{"a":1}'
    deliver("https://example.com/hook", body, headers, client=client)
    client.post.assert_called_once_with(
        "https://example.com/hook", content=body, headers=headers
    )


def test_should_retry_respects_max_attempts_and_retryability():
    from foreman.jobs.webhooks.delivery import DeliveryResult

    retryable = DeliveryResult(success=False, status_code=503, error="x", retryable=True)
    permanent = DeliveryResult(success=False, status_code=404, error="x", retryable=False)
    assert should_retry(0, retryable) is True
    assert should_retry(MAX_ATTEMPTS - 1, retryable) is True
    assert should_retry(MAX_ATTEMPTS, retryable) is False
    assert should_retry(0, permanent) is False


def test_signature_consumer_can_verify():
    """Document the verification path a downstream consumer would use."""
    secret = "topsecret"
    payload = build_payload("message.created", "ws-a", {"id": "m1"})
    body = serialize_body(payload)
    headers = build_headers("message.created", body, secret)

    sig = headers[SIGNATURE_HEADER]
    assert sig.startswith("sha256=")
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert hmac.compare_digest(sig, expected)

    # And the signed body is exactly what would be sent over the wire.
    assert json.loads(body)["event_type"] == "message.created"
