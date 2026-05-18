"""Webhook delivery primitives.

Pure functions: payload assembly, HMAC-SHA256 signing, retry classification, and
a single synchronous httpx POST attempt. Kept free of Spark / Lakebase imports
so they're trivially unit-testable and reusable from the orchestrator.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

import httpx

SIGNATURE_HEADER = "X-Foreman-Signature"
EVENT_HEADER = "X-Foreman-Event"
DEFAULT_TIMEOUT_SECONDS = 30.0
MAX_ATTEMPTS = 5


@dataclass(frozen=True)
class DeliveryResult:
    success: bool
    status_code: int | None
    error: str | None
    retryable: bool


def build_payload(
    event_type: str,
    workspace_name: str,
    data: Mapping[str, Any],
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    ts = (occurred_at or datetime.now(tz=timezone.utc)).isoformat()
    return {
        "event_type": event_type,
        "workspace_name": workspace_name,
        "occurred_at": ts,
        "data": dict(data),
    }


def serialize_body(payload: Mapping[str, Any]) -> bytes:
    # Stable serialization: sort keys + no whitespace so signature is reproducible
    # by consumers regardless of dict insertion order.
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign(body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def event_matches(endpoint_event_types: list[str] | None, event_type: str) -> bool:
    # Empty / missing event_types means "subscribe to everything" — matches honcho's behavior.
    if not endpoint_event_types:
        return True
    return event_type in endpoint_event_types


def classify_status(status_code: int) -> tuple[bool, bool]:
    """Return (success, retryable) for an HTTP status code.

    2xx → success. 408 / 429 → retryable (timeout / rate-limited). Other 4xx →
    permanent failure (caller error, no point retrying). 5xx → retryable.
    """
    if 200 <= status_code < 300:
        return True, False
    if status_code in (408, 429):
        return False, True
    if 400 <= status_code < 500:
        return False, False
    return False, True


def deliver(
    url: str,
    body: bytes,
    headers: Mapping[str, str],
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    client: httpx.Client | None = None,
) -> DeliveryResult:
    """POST a single webhook attempt. Returns delivery outcome — never raises."""
    own_client = client is None
    client = client or httpx.Client(timeout=timeout)
    try:
        response = client.post(url, content=body, headers=dict(headers))
        success, retryable = classify_status(response.status_code)
        error = None if success else f"HTTP {response.status_code}"
        return DeliveryResult(
            success=success,
            status_code=response.status_code,
            error=error,
            retryable=retryable,
        )
    except httpx.HTTPError as exc:
        return DeliveryResult(
            success=False,
            status_code=None,
            error=f"{type(exc).__name__}: {exc}",
            retryable=True,
        )
    finally:
        if own_client:
            client.close()


def build_headers(event_type: str, body: bytes, secret: str | None) -> dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        EVENT_HEADER: event_type,
    }
    if secret:
        headers[SIGNATURE_HEADER] = sign(body, secret)
    return headers


def should_retry(attempt_count: int, result: DeliveryResult) -> bool:
    return result.retryable and attempt_count < MAX_ATTEMPTS
