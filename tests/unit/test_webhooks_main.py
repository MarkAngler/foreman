"""Unit tests for the webhooks job orchestrator. Spark and httpx are mocked."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import httpx
import pytest

from foreman.jobs.webhooks import main
from foreman.jobs.webhooks.delivery import MAX_ATTEMPTS, SIGNATURE_HEADER


def _client_returning(status_code: int) -> MagicMock:
    client = MagicMock(spec=httpx.Client)
    response = MagicMock()
    response.status_code = status_code
    client.post.return_value = response
    return client


def _endpoint(
    id: str = "ep_1",
    workspace_name: str = "ws-a",
    url: str = "https://example.com/hook",
    secret: str | None = "shh",
    event_types: list[str] | None = None,
) -> dict:
    return {
        "id": id,
        "workspace_name": workspace_name,
        "url": url,
        "secret": secret,
        "event_types": event_types,
    }


def _item(item_id: int, payload: dict) -> dict:
    return {"id": item_id, "payload": json.dumps(payload), "created_at": None}


def test_parse_payload_handles_dict_string_and_none():
    assert main._parse_payload(None) == {}
    assert main._parse_payload({"a": 1}) == {"a": 1}
    assert main._parse_payload('{"a":1}') == {"a": 1}


def test_group_endpoints_buckets_by_workspace():
    rows = [
        MagicMock(asDict=lambda: _endpoint("ep_1", workspace_name="ws-a")),
        MagicMock(asDict=lambda: _endpoint("ep_2", workspace_name="ws-b")),
        MagicMock(asDict=lambda: _endpoint("ep_3", workspace_name="ws-a")),
    ]
    grouped = main._group_endpoints(rows)
    assert set(grouped.keys()) == {"ws-a", "ws-b"}
    assert {e["id"] for e in grouped["ws-a"]} == {"ep_1", "ep_3"}


def test_process_item_skips_when_payload_missing_required_fields():
    client = _client_returning(200)
    rows = main._process_item(
        item={"id": 1, "payload": json.dumps({"data": {}})},
        endpoints_by_workspace={"ws-a": [_endpoint()]},
        prior_attempts={},
        client=client,
    )
    assert rows == []
    client.post.assert_not_called()


def test_process_item_filters_by_event_type():
    client = _client_returning(200)
    item = _item(1, {"event_type": "session.summarized", "workspace_name": "ws-a", "data": {}})
    endpoints = {
        "ws-a": [
            _endpoint("ep_1", event_types=["message.created"]),
            _endpoint("ep_2", event_types=["session.summarized"]),
        ]
    }
    rows = main._process_item(item, endpoints, {}, client)
    assert [r["endpoint_id"] for r in rows] == ["ep_2"]
    assert client.post.call_count == 1


def test_process_item_no_endpoints_for_workspace_yields_no_rows():
    client = _client_returning(200)
    item = _item(1, {"event_type": "x", "workspace_name": "ws-other", "data": {}})
    rows = main._process_item(item, {"ws-a": [_endpoint()]}, {}, client)
    assert rows == []
    client.post.assert_not_called()


def test_process_item_records_attempt_count_from_prior_attempts():
    client = _client_returning(500)
    item = _item(42, {"event_type": "x", "workspace_name": "ws-a", "data": {}})
    endpoints = {"ws-a": [_endpoint("ep_1")]}
    rows = main._process_item(item, endpoints, prior_attempts={(42, "ep_1"): 2}, client=client)
    assert len(rows) == 1
    assert rows[0]["attempt_count"] == 3
    assert rows[0]["success"] is False
    assert rows[0]["retryable"] is True
    assert rows[0]["status_code"] == 500


def test_process_item_skips_endpoint_at_max_attempts():
    client = _client_returning(200)
    item = _item(7, {"event_type": "x", "workspace_name": "ws-a", "data": {}})
    endpoints = {"ws-a": [_endpoint("ep_1")]}
    rows = main._process_item(
        item, endpoints, prior_attempts={(7, "ep_1"): MAX_ATTEMPTS}, client=client
    )
    assert rows == []
    client.post.assert_not_called()


def test_process_item_signs_body_with_endpoint_secret():
    client = _client_returning(200)
    item = _item(1, {"event_type": "message.created", "workspace_name": "ws-a", "data": {"x": 1}})
    endpoints = {"ws-a": [_endpoint("ep_1", secret="rotated-key")]}
    main._process_item(item, endpoints, {}, client)
    _, kwargs = client.post.call_args
    assert SIGNATURE_HEADER in kwargs["headers"]
    assert kwargs["headers"][SIGNATURE_HEADER].startswith("sha256=")


def test_process_item_omits_signature_when_endpoint_has_no_secret():
    client = _client_returning(200)
    item = _item(1, {"event_type": "message.created", "workspace_name": "ws-a", "data": {}})
    endpoints = {"ws-a": [_endpoint("ep_1", secret=None)]}
    main._process_item(item, endpoints, {}, client)
    _, kwargs = client.post.call_args
    assert SIGNATURE_HEADER not in kwargs["headers"]


def test_process_item_records_success_and_failure_per_endpoint():
    successful_response = MagicMock()
    successful_response.status_code = 202
    failing_response = MagicMock()
    failing_response.status_code = 500

    client = MagicMock(spec=httpx.Client)
    client.post.side_effect = [successful_response, failing_response]

    item = _item(1, {"event_type": "message.created", "workspace_name": "ws-a", "data": {}})
    endpoints = {
        "ws-a": [
            _endpoint("ep_ok", url="https://ok.example.com/hook"),
            _endpoint("ep_fail", url="https://fail.example.com/hook"),
        ]
    }
    rows = main._process_item(item, endpoints, {}, client)
    by_id = {r["endpoint_id"]: r for r in rows}
    assert by_id["ep_ok"]["success"] is True
    assert by_id["ep_ok"]["status_code"] == 202
    assert by_id["ep_fail"]["success"] is False
    assert by_id["ep_fail"]["retryable"] is True


def test_process_item_uses_payload_data_field():
    client = _client_returning(200)
    payload_data = {"message_id": "m_42", "session_name": "s1"}
    item = _item(
        1,
        {"event_type": "message.created", "workspace_name": "ws-a", "data": payload_data},
    )
    endpoints = {"ws-a": [_endpoint("ep_1", secret=None)]}
    main._process_item(item, endpoints, {}, client)
    _, kwargs = client.post.call_args
    sent = json.loads(kwargs["content"])
    assert sent["data"] == payload_data
    assert sent["event_type"] == "message.created"
    assert sent["workspace_name"] == "ws-a"


def test_module_imports_cleanly():
    # Acceptance gate: `from foreman.jobs.webhooks.main import main` must work.
    from foreman.jobs.webhooks.main import main as entrypoint

    assert callable(entrypoint)


def test_unparseable_iso_timestamp_raises():
    # Surface bad payloads loudly rather than silently dropping the timestamp.
    client = _client_returning(200)
    item = _item(
        1,
        {
            "event_type": "x",
            "workspace_name": "ws-a",
            "data": {},
            "occurred_at": "not-a-timestamp",
        },
    )
    endpoints = {"ws-a": [_endpoint("ep_1")]}
    with pytest.raises(ValueError):
        main._process_item(item, endpoints, {}, client)
