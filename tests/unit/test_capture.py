from unittest.mock import AsyncMock

import pytest

from foreman.mcp import capture


def test_normalize_session_lowercases_and_dashes():
    assert capture.normalize_session("feature/Foo Bar") == "feature-foo-bar"
    assert capture.normalize_session("main") == "main"


def test_require_raises_when_missing(monkeypatch):
    monkeypatch.delenv("FOREMAN_DEFAULT_WORKSPACE", raising=False)
    monkeypatch.setattr(capture, "_mcp_env_cache", {}, raising=False)
    with pytest.raises(RuntimeError, match="FOREMAN_DEFAULT_WORKSPACE"):
        capture._require("FOREMAN_DEFAULT_WORKSPACE")


def test_config_prefers_process_env_over_mcp_json(monkeypatch):
    monkeypatch.setenv("FOREMAN_DEFAULT_WORKSPACE", "from-env")
    monkeypatch.setattr(capture, "_mcp_env_cache", {"FOREMAN_DEFAULT_WORKSPACE": "from-mcp"}, raising=False)
    assert capture._config("FOREMAN_DEFAULT_WORKSPACE") == "from-env"


def test_config_falls_back_to_mcp_json(monkeypatch):
    monkeypatch.delenv("FOREMAN_DEFAULT_WORKSPACE", raising=False)
    monkeypatch.setattr(capture, "_mcp_env_cache", {"FOREMAN_DEFAULT_WORKSPACE": "from-mcp"}, raising=False)
    assert capture._config("FOREMAN_DEFAULT_WORKSPACE") == "from-mcp"


@pytest.mark.asyncio
async def test_capture_turn_creates_session_and_sends_two_messages(monkeypatch):
    monkeypatch.setenv("FOREMAN_DEFAULT_WORKSPACE", "ws-a")
    monkeypatch.setenv("FOREMAN_USER_PEER", "mark")
    monkeypatch.setenv("FOREMAN_ASSISTANT_PEER", "claude")

    fake_client = AsyncMock()
    fake_client.create_session.return_value = {"name": "feature-x"}
    fake_client.send_messages.return_value = [{"public_id": "m1"}, {"public_id": "m2"}]

    result = await capture.capture_turn(
        user_text="hello",
        assistant_text="hi back",
        branch="feature/X",
        metadata={"k": "v"},
        client=fake_client,
    )

    fake_client.create_session.assert_awaited_once_with(
        workspace_name="ws-a",
        name="feature-x",
        peers={"mark": {}, "claude": {}},
    )
    send_kwargs = fake_client.send_messages.await_args.kwargs
    assert send_kwargs["workspace_name"] == "ws-a"
    assert send_kwargs["session_name"] == "feature-x"
    assert send_kwargs["messages"] == [
        {"peer_name": "mark", "content": "hello", "metadata": {"k": "v"}},
        {"peer_name": "claude", "content": "hi back", "metadata": {"k": "v"}},
    ]
    assert result["workspace"] == "ws-a"
    assert result["session_name"] == "feature-x"
    assert len(result["messages"]) == 2
