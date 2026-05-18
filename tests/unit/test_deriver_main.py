"""Unit tests for deriver main: LLM response extraction and batch orchestration.

Spark is mocked at the module boundary — we exercise the orchestration logic
(observer fan-out + LLM dispatch + draft assembly) without spinning up a real
SparkSession. The Spark-touching helpers (`fetch_new_messages`,
`build_doc_dataframe`, etc.) are exercised by the integration test.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from foreman.jobs.deriver import main as deriver_main
from foreman.jobs.deriver.observer_observed import MessageRow


def _msg(id_: int, peer: str, session: str = "s1", workspace: str = "ws") -> MessageRow:
    return MessageRow(
        id=id_,
        public_id=f"pub-{id_}",
        workspace_name=workspace,
        session_name=session,
        peer_name=peer,
        content=f"hi from {peer} #{id_}",
        created_at=datetime(2026, 1, 1, 12, 0, id_, tzinfo=timezone.utc),
    )


# ---- _extract_text ----------------------------------------------------------


def test_extract_text_handles_plain_string():
    assert deriver_main._extract_text("hello") == "hello"


def test_extract_text_handles_openai_style_dict():
    response = {"choices": [{"message": {"content": "hello"}}]}
    assert deriver_main._extract_text(response) == "hello"


def test_extract_text_handles_object_with_as_dict():
    obj = SimpleNamespace(
        as_dict=lambda: {"choices": [{"message": {"content": "from sdk"}}]}
    )
    assert deriver_main._extract_text(obj) == "from sdk"


def test_extract_text_falls_back_to_top_level_content():
    assert deriver_main._extract_text({"content": "fallback"}) == "fallback"


def test_extract_text_raises_on_none():
    with pytest.raises(ValueError):
        deriver_main._extract_text(None)


def test_extract_text_raises_on_unrecognised_shape():
    with pytest.raises(ValueError):
        deriver_main._extract_text(SimpleNamespace())


# ---- _as_datetime ----------------------------------------------------------


def test_as_datetime_passes_through_datetime():
    dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert deriver_main._as_datetime(dt) is dt


def test_as_datetime_parses_iso_string():
    parsed = deriver_main._as_datetime("2026-01-01T00:00:00Z")
    assert parsed == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_as_datetime_rejects_int():
    with pytest.raises(TypeError):
        deriver_main._as_datetime(1234567890)


# ---- assemble_drafts -------------------------------------------------------


def test_assemble_drafts_dispatches_one_llm_call_per_observed_peer():
    messages = [_msg(1, "alice"), _msg(2, "bob"), _msg(3, "alice")]
    session_peers = {("ws", "s1"): ["alice", "bob"]}

    chat_calls: list[dict[str, object]] = []

    def fake_chat_sync(*, workspace_name, role, messages):
        chat_calls.append({"workspace": workspace_name, "role": role})
        observed = "alice" if "alice" in messages[0]["content"] else "bob"
        return {
            "choices": [
                {
                    "message": {
                        "content": f'{{"explicit": [{{"content": "{observed} likes tests"}}]}}'
                    }
                }
            ]
        }

    with patch.object(deriver_main.llm_dispatch, "chat_sync", side_effect=fake_chat_sync):
        drafts = deriver_main.assemble_drafts(
            messages=messages, session_peers=session_peers
        )

    # Two observed peers (alice, bob) -> two LLM calls.
    assert len(chat_calls) == 2
    assert all(c["role"] == "deriver" for c in chat_calls)
    assert all(c["workspace"] == "ws" for c in chat_calls)

    # 1 fact per observed peer * 2 observers each = 4 documents total.
    assert len(drafts) == 4
    by_observer_observed = {(d.observer, d.observed) for d in drafts}
    assert by_observer_observed == {
        ("alice", "alice"),
        ("bob", "alice"),
        ("alice", "bob"),
        ("bob", "bob"),
    }


def test_assemble_drafts_skips_groups_when_llm_returns_garbage():
    messages = [_msg(1, "alice"), _msg(2, "bob")]
    session_peers = {("ws", "s1"): ["alice", "bob"]}

    def fake_chat_sync(*, workspace_name, role, messages):
        if "alice" in messages[0]["content"]:
            return {
                "choices": [
                    {"message": {"content": '{"explicit": [{"content": "fact"}]}'}}
                ]
            }
        return {"choices": [{"message": {"content": "totally not json"}}]}

    with patch.object(deriver_main.llm_dispatch, "chat_sync", side_effect=fake_chat_sync):
        drafts = deriver_main.assemble_drafts(
            messages=messages, session_peers=session_peers
        )

    # Only alice's batch survived; 1 fact * 2 observers = 2.
    assert len(drafts) == 2
    assert {d.observed for d in drafts} == {"alice"}


def test_assemble_drafts_returns_empty_when_no_facts_extracted():
    messages = [_msg(1, "alice")]
    session_peers = {("ws", "s1"): ["alice"]}

    def fake_chat_sync(**_kwargs):
        return {"choices": [{"message": {"content": '{"explicit": []}'}}]}

    with patch.object(deriver_main.llm_dispatch, "chat_sync", side_effect=fake_chat_sync):
        drafts = deriver_main.assemble_drafts(
            messages=messages, session_peers=session_peers
        )

    assert drafts == []


def test_extract_facts_calls_dispatcher_with_deriver_role():
    from foreman.jobs.deriver.observer_observed import DeriverBatch

    batch = DeriverBatch(
        workspace_name="ws",
        session_name="s1",
        observed="alice",
        observers=["alice"],
        messages=[_msg(1, "alice")],
    )

    with patch.object(deriver_main.llm_dispatch, "chat_sync") as mock_chat:
        mock_chat.return_value = {"choices": [{"message": {"content": "ok"}}]}
        result = deriver_main.extract_facts(batch)

    assert result == "ok"
    kwargs = mock_chat.call_args.kwargs
    assert kwargs["workspace_name"] == "ws"
    assert kwargs["role"] == "deriver"
    assert len(kwargs["messages"]) == 1
    assert kwargs["messages"][0]["role"] == "user"
    assert "alice" in kwargs["messages"][0]["content"]
