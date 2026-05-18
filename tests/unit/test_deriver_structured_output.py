"""Unit tests for parsing the deriver LLM response and building Document drafts."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from foreman.jobs.deriver.structured_output import (
    PromptRepresentation,
    build_document_drafts,
    document_id,
    parse_response,
)


def test_parse_response_handles_clean_json():
    raw = '{"explicit": [{"content": "Alice lives in NYC"}]}'
    rep = parse_response(raw)
    assert isinstance(rep, PromptRepresentation)
    assert [f.content for f in rep.explicit] == ["Alice lives in NYC"]


def test_parse_response_strips_markdown_fence():
    raw = '```json\n{"explicit": [{"content": "Alice has a dog"}]}\n```'
    rep = parse_response(raw)
    assert [f.content for f in rep.explicit] == ["Alice has a dog"]


def test_parse_response_extracts_object_from_surrounding_prose():
    raw = "Sure! Here is the result: {\"explicit\": [{\"content\": \"x\"}]} (hope it helps)"
    rep = parse_response(raw)
    assert [f.content for f in rep.explicit] == ["x"]


def test_parse_response_accepts_null_explicit_list():
    rep = parse_response('{"explicit": null}')
    assert rep.explicit == []


def test_parse_response_rejects_non_json():
    with pytest.raises(ValueError):
        parse_response("not json at all")


def test_parse_response_rejects_empty_string():
    with pytest.raises(ValueError):
        parse_response("")


def test_parse_response_tolerates_top_level_array_returning_empty():
    # When the model wraps the response in an array, the extractor falls back
    # to the first balanced object inside. If that object lacks `explicit`,
    # Pydantic's default produces an empty representation rather than raising.
    rep = parse_response('[{"content": "x"}]')
    assert rep.explicit == []


def test_parse_response_rejects_array_of_pure_garbage():
    with pytest.raises(ValueError):
        parse_response("[1, 2, 3]")


def test_parse_response_rejects_empty_content():
    with pytest.raises(ValueError):
        parse_response('{"explicit": [{"content": ""}]}')


def test_document_id_is_deterministic():
    a = document_id("ws", "alice", "alice", "lives in NYC")
    b = document_id("ws", "alice", "alice", "lives in NYC")
    assert a == b
    assert len(a) == 24


def test_document_id_changes_with_any_input_field():
    base = document_id("ws", "alice", "alice", "fact")
    assert document_id("ws2", "alice", "alice", "fact") != base
    assert document_id("ws", "bob", "alice", "fact") != base
    assert document_id("ws", "alice", "bob", "fact") != base
    assert document_id("ws", "alice", "alice", "fact!") != base


def test_build_document_drafts_fans_out_across_observers():
    rep = PromptRepresentation.model_validate(
        {"explicit": [{"content": "Alice has a dog"}, {"content": "Alice lives in NYC"}]}
    )
    drafts = build_document_drafts(
        representation=rep,
        workspace_name="ws",
        observed="alice",
        observers=["alice", "bob"],
        source_message_public_ids=["pub-1", "pub-2"],
        session_name="s1",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    # 2 facts * 2 observers = 4 drafts
    assert len(drafts) == 4
    assert {(d.observer, d.content) for d in drafts} == {
        ("alice", "Alice has a dog"),
        ("alice", "Alice lives in NYC"),
        ("bob", "Alice has a dog"),
        ("bob", "Alice lives in NYC"),
    }
    for d in drafts:
        assert d.observed == "alice"
        assert d.workspace_name == "ws"
        assert d.level == "explicit"
        assert d.source_ids == ["pub-1", "pub-2"]
        assert json.loads(d.metadata) == {"session_name": "s1"}


def test_build_document_drafts_returns_empty_when_no_facts():
    rep = PromptRepresentation(explicit=[])
    drafts = build_document_drafts(
        representation=rep,
        workspace_name="ws",
        observed="alice",
        observers=["alice"],
        source_message_public_ids=["pub-1"],
        session_name="s1",
    )
    assert drafts == []


def test_build_document_drafts_uses_now_when_created_at_missing():
    rep = PromptRepresentation.model_validate({"explicit": [{"content": "x"}]})
    before = datetime.now(timezone.utc)
    drafts = build_document_drafts(
        representation=rep,
        workspace_name="ws",
        observed="alice",
        observers=["alice"],
        source_message_public_ids=["pub-1"],
        session_name="s1",
    )
    after = datetime.now(timezone.utc)
    assert before <= drafts[0].created_at <= after
