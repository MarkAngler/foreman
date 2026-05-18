"""Prompt assembly — perspective sections, peer-card injection, tool inventory."""

from __future__ import annotations

from foreman.app.dialectic import prompts
from foreman.app.dialectic.reasoning_levels import resolve


def test_self_observation_uses_global_perspective():
    text = prompts.agent_system_prompt(
        observer="alice",
        observed="alice",
        observer_peer_card=["likes coffee"],
        observed_peer_card=None,
        available_tools=["search_memory"],
    )
    assert "answering queries about 'alice'" in text
    assert "directional query" not in text
    assert "likes coffee" in text


def test_directional_query_includes_both_cards():
    text = prompts.agent_system_prompt(
        observer="alice",
        observed="bob",
        observer_peer_card=["asks careful questions"],
        observed_peer_card=["loves jazz"],
        available_tools=["search_memory"],
    )
    assert "alice" in text and "bob" in text
    assert "directional query" in text
    assert "asks careful questions" in text
    assert "loves jazz" in text


def test_no_peer_cards_omits_explanation():
    text = prompts.agent_system_prompt(
        observer="x",
        observed="y",
        observer_peer_card=None,
        observed_peer_card=None,
        available_tools=["search_memory"],
    )
    assert "Peer cards are constructed summaries" not in text
    assert "<peer_card>" not in text


def test_tool_inventory_only_lists_enabled_tools():
    text = prompts.agent_system_prompt(
        observer="a",
        observed="a",
        observer_peer_card=None,
        observed_peer_card=None,
        available_tools=["search_memory", "grep_messages"],
    )
    inventory = text.split("## AVAILABLE TOOLS")[1].split("## WORKFLOW")[0]
    assert "`search_memory`" in inventory
    assert "`grep_messages`" in inventory
    # Tools NOT in available_tools must not appear in the inventory section.
    assert "`get_reasoning_chain`" not in inventory
    assert "`search_messages`" not in inventory
    assert "`get_observation_context`" not in inventory
    assert "`get_messages_by_date_range`" not in inventory


def test_query_message_with_prefetched_context():
    out = prompts.query_message("what does she like?", "obs1\nobs2")
    assert out.startswith("Query: what does she like?")
    assert "obs1" in out and "obs2" in out


def test_query_message_without_prefetched_context():
    out = prompts.query_message("hi", None)
    assert out == "Query: hi"


def test_system_prompt_for_level_passes_through_tools():
    level = resolve("high")
    text = prompts.system_prompt_for_level(
        level=level,
        observer="o",
        observed="o",
        observer_peer_card=None,
        observed_peer_card=None,
    )
    for t in level.tools:
        assert f"`{t}`" in text
