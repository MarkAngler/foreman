"""Unit tests for the deriver prompt template."""

from __future__ import annotations

from datetime import datetime, timezone

from foreman.jobs.deriver.prompts import (
    JSON_OUTPUT_INSTRUCTIONS,
    format_message_turn,
    minimal_deriver_prompt,
)


def test_minimal_prompt_mentions_peer_id_and_messages():
    prompt = minimal_deriver_prompt(peer_id="alice", messages="hello world")
    assert "alice" in prompt
    assert "hello world" in prompt
    assert "<messages>" in prompt
    assert "</messages>" in prompt


def test_minimal_prompt_includes_explicit_extraction_rules():
    prompt = minimal_deriver_prompt(peer_id="alice", messages="m")
    assert "[EXPLICIT]" in prompt
    assert "atomic facts" in prompt
    assert JSON_OUTPUT_INSTRUCTIONS in prompt


def test_minimal_prompt_includes_custom_instructions_when_present():
    prompt = minimal_deriver_prompt(
        peer_id="alice",
        messages="m",
        custom_instructions="Focus on dietary preferences.",
    )
    assert "CUSTOM INSTRUCTIONS:" in prompt
    assert "Focus on dietary preferences." in prompt


def test_minimal_prompt_omits_custom_section_when_blank():
    prompt = minimal_deriver_prompt(peer_id="alice", messages="m", custom_instructions="   ")
    assert "CUSTOM INSTRUCTIONS:" not in prompt


def test_minimal_prompt_omits_custom_section_when_none():
    prompt = minimal_deriver_prompt(peer_id="alice", messages="m", custom_instructions=None)
    assert "CUSTOM INSTRUCTIONS:" not in prompt


def test_format_message_turn_renders_iso_timestamp_and_peer():
    line = format_message_turn(
        "I love pasta",
        datetime(2026, 5, 15, 12, 30, 45, 123456, tzinfo=timezone.utc),
        "alice",
    )
    # Microseconds stripped, peer + content present.
    assert "2026-05-15T12:30:45+00:00" in line
    assert "alice" in line
    assert "I love pasta" in line
    assert "123456" not in line
