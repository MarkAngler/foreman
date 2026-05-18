"""Reasoning-level config for the dialectic agent.

Five levels — minimal/low/medium/high/max — gate three things:

  - which tools the LLM may call
  - which `llm_dispatch` role to use (so the workspace LLM config can map
    each level to a different endpoint)
  - the iteration / output-token budget of the tool loop

Ported from honcho `src/config.py::DialecticSettings.LEVELS`. The honcho
implementation maps every level to the same role today; we keep the same
single 'dialectic' role for parity but expose the indirection so a future
config row could route 'minimal' to a cheaper endpoint without touching code.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ReasoningLevel = Literal["minimal", "low", "medium", "high", "max"]

# Tool names — must match the `name` field in dialectic.tools.TOOL_SPECS.
TOOL_SEARCH_MEMORY = "search_memory"
TOOL_GET_OBSERVATION_CONTEXT = "get_observation_context"
TOOL_SEARCH_MESSAGES = "search_messages"
TOOL_GREP_MESSAGES = "grep_messages"
TOOL_GET_MESSAGES_BY_DATE_RANGE = "get_messages_by_date_range"
TOOL_GET_REASONING_CHAIN = "get_reasoning_chain"


@dataclass(frozen=True)
class LevelConfig:
    name: ReasoningLevel
    role: str  # llm_dispatch role — currently always 'dialectic'
    tools: tuple[str, ...]
    max_iterations: int
    max_output_tokens: int


LEVELS: dict[ReasoningLevel, LevelConfig] = {
    "minimal": LevelConfig(
        name="minimal",
        role="dialectic",
        tools=(TOOL_SEARCH_MEMORY,),
        max_iterations=3,
        max_output_tokens=1024,
    ),
    "low": LevelConfig(
        name="low",
        role="dialectic",
        tools=(TOOL_SEARCH_MEMORY, TOOL_GET_OBSERVATION_CONTEXT),
        max_iterations=4,
        max_output_tokens=1536,
    ),
    "medium": LevelConfig(
        name="medium",
        role="dialectic",
        tools=(
            TOOL_SEARCH_MEMORY,
            TOOL_GET_OBSERVATION_CONTEXT,
            TOOL_SEARCH_MESSAGES,
        ),
        max_iterations=5,
        max_output_tokens=2048,
    ),
    "high": LevelConfig(
        name="high",
        role="dialectic",
        tools=(
            TOOL_SEARCH_MEMORY,
            TOOL_GET_OBSERVATION_CONTEXT,
            TOOL_SEARCH_MESSAGES,
            TOOL_GREP_MESSAGES,
            TOOL_GET_MESSAGES_BY_DATE_RANGE,
        ),
        max_iterations=7,
        max_output_tokens=3072,
    ),
    "max": LevelConfig(
        name="max",
        role="dialectic",
        tools=(
            TOOL_SEARCH_MEMORY,
            TOOL_GET_OBSERVATION_CONTEXT,
            TOOL_SEARCH_MESSAGES,
            TOOL_GREP_MESSAGES,
            TOOL_GET_MESSAGES_BY_DATE_RANGE,
            TOOL_GET_REASONING_CHAIN,
        ),
        max_iterations=10,
        max_output_tokens=4096,
    ),
}


def resolve(level: ReasoningLevel | str) -> LevelConfig:
    if level not in LEVELS:
        valid = ", ".join(LEVELS)
        raise ValueError(f"unknown reasoning_level {level!r}; valid: {valid}")
    return LEVELS[level]  # type: ignore[index]
