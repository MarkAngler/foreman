"""Reasoning-level config — tool gating, iteration budgets, role mapping."""

from __future__ import annotations

import pytest

from foreman.app.dialectic.reasoning_levels import LEVELS, resolve


def test_minimal_only_enables_search_memory():
    cfg = resolve("minimal")
    assert cfg.tools == ("search_memory",)
    assert cfg.max_iterations == 3


def test_levels_strictly_widen_tool_set():
    order = ["minimal", "low", "medium", "high", "max"]
    prev: set[str] = set()
    for name in order:
        current = set(LEVELS[name].tools)  # type: ignore[index]
        assert prev.issubset(current), f"{name} dropped a tool from a lower level"
        prev = current


def test_max_includes_all_six_tools():
    cfg = resolve("max")
    assert set(cfg.tools) == {
        "search_memory",
        "get_observation_context",
        "search_messages",
        "grep_messages",
        "get_messages_by_date_range",
        "get_reasoning_chain",
    }
    assert cfg.max_iterations == 10


def test_iteration_budgets_increase_monotonically():
    budgets = [LEVELS[n].max_iterations for n in ("minimal", "low", "medium", "high", "max")]  # type: ignore[index]
    assert budgets == sorted(budgets)
    assert len(set(budgets)) == len(budgets)


def test_unknown_level_raises():
    with pytest.raises(ValueError, match="unknown reasoning_level"):
        resolve("super-max")


def test_all_levels_use_dialectic_role():
    for cfg in LEVELS.values():
        assert cfg.role == "dialectic"
