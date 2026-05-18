"""Unit tests for config defaults and derived properties."""

from __future__ import annotations

from foreman.lib.config import Settings


def test_index_paths_compose_from_catalog_and_schema():
    s = Settings(catalog="cat", FOREMAN_SCHEMA="sch")
    assert s.messages_index == "cat.sch.messages_idx"
    assert s.documents_index == "cat.sch.documents_idx"
    assert s.documents_delta == "cat.sch.documents_delta"


def test_default_endpoint_for_role_lookup():
    s = Settings()
    assert s.default_endpoint_for("dialectic") == s.llm_default_dialectic
    assert s.default_endpoint_for("deriver") == s.llm_default_deriver
    assert s.default_endpoint_for("embeddings") == s.llm_default_embeddings


def test_default_endpoint_for_unknown_role_raises():
    s = Settings()
    try:
        s.default_endpoint_for("unknown")  # type: ignore[arg-type]
    except KeyError:
        return
    raise AssertionError("expected KeyError for unknown role")
