"""Env-var overrides flow through Settings and the job bootstrap helper.

Covers the wiring added to make LLM endpoints + embedding model configurable
via Databricks Asset Bundle variables (FOREMAN_* env vars in app config and
job parameters).
"""

from __future__ import annotations

import pytest

from foreman.jobs._bootstrap import apply_env_overrides
from foreman.lib.config import Settings


@pytest.mark.parametrize(
    "env_var, attr, value",
    [
        ("FOREMAN_LLM_DEFAULT_DIALECTIC", "llm_default_dialectic", "endpoint-a"),
        ("FOREMAN_LLM_DEFAULT_DERIVER", "llm_default_deriver", "endpoint-b"),
        ("FOREMAN_LLM_DEFAULT_SUMMARIZER", "llm_default_summarizer", "endpoint-c"),
        ("FOREMAN_LLM_DEFAULT_DREAMER", "llm_default_dreamer", "endpoint-d"),
        ("FOREMAN_LLM_DEFAULT_EMBEDDINGS", "llm_default_embeddings", "endpoint-e"),
    ],
)
def test_llm_endpoint_env_override(monkeypatch, env_var, attr, value):
    monkeypatch.setenv(env_var, value)
    s = Settings()
    assert getattr(s, attr) == value
    role = attr.removeprefix("llm_default_")
    assert s.default_endpoint_for(role) == value


def test_embedding_dim_env_override(monkeypatch):
    monkeypatch.setenv("FOREMAN_EMBEDDING_DIM", "2048")
    s = Settings()
    assert s.embedding_dim == 2048


def test_apply_env_overrides_sets_missing(monkeypatch):
    monkeypatch.delenv("FOREMAN_LLM_DEFAULT_DERIVER", raising=False)
    apply_env_overrides(["FOREMAN_LLM_DEFAULT_DERIVER=from-args"])
    assert Settings().llm_default_deriver == "from-args"


def test_apply_env_overrides_does_not_clobber_existing(monkeypatch):
    monkeypatch.setenv("FOREMAN_LLM_DEFAULT_DIALECTIC", "preset")
    apply_env_overrides(["FOREMAN_LLM_DEFAULT_DIALECTIC=from-args"])
    assert Settings().llm_default_dialectic == "preset"


def test_apply_env_overrides_ignores_non_kv_args(monkeypatch):
    monkeypatch.delenv("FOREMAN_LLM_DEFAULT_SUMMARIZER", raising=False)
    apply_env_overrides(["--flag", "positional", "FOREMAN_LLM_DEFAULT_SUMMARIZER=x"])
    assert Settings().llm_default_summarizer == "x"
