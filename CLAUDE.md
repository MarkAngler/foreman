# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A clean-room rebuild of [honcho.dev](https://honcho.dev) (Plastic Labs' identity & memory layer for AI agents) on the Databricks platform, distributable as a Databricks Asset Bundle. Honcho upstream — port semantics, not the wire — is at <https://github.com/plastic-labs/honcho>. Full architecture/build plan: `C:\Users\marka\.claude\plans\sorted-weaving-yao.md`. Project rules are split across `.claude/rules/rules.md` (production-code rules) and `.claude/rules/Reasoning.md` (decision discipline) — both are auto-loaded and must be followed.

## Commands

Windows / PowerShell — the venv interpreter is `.venv/Scripts/python.exe`. The Makefile assumes a POSIX shell; on Windows use the underlying commands directly.

```bash
# Unit tests (no Databricks needed) — the default fast loop
PYTHONPATH=src .venv/Scripts/python -m pytest tests/unit/ -v

# Run a single test file or test
PYTHONPATH=src .venv/Scripts/python -m pytest tests/unit/test_auth.py -v
PYTHONPATH=src .venv/Scripts/python -m pytest tests/unit/test_auth.py::test_issue_admin -v

# Integration tests (requires deployed bundle + auth) — marked @pytest.mark.integration
PYTHONPATH=src .venv/Scripts/python -m pytest tests/integration/ -m integration -v

# Phase-1 spine smoke test (requires deployed bundle + auth)
PYTHONPATH=src .venv/Scripts/python -m pytest tests/spine_smoke.py -s -v

# Lint / format
.venv/Scripts/python -m ruff check src tests install schema
.venv/Scripts/python -m ruff format src tests install schema

# Bundle lifecycle
databricks bundle validate
databricks bundle deploy
databricks bundle run schema_init   # Alembic + Delta + synced tables + VS indexes
databricks bundle run foreman_app

# One-shot full install (Lakebase + VS endpoint + secrets + bundle deploy + migrate + run)
.venv/Scripts/python install/install.py
```

Pytest is configured with `asyncio_mode = "auto"` (see `pyproject.toml`) — async tests don't need an explicit marker. The `integration` marker is registered there too.

## Architecture

OLTP truth lives in **Lakebase Provisioned Postgres**. It's the system of record for all 11 honcho tables (workspaces, peers, sessions, session_peers, messages, documents, queue_items, active_queue_session, webhook_endpoints, peer_cards, workspace_llm_config + the `documents_mirror` synced table). Vector Search is index-only and cannot replace it (no uniqueness, no ordered reads, no composite PKs, no partial unique indexes, no transactional writes).

Two Vector Search indexes on the `foreman-vs` endpoint:
- **`messages_idx`** — Direct Access. The FastAPI app embeds and upserts inline at message-create so a "send then immediately query memory" flow has <1s freshness.
- **`documents_idx`** — Delta Sync with managed embeddings, sourced from `documents_delta`. Written by the async deriver job; minute-level latency is fine.

Compute split:
- **FastAPI on Databricks Apps** (`src/foreman/app/`) — async REST surface + the dialectic agent (a tool-calling chat loop).
- **Four Lakeflow Jobs** (`src/foreman/jobs/`), all sync Spark: `deriver` (1 min — extract explicit facts from new messages), `summarizer` (5 min — rolling session summaries), `dreamer` (hourly — surprisal-prioritized deduction/induction + peer-card updates), `webhooks` (1 min — outbound delivery with HMAC). All jobs read Lakebase via JDBC and write to Delta.
- **MCP server for Claude Code** (`src/foreman/mcp/`) — stdio server exposing ~10 foreman tools to MCP clients. Auth flows through `databricks-sdk` by default (OAuth U2M / PAT / profile); `FOREMAN_TOKEN` / `FOREMAN_BASE_URL` env vars override for CI. See `src/foreman/mcp/README.md`.

### Resource tagging

Every foreman resource carries `project=foreman` so the entire footprint is filterable in Account Usage / Billing. Jobs set it via the `tags:` field in `resources/jobs.yml`; the Lakebase instance and Vector Search endpoint get it applied by `install/bootstrap.py` (`custom_tags` on create + `update_endpoint_custom_tags` to keep existing resources current). Databricks Apps do not support custom tags in `databricks-sdk` 0.81.0, so the FastAPI app is identifiable only by its `foreman-${bundle.target}` name; re-tag when SDK adds support. UC catalog/schema are not tagged — the `foreman` catalog name is self-identifying.

### The spine — `src/foreman/lib/` (frozen contracts)

Every higher layer flows through these. **Do not modify these files** — if you find a bug, document it for Phase 3 instead of fixing it in place. See `AGENTS.md` for the full frozen list.

- `config.py` — `settings()` (cached). Catalog/schema, Lakebase + VS endpoint names, per-role default Model Serving endpoints, embedding dim (1024). Computed UC names like `messages_index`, `documents_delta`.
- `lakebase.py` — async SQLAlchemy engine for Postgres with rolling 50-minute OAuth token refresh (Lakebase tokens expire after 1h). Use `async with session() as s:` from FastAPI handlers. Engine is started/stopped from the FastAPI lifespan.
- `vector_search.py` — typed wrappers: `upsert_message`, `query_messages`, `query_documents`, `embed`, plus `ensure_*_index` lifecycle helpers used by `schema_init`.
- `auth.py` — HS256 JWT issuance + scope guards. Hierarchy: **admin > workspace > peer/session**. Signing key is loaded from Databricks secret `foreman/jwt_signing_key` (cached). Use the `request_workspace_guard()` / `workspace_guard()` dependency factories on routers.
- `llm_dispatch.py` — **all** LLM calls go through here. Never call Anthropic/OpenAI/the SDK directly. Resolves `(workspace_name, role)` → endpoint via the `workspace_llm_config` table, falling back to install defaults. Two entrypoints: `chat()` (async, FastAPI) and `chat_sync()` (sync, Spark jobs). See "Tool-calling quirk" below.
- `lakebase_read.py` / `delta_write.py` — sync helpers for Spark jobs.
- `tracing.py` — MLflow tracing init + the `@trace` decorator. Decorate non-trivial functions; tag spans with workspace/peer/session names.

### Tool-calling quirk (dialectic)

The Databricks SDK's `serving_endpoints.query` does **not** propagate a `tools=` parameter to the underlying endpoint. `llm_dispatch._invoke` therefore routes any request with `tools` in kwargs through the OpenAI-compatible path against the same endpoint, regardless of the declared provider. For the dialectic agent to function, a workspace must have `workspace_llm_config(role='dialectic', provider='openai-compatible', ...)`. See `worktree_notes_dialectic.md`.

### Per-workspace LLM config

Five roles override-able per workspace via the `workspace_llm_config` table: `dialectic`, `deriver`, `summarizer`, `dreamer`, `embeddings`. Providers: `fmapi` (default, Foundation Model APIs), `external` (registered external Model Serving endpoint — Anthropic/OpenAI/etc.), `openai-compatible` (any OpenAI Chat Completions wire-format endpoint, including for tool-calling), `custom` (fine-tuned/ChatAgent). `params` JSON is merged into every call. There is no cache — every call hits Lakebase to resolve.

### Routers

Wired by `src/foreman/app/routers/__init__.py:all_routers()` in `main.py`. The dialectic `chat` router is mounted separately. JWT auth is enforced on every route via dependency guards in `lib/auth.py`; the workspace path param is checked against the token's scope.

## Conventions (project-specific — these override defaults)

- **Python 3.11+**, line length 100, ruff configured (E, F, I, B, UP, N, W; E501 ignored).
- **No comments** except non-obvious WHY. **No docstrings on every function** — only on public module APIs. Match the existing voice in `src/foreman/lib/`.
- Routers + dialectic agent are **async**; use `session()` from `foreman.lib.lakebase`. Spark jobs are **sync**; use `chat_sync()` and the sync read/write helpers.
- Validate at request boundaries (Pydantic). Trust internal contracts. Don't add fallbacks for can't-happen cases.
- Don't swallow errors — log and re-raise, or convert to `HTTPException` at the router level.
- **Never call Anthropic/OpenAI directly.** Always go through `llm_dispatch` so per-workspace endpoint switching works.
- Use `fastapi-pagination` (already a dep) for list endpoints.
- **Commit messages**: Conventional Commits (`feat:`, `fix:`, `refactor:`, etc.). **Do not reference Claude, Anthropic, or any AI tool in commit messages.**

## Using foreman as memory (dogfood)

Foreman MCP tools are wired via `.mcp.json` and exposed to Claude Code as `mcp__foreman__*`. Workspace + peer attribution are read from `mcpServers.foreman.env` in `.mcp.json` (`FOREMAN_DEFAULT_WORKSPACE`, `FOREMAN_USER_PEER`, `FOREMAN_ASSISTANT_PEER`) — the MCP server and the Stop-hook CLI share that single config block. Session name = current git branch (slash → dash, lowercased). Substantive turns are auto-captured by the `Stop` hook in `.claude/settings.json`, which invokes `python -m foreman.mcp.capture`. That CLI shares the `capture_turn()` code with the `foreman_capture_turn` MCP tool ([src/foreman/mcp/capture.py](src/foreman/mcp/capture.py)). Capture is filtered — trivial chat is skipped — and soft-fails so a foreman outage never blocks a turn.

Don't call foreman MCP tools every turn. Only when prior context is load-bearing on the current task:

- **`mcp__foreman__foreman_search`** — when the user references something with a definite article you can't resolve from this conversation ("the bug we found", "the approach we picked", "that PR"); when deciding between approaches that may have been debated before; when returning to a topic after a long context gap (e.g. post-compaction).
- **`mcp__foreman__foreman_session_context`** — once at the top of a conversation that resumes mid-task on a branch with prior captured history. Pass `session_name=<branch>`, `tokens<=2000`.
- **`mcp__foreman__foreman_chat`** (dialectic) — when you need synthesized belief across many prior turns ("what does Mark prefer when X comes up") rather than a raw recall. Use `peer_name="mark"`, `reasoning_level="low"` by default. Requires `workspace_llm_config(role='dialectic', provider='openai-compatible', ...)` per the tool-calling quirk above; surface failures rather than silently degrading.

Don't query foreman to answer questions about *the foreman codebase itself* — read the code. Foreman is for things you can't get from grep: prior decisions, conversational context, evolving user preferences.

## Spine = frozen

Files listed under "Spine contracts" in `AGENTS.md` are Phase-1 frozen. This includes everything in `src/foreman/lib/`, all of `schema/`, `databricks.yml`, every `resources/*.yml`, `install/bootstrap.py`, `src/foreman/jobs/schema_init/`, `src/foreman/app/main.py`, `tests/spine_smoke.py`, `tests/unit/test_auth.py`, and `tests/unit/test_config.py`. Extend `main.py` to wire new routers; do not replace it. New files anywhere else are fair game. New dependencies in `requirements.txt`/`pyproject.toml` are allowed when clearly justified.
