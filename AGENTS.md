# AGENTS.md — context for Phase 2 feature implementations

This document is read by every Phase 2 agent. It captures the project's spine
contracts, conventions, and what is in and out of scope.

## What we're building

A clean-room rebuild of [honcho.dev](https://honcho.dev) — Plastic Labs' identity
and memory layer for AI agents — on the Databricks platform, distributable as a
Databricks Asset Bundle. Architecture: Lakebase (Postgres) for OLTP truth,
Vector Search for semantic recall (Direct Access for messages, Delta Sync for
derived documents), Model Serving for LLM compute, FastAPI on Databricks Apps
for the REST surface + dialectic agent, four Lakeflow Jobs as background workers.

Full plan: `C:\Users\marka\.claude\plans\sorted-weaving-yao.md` — read it before
writing code.

Honcho upstream to port from: <https://github.com/plastic-labs/honcho>
- `src/models.py` — data model (already ported into our schema/migrations)
- `src/routers/` — REST routers
- `src/dialectic/` — dialectic agent loop, six tools
- `src/deriver/` — fact-extraction prompts and queue handling
- `src/summarizer/` — rolling summaries
- `src/dreamer/` — surprisal + deduction/induction phases
- `src/schemas/api.py` — request/response shapes (port semantics, redesign API)

API design constraint: **clean redesign**, not wire-compatible. Take honcho's
semantics; redesign the URLs and payloads to be cleaner where useful. Keep
resource names and conceptual model intact (workspaces / peers / sessions /
messages / collections via observer-observed pairs / dialectic chat).

## Spine contracts (DO NOT MODIFY)

The following files are frozen Phase-1 spine. If you find a bug, document it
and surface it for Phase 3 — do not fix it in your worktree.

```
databricks.yml
resources/*.yml
schema/                            # Lakebase schema, migrations
install/bootstrap.py               # Pre-deploy infra creation
src/foreman/__init__.py
src/foreman/lib/                   # ALL files in here are frozen contracts
  config.py                        # Settings — env-driven config
  lakebase.py                      # async engine + 50-min OAuth refresh
  vector_search.py                 # typed wrappers: upsert_message, query_messages, query_documents, embed
  auth.py                          # JWT issuance + scope guards (admin/workspace/peer/session)
  llm_dispatch.py                  # per-workspace LLM dispatcher (chat, chat_sync)
  lakebase_read.py                 # Spark JDBC read helpers
  delta_write.py                   # Delta upsert helpers
  tracing.py                       # MLflow tracing init
src/foreman/jobs/schema_init/      # frozen — runs migrations and infra setup
src/foreman/app/main.py            # FastAPI lifespan + /healthz — extend, don't replace
src/foreman/app/__init__.py
app.yaml
requirements.txt
pyproject.toml
tests/spine_smoke.py               # Phase-1 acceptance test — extend, don't replace
tests/unit/test_auth.py
tests/unit/test_config.py
```

You may add new files anywhere outside the frozen list. You may add new
dependencies to `requirements.txt` (clearly justified) and `pyproject.toml`.

## Conventions

- **Code style**: Python 3.11+, no comments except for non-obvious WHY. No
  docstrings on every function — only on public module APIs. Follow the
  existing voice in `src/foreman/lib/`.
- **Async**: Routers and the dialectic agent are async. Use the `session()`
  context manager from `foreman.lib.lakebase` for DB access.
- **Sync**: Spark jobs are sync. Use `chat_sync()` from `llm_dispatch` and
  helpers from `lakebase_read` / `delta_write`.
- **Error handling**: Validate at request boundaries (Pydantic). Trust internal
  contracts. Don't add fallbacks for can't-happen cases.
- **No swallowed errors**: log and re-raise, or convert to `HTTPException` at
  the router level.
- **Tracing**: decorate non-trivial functions with `@trace` from
  `foreman.lib.tracing`. Tag spans with workspace/peer/session names.
- **No Anthropic / OpenAI direct calls**: always go through `llm_dispatch` so
  per-workspace endpoint switching works.
- **Pagination**: use `fastapi-pagination` (already a dep) for list endpoints.

## Acceptance pattern

Each agent's worktree must include:
1. The implementation under the agreed paths.
2. A test file that exercises the new code's contract. Unit tests if possible
   (mocking spine calls); integration tests can be marked
   `@pytest.mark.integration` and skipped in unit runs.
3. `PYTHONPATH=src .venv/Scripts/python.exe -m pytest tests/unit/ -x` must pass.

If you cannot complete a piece (missing context, blocked by a spine gap),
document it in `worktree_notes.md` at the worktree root and move on.

## Honcho semantics quick reference

- **Workspace**: top-level tenant container, identified by `name`.
- **Peer**: an entity with persistent identity in a workspace. Either a human
  user or an AI agent — same shape. `(workspace_name, peer_name)` is the key.
- **Session**: a conversational context where multiple peers interact.
  `(workspace_name, session_name)` is the key. Many-to-many to peers via
  `session_peers`.
- **Message**: a single utterance from a peer in a session. Has `public_id`
  (URL-friendly) and `id` (internal bigint). Content ≤ 65535 chars.
- **Document / "conclusion"**: a fact / observation / inference about a peer,
  keyed by the *(observer, observed, workspace)* triple. Levels:
  `explicit` (extracted directly from messages), `deductive` (inferred from
  explicit + deductive), `inductive` (cross-pattern), `contradiction` (flagged
  conflicts). `source_ids` forms a reasoning DAG.
- **Peer card**: ≤40 dedup'd facts, the compact peer identity summary.
- **Queue items**: durable work for background workers. Partial unique index
  on `(work_unit_key, task_type) WHERE processed=false` enforces honcho's
  exactly-once semantics — respect it.
- **JWT scopes**: admin > workspace > peer/session. Use the guards in
  `foreman.lib.auth`.
- **Workspace LLM config**: per-workspace overrides for model endpoints by
  role (dialectic / deriver / dreamer / summarizer / embeddings). Always go
  through `llm_dispatch` which resolves this transparently.
