# Changelog

All notable changes to foreman.

## [Unreleased]

### Phase 1 — spine (sequential)
- Bundle entrypoint `databricks.yml` with dev/prod targets and per-role default endpoint variables.
- UC schema declared in `resources/catalog.yml`.
- Lakebase Provisioned + Vector Search endpoint + secret scope created via `install/bootstrap.py` (DABs cannot declare these resource types).
- Alembic schema (`schema/versions/0001_initial_schema.py`) with all 11 honcho tables — composite keys, partial unique indexes for queue dedup, GIN index on `documents.source_ids`, JSONB metadata columns, CHECK constraints — plus `workspace_llm_config` (per-workspace LLM overrides) and `documents_mirror` (Delta→Lakebase sync target).
- `schema_init` Lakeflow Job: runs migrations, creates Delta tables, configures Lakebase↔Delta synced tables in both directions, creates the two Vector Search indexes (`messages_idx` Direct Access, `documents_idx` Delta Sync).
- Shared helpers under `src/foreman/lib/`:
  - `config.py` — Pydantic Settings, env-driven config
  - `lakebase.py` — async SQLAlchemy engine with rolling 50-min OAuth refresh
  - `vector_search.py` — typed wrappers (`upsert_message`, `query_messages`, `query_documents`, `embed`)
  - `auth.py` — HS256 JWT issuance + hierarchical scope guards (admin/workspace/peer/session)
  - `llm_dispatch.py` — per-workspace LLM dispatcher (FMAPI / external / OpenAI-compatible / custom)
  - `lakebase_read.py` — Spark JDBC read helpers
  - `delta_write.py` — Delta upsert helpers
  - `tracing.py` — MLflow tracing init
- FastAPI app spine at `src/foreman/app/main.py` with lifespan and `/healthz`.
- Four worker job stubs with bundle resources in `resources/jobs.yml`.
- Apps runtime config at `app.yaml` with FMAPI defaults.
- Spine smoke test (`tests/spine_smoke.py`).
- 12 unit tests covering JWT scope semantics and config defaults.
- One-command install orchestrator (`install/install.py`).
- `.databricksignore`, `.gitignore`, `Makefile`, `pyproject.toml`, `requirements.txt`, `README.md`, `AGENTS.md`.

### Phase 2 — features (parallel agents)
- `feat(deriver)` — Spark job that pulls new messages from Lakebase via JDBC, batches by (workspace, observed-peer), calls deriver LLM for explicit-fact extraction, MERGEs into `documents_delta`. Watermarked + idempotent via deterministic doc IDs. (`b28c556`)
- `feat(jobs)` — Summarizer (rolling per-session summaries on N-message threshold) and Dreamer (surprisal-prioritized deduction → induction). (`96780ab`)
- `feat(webhooks)` — HMAC-signed delivery, retry classification, results recorded to `webhook_results_delta`. (`7044cd6`)
- `feat(routers)` — Workspaces / peers / sessions / messages / conclusions / keys / webhooks routers + Pydantic schemas. Inline embedding on message-create. JWT scope guards on every endpoint.
- `feat(dialectic)` — Agent loop with six tools (search_memory, search_messages, get_observation_context, grep_messages, get_messages_by_date_range, get_reasoning_chain) and five reasoning levels (minimal/low/medium/high/max). SSE streaming. MLflow tracing. Pre-fetch optimization injects top-5 self-collection documents.

### Phase 3 — integration (sequential)
- `ci+docs` — GitHub Actions CI (unit tests + lint + bundle validate), `PHASE3.md` integration checklist, webhook-reconciler skeleton. (`ff40a10`)
- `feat(integration)` — All routers + chat + fastapi-pagination + webhook reconciler wired in `app/main.py` lifespan. App exposes 40 routes. (`6c3e510`)

### Final state
- **171 unit tests pass**.
- **Bundle YAML validates** structurally (auth error in CI is environmental).
- **40 routes registered** across the FastAPI app.
- **6 commits** of feature implementation across the spine + 5 agents + integration.

### Known follow-ups (Phase-3 deferrals, captured in `worktree_notes.md`)
- Webhook results synced table (`webhook_results_delta` → `webhook_results_mirror`) — needs to be added to `schema_init` so the reconciler can mark queue items processed.
- `peer_cards` regeneration in dreamer — needs a `peer_cards_delta` synced table.
- `document.derived` webhook emission from the deriver job — Spark cannot enqueue to `queue_items`; do it from a follow-up Lakeflow task that reads new rows from `documents_delta` CDF.
- End-to-end deployment verification — requires a Databricks workspace with refreshed auth.
