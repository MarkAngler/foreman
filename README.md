# foreman

A clean-room rebuild of [honcho.dev](https://honcho.dev) — Plastic Labs' identity & memory layer for AI agents — on the Databricks platform. Distributable as a Databricks Asset Bundle.

## Architecture at a glance

| Concern | Databricks primitive | Why |
|---|---|---|
| OLTP truth (workspaces, peers, sessions, messages, queue) | Lakebase Provisioned (Postgres) | ACID + ordered list semantics + composite keys; honcho's schema needs all three |
| Semantic recall on messages | Vector Search **Direct Access** index, written inline by the app | "Send a message, immediately query memory" needs <1s freshness |
| Semantic recall on derived facts | Vector Search **Delta Sync** index with managed embeddings | Deriver is async; minute-level latency is fine; no embedding job to maintain |
| LLM compute (dialectic, deriver, dreamer, summarizer, embeddings) | Model Serving — defaults to Foundation Model APIs, per-workspace overrides | One install template, many provider choices |
| REST API + dialectic agent | FastAPI on Databricks Apps | Native auth + access to UC + Lakebase + Vector Search + Model Serving |
| Background workers (deriver, summarizer, dreamer, webhooks) | Lakeflow Jobs (scheduled batch) | Spark scale + native UC integration; jobs read Lakebase via JDBC, write to Delta |
| Observability | MLflow Tracing | Native trace capture for the agent loop and downstream calls |
| Distribution | Databricks Asset Bundle | One `bundle deploy` per install |

Why not "just Vector Search"? Vector Search is an index, not a system of record. It can't enforce uniqueness, ordered list reads (`ORDER BY created_at` for message timelines), composite primary keys (every honcho table uses them), partial unique indexes (honcho's queue dedup), or transactional writes. Lakebase is mandatory.

## Install

Prereqs:
- Databricks workspace (any cloud; AWS regions: us-east-1/2, us-west-2, eu-central-1, eu-west-1, ap-south-1, ap-southeast-1/2)
- Databricks CLI v0.281.0+
- Python 3.11+
- Authenticated profile: `databricks auth login --profile DEFAULT`

One command:

```bash
python install/install.py
```

That runs:
1. `python install/bootstrap.py` — creates the Lakebase instance (~5 min), the Vector Search endpoint (~10 min), the secret scope, and writes the JWT signing key + bootstrap admin token to secrets.
2. `databricks bundle deploy` — deploys UC schema, the FastAPI app, and the four background jobs.
3. `databricks bundle run schema_init` — runs Alembic migrations against Lakebase, creates Delta tables, sets up Lakebase ↔ Delta synced tables, creates the two Vector Search indexes.
4. `databricks bundle run foreman_app` — starts the FastAPI app.

Total wall time: ~20 minutes for a fresh install (Lakebase + VS endpoint provisioning dominate).

After install:

```bash
# Retrieve the bootstrap admin JWT (use this to call all admin endpoints)
databricks secrets get-secret --scope foreman --key bootstrap_admin_token

# Get the app URL
databricks apps get foreman-dev --output json | jq -r .url
```

## Per-workspace LLM configuration

Each honcho workspace can override the default Model Serving endpoint per role (dialectic / deriver / dreamer / summarizer / embeddings). Defaults come from bundle variables and apply to any workspace that hasn't set an override.

```sql
-- Connect to Lakebase via the Databricks SQL editor or psql
INSERT INTO workspace_llm_config (workspace_name, role, endpoint_name, provider, params)
VALUES
  ('acme', 'dialectic', 'my-anthropic-claude-sonnet-4', 'external',
   '{"max_tokens": 2048, "temperature": 0.3}'::jsonb),
  ('acme', 'deriver',   'databricks-meta-llama-3-3-70b-instruct', 'fmapi', '{}'::jsonb);
```

Providers:
- `fmapi` — Databricks Foundation Model APIs pay-per-token (default).
- `external` — Any external endpoint registered in Model Serving (Anthropic, OpenAI, etc.).
- `openai-compatible` — Any endpoint that speaks the OpenAI Chat Completions wire format.
- `custom` — A custom Model Serving endpoint (your fine-tuned model, ChatAgent, etc.).

To use Anthropic, register an external Model Serving endpoint with your Anthropic key (one-time, manual — DABs cannot create Model Serving endpoints):

```bash
databricks serving-endpoints create my-anthropic-claude-sonnet-4 \
  --json '{
    "config": {
      "served_entities": [{
        "name": "claude-sonnet-4",
        "external_model": {
          "name": "claude-sonnet-4-20250514",
          "provider": "anthropic",
          "task": "llm/v1/chat",
          "anthropic_config": {
            "anthropic_api_key": "{{secrets/foreman/anthropic_api_key}}"
          }
        }
      }]
    }
  }'
```

## JWT issuance

All API access is authenticated by HS256 JWTs with hierarchical scopes (admin > workspace > peer/session). The bootstrap admin JWT is generated at install time. Use it to call `POST /workspaces/{w}/keys` to issue scoped tokens for downstream consumers.

```bash
ADMIN=$(databricks secrets get-secret --scope foreman --key bootstrap_admin_token | jq -r .value | base64 -d)
APP_URL=$(databricks apps get foreman-dev --output json | jq -r .url)

# Issue a workspace-scoped token (full access to one workspace)
curl -X POST "$APP_URL/workspaces/acme/keys" \
  -H "Authorization: Bearer $ADMIN" \
  -d '{"scope": "workspace", "ttl_seconds": 2592000}'

# Issue a session-scoped token (only message-create within one session)
curl -X POST "$APP_URL/workspaces/acme/keys" \
  -H "Authorization: Bearer $ADMIN" \
  -d '{"scope": "session", "session_name": "chat-1", "ttl_seconds": 86400}'
```

Token claims: `{ad?: bool, w?: str, p?: str, s?: str, t: int, exp: int}`. Stored signing key never leaves the Databricks secret scope.

## Background workers

Four Lakeflow Jobs deployed by the bundle. All start in `PAUSED` state — un-pause them after install:

```bash
databricks jobs reset --json '{"job_id": <id>, "new_settings": {"schedule": {"pause_status": "UNPAUSED", ...}}}'
# or via the workspace UI
```

| Job | Cadence | What it does |
|---|---|---|
| `deriver` | Every minute | Pulls new messages from Lakebase via JDBC, batches by (workspace, observer, observed), extracts explicit facts via LLM, writes to `documents_delta`, triggers `documents_idx` sync |
| `summarizer` | Every 5 min | For sessions that have crossed N new messages since last summary, calls LLM, appends to `session_summaries_delta` |
| `dreamer` | Hourly | Surprisal-prioritized deduction → induction passes; writes deductive/inductive documents linked via `source_ids`; updates peer cards |
| `webhooks` | Every minute | Pulls pending webhook items, POSTs to registered endpoints with HMAC signatures, records results to `webhook_results_delta` |

## Layout

```
databricks.yml             Bundle entrypoint
resources/                 DAB resource YAML
  catalog.yml              UC schema
  jobs.yml                 schema_init + 4 worker jobs
  app.yml                  Databricks App
  secrets.yml              Placeholder; real secrets created by bootstrap
install/
  bootstrap.py             Pre-deploy SDK calls (Lakebase, VS endpoint, secrets)
  install.py               One-command orchestrator
schema/
  alembic.ini, env.py, script.py.mako
  versions/0001_initial_schema.py    All 11 honcho tables + workspace_llm_config + documents_mirror
src/foreman/
  __init__.py
  lib/                     Shared helpers (frozen contracts)
    config.py              Settings — env-driven config
    lakebase.py            asyncpg pool + 50-min OAuth refresh
    vector_search.py       Typed wrappers + embed()
    auth.py                JWT issuance + scope guards
    llm_dispatch.py        Per-workspace LLM dispatcher
    lakebase_read.py       Spark JDBC reads
    delta_write.py         Delta upsert helpers
    tracing.py             MLflow tracing init
  app/
    main.py                FastAPI lifespan + /healthz; routers wired in Phase 3
    routers/               Implemented in Phase 2 by Agent A
    dialectic/             Implemented in Phase 2 by Agent B (chat router included)
    schemas.py             Pydantic request/response models (Agent A)
  jobs/
    schema_init/main.py    Migrations + Delta + synced tables + VS indexes
    deriver/               Implemented in Phase 2 by Agent C
    summarizer/            Implemented in Phase 2 by Agent D
    dreamer/               Implemented in Phase 2 by Agent D
    webhooks/              Implemented in Phase 2 by Agent E
tests/
  spine_smoke.py           Phase-1 acceptance test
  unit/                    Pure-logic tests (no Databricks needed)
  integration/             Tests against deployed bundle (require auth + bundle)
app.yaml                   Databricks Apps runtime config
requirements.txt           App + jobs deps
```

## Development

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt pytest pytest-asyncio

# Unit tests (no Databricks required)
PYTHONPATH=src .venv/Scripts/python -m pytest tests/unit/ -v

# Spine smoke test (requires deployed bundle + auth)
PYTHONPATH=src .venv/Scripts/python -m pytest tests/spine_smoke.py -s

# Integration tests (requires deployed bundle + auth)
PYTHONPATH=src .venv/Scripts/python -m pytest tests/integration/ -m integration -v

# Validate bundle
databricks bundle validate
```

## Verification checklist (post-install)

1. App URL responds to `GET /healthz` with `{"status": "ok"}`.
2. `psql` against Lakebase shows all 11 tables: workspaces, peers, sessions, session_peers, messages, documents, queue_items, active_queue_session, webhook_endpoints, peer_cards, workspace_llm_config (+ documents_mirror).
3. Vector Search lists `messages_idx` (Direct Access) and `documents_idx` (Delta Sync) under endpoint `foreman-vs`, both ONLINE.
4. `POST /workspaces` returns 201; `POST /workspaces/list` shows the new workspace.
5. Create two peers, a session containing both, append messages from each. Within 2s, `POST /workspaces/{w}/sessions/{s}/search` finds the messages by content (validates inline embedding + Direct Access write path).
6. After ~60s, `documents_delta` has rows with `level='explicit'` (validates deriver job).
7. `POST /workspaces/{w}/peers/{p}/chat` streams an SSE response. The MLflow trace shows tool calls including `search_memory` and `search_messages`.
8. Insert a `workspace_llm_config` row pointing the dialectic role at a different endpoint; re-run chat; trace shows the new endpoint.
9. Issue a session-scoped JWT; cross-session message create returns 403; in-scope create returns 201.
10. Register a webhook endpoint to a request-bin URL; create a message; the POST is delivered with a valid `X-Foreman-Signature` header within 60s.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `bundle validate` shows `error getting token: refresh token invalid` | Databricks CLI auth expired | `databricks auth login --profile DEFAULT` |
| `bootstrap.py` hangs on Lakebase creation | Provisioning genuinely takes ~5m | Be patient; check workspace UI > Compute > Lakebase |
| `bootstrap.py` hangs on VS endpoint creation | Storage-Optimized provisioning takes ~10m | Check workspace UI > Compute > Vector Search |
| `schema_init` fails on `CREATE DATABASE` | User lacks Lakebase superuser | Grant CREATE on the Lakebase instance to the deploying principal |
| App fails to start with `RuntimeError: lakebase engine not initialized` | Lakebase resource not bound to app | Check `resources/app.yml` declares the database resource; redeploy |
| Vector Search query returns 0 results immediately after upsert | Direct Access write hasn't propagated | Allow ~1-2s before querying; if still empty, check the index `online` status |
| `databricks-sdk` import error in jobs | Job environment dependencies not installed | Check `resources/jobs.yml` `environments[].spec.dependencies` includes the SDK pin |
| Per-workspace endpoint override not taking effect | Cached resolution (none in v1) or wrong role string | Verify `(workspace_name, role)` row exists in `workspace_llm_config`; role is one of `dialectic`/`deriver`/`dreamer`/`summarizer`/`embeddings` |

## Constraints (v1)

- **No Lakebase RLS**. Auth is enforced at the API boundary (matching honcho's design). Anyone with direct Lakebase access bypasses honcho ACLs. If multi-tenant isolation matters at the storage layer, add Postgres RLS in a follow-up migration.
- **No Redis cache**. Honcho ships an optional Redis cache; not ported. Lakebase is fast enough for the access patterns; revisit if profiling shows otherwise.
- **DABs cannot create Model Serving endpoints**. The install template uses FMAPI endpoints (which exist by default in every workspace). Custom endpoints (Anthropic, OpenAI, fine-tunes) are a manual post-install step (`databricks serving-endpoints create ...`).
- **No multi-region**. Lakebase + VS endpoint are single-region. Multi-region is a v2+ concern.

## Plan

Full architecture and build plan: [`C:\Users\marka\.claude\plans\sorted-weaving-yao.md`](C:\Users\marka\.claude\plans\sorted-weaving-yao.md)

## License

(TBD)
