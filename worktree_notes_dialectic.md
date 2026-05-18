# Phase 2 Agent B — Dialectic worktree notes

## Files added

| Path | Purpose |
|---|---|
| `src/foreman/app/dialectic/__init__.py` | Public surface: `run_agent_stream`, `AgentEvent`, `LEVELS`, `ReasoningLevel` |
| `src/foreman/app/dialectic/reasoning_levels.py` | Five level configs (tools enabled, role, max iterations, max output tokens) |
| `src/foreman/app/dialectic/prompts.py` | System prompt assembly: perspective section, peer-card injection, dynamic tool inventory; preliminary-context block helper |
| `src/foreman/app/dialectic/tools.py` | The six tools: specs (OpenAI function-calling shape) + async implementations + `dispatch()` with reasoning-level gate + result summarizer |
| `src/foreman/app/dialectic/agent.py` | Main loop: peer-card load, prefetch, message assembly, tool-calling rounds, forced final synthesis on iteration exhaustion, SSE event stream |
| `src/foreman/app/routers/chat.py` | `POST /workspaces/{w}/peers/{p}/chat` — SSE endpoint, `ChatRequest` Pydantic body, `require_peer` scope guard |
| `tests/unit/test_dialectic_reasoning_levels.py` | Tool gating, monotonic iteration budgets, role mapping |
| `tests/unit/test_dialectic_prompts.py` | Self vs directional perspective, peer-card injection, tool-inventory scoping to enabled tools |
| `tests/unit/test_dialectic_tools.py` | Spec validation, scope guard, all six tool implementations against mocked spine |
| `tests/integration/test_dialectic_loop.py` | Full SSE handshake through the FastAPI app with mocked LLM + spine; JWT scope rejection |

Untouched (per scope): all spine files, `main.py`, `schemas.py`, `requirements.txt`, all wave-1 jobs, all router files except `chat.py`.

## Test results

- `PYTHONPATH=src .venv/Scripts/python.exe -m pytest tests/unit/ -x` — **156 passed** (43 new dialectic tests; previously-passing tests still pass).
- `PYTHONPATH=src .venv/Scripts/python.exe -m pytest tests/integration/test_dialectic_loop.py -m integration` — **2 passed**.
- `python -c "from foreman.app.routers.chat import router; print('OK')"` — clean import.

## Spine bug / limitation found

**`llm_dispatch.chat()` cannot reach tool-calling endpoints through the
default `fmapi` provider.**

Mechanism: `llm_dispatch._invoke()` calls
`WorkspaceClient().serving_endpoints.query(...)`. That SDK method's
parameter list (verified via `inspect.signature`) does NOT include `tools=`
or `tool_choice=`. Any `tools=` we pass via `**kwargs` is dropped silently.
Tool calling only works via the `openai-compatible` provider branch
(`_invoke_openai`), which uses the OpenAI client and forwards `tools=`.

Implication for installs: for the dialectic agent to function the workspace
must have a `workspace_llm_config` row of the form

```sql
INSERT INTO workspace_llm_config (workspace_name, role, endpoint_name, provider)
VALUES ('<ws>', 'dialectic', '<endpoint>', 'openai-compatible');
```

A Databricks FMAPI endpoint name (e.g. `databricks-meta-llama-3-3-70b-instruct`)
works fine here because Databricks serving endpoints accept the OpenAI Chat
Completions wire format at `<host>/serving-endpoints/<name>/invocations`.

Phase-3 fixes I'd suggest:

1. Have `llm_dispatch._invoke()` route to `_invoke_openai` whenever `tools`
   is present in kwargs, regardless of resolved provider.
2. Document the requirement in the install README, OR
3. Default the install seed to `provider='openai-compatible'` for the
   `dialectic` role so out-of-the-box installs work.

The agent code itself is provider-agnostic — once dispatch routes correctly,
no agent change is needed.

## Other notes

- `databricks bundle validate` from this dev environment fails with an OAuth
  refresh-token error; that error is independent of the YAML (network-auth
  failure to the workspace, not a bundle-schema issue). My changes did not
  touch any `*.yml` so bundle structure is unaffected.
- No new dependencies added. `sse-starlette` was already in `requirements.txt`.
- `@trace` decorators applied to: `run_agent_stream` entry, `_prefetch_observations`,
  and each of the six tool implementations. MLflow autotracing of OpenAI client
  calls (set up in `tracing.init_tracing`) handles the LLM spans automatically.
- Tag-rich tracing: each tool span runs inside the agent span so `workspace_name`,
  `peer_name`, `session_name`, `reasoning_level` attributes propagate via the
  enclosing span context. Adding explicit `mlflow.update_current_trace(tags=...)`
  could be done in Phase 3 if a richer tag schema is desired.

## Deferred / out-of-scope

- The `get_observation_context` tool reads from `documents_mirror`, which is
  populated by the Delta→Lakebase synced table. The mirror schema matches
  `documents` so the agent works against either table; flipping the source
  is one-line if the synced table proves laggy in practice.
- Honcho's `search_messages_temporal` tool (semantic search + date filter) is
  not ported — the same shape can be obtained by combining `search_messages`
  with `get_messages_by_date_range`, and the spec said six tools.
- Honcho's `create_observations_deductive` write tool is intentionally out of
  scope; only the deriver and dreamer jobs write documents.
- No retry/backoff on LLM dispatch errors — the current behaviour surfaces
  the error as an SSE `error` event so the client can decide. Fine for v1.
