# worktree_notes.md

Notes from Phase 2 agents. Each section is owned by one agent.

---

## Agent E — Webhooks Job

### Files added

- `src/foreman/jobs/webhooks/delivery.py` — pure delivery primitives: payload assembly, HMAC-SHA256 signing, retry classification, single httpx POST attempt.
- `src/foreman/jobs/webhooks/results.py` — `webhook_results_delta` table CREATE + append helper.
- `src/foreman/jobs/webhooks/main.py` — orchestrator: reads `queue_items` and `webhook_endpoints` from Lakebase, fans out signed POSTs, appends per-attempt rows to `webhook_results_delta`.
- `tests/unit/test_webhooks_delivery.py` — 30 unit tests covering signature, retry classification, header construction, event filtering.
- `tests/unit/test_webhooks_main.py` — 13 unit tests covering orchestration: payload parsing, endpoint grouping, event filtering, attempt counting, max-attempts cap, signature inclusion, multi-endpoint fan-out.
- `tests/integration/test_webhooks_delivery.py` — `@pytest.mark.integration` round-trip test using `httpx.MockTransport` for the full sign+POST+verify path.
- `tests/integration/__init__.py` — package marker.

### Files modified

- `pyproject.toml` — registered the `integration` pytest marker. No dependency or build changes.

### Spine bugs

None found.

### Deferred items (Phase 3)

1. **Mark-processed handshake.** This job records delivery results to `webhook_results_delta` but cannot update `queue_items.processed` in Lakebase (no JDBC writes from Spark, per spine contract). Phase 3 needs a reconciler that, for each `queue_item_id` in `webhook_results_delta`:
   - Treats the item as processed when *every* matching endpoint has a row with `success=true` **or** `attempt_count >= 5`.
   - Updates `queue_items.processed = TRUE, processed_at = NOW()` via the FastAPI app's async pool (which holds Lakebase write privileges) — easiest place is a small endpoint or a periodic background task in `src/foreman/app/`.
   - Until that reconciler ships, the same queue item will be re-read and re-delivered every job run; the per-(item, endpoint) `attempt_count` cap (5) prevents unbounded retries to the same endpoint, and successful endpoints will record duplicate success rows on subsequent runs. Acceptable for v1; document it in the README before public release.

2. **Reverse-ETL of `webhook_results_delta` → Lakebase `webhook_results_mirror`.** Add a synced table in `resources/lakebase.yml` (Phase 3 spine extension) so the reconciler in #1 can read results without crossing into Spark. Schema = same columns as `webhook_results_delta`.

3. **Webhook signature replay protection.** Honcho doesn't include a timestamp inside the signed body specifically for replay defence — `occurred_at` is informational. If consumers need replay protection, add an `X-Foreman-Timestamp` header and include it in the HMAC input. Out of scope for v1.

4. **Per-endpoint failure backoff.** Today every job run retries every still-eligible endpoint immediately. Phase 3 could add an exponential delay (e.g. don't retry if `now() - max(attempted_at) < 2^attempt_count seconds`). The data is already in `webhook_results_delta` to compute it.

### Expected event types (producer-side contract for Agents A/B/C/D)

The orchestrator is event-name-agnostic, but `webhook_endpoints.event_types` filtering means producers must pick names that consumers can subscribe to. Suggested vocabulary, ported from honcho conventions:

- `message.created` — emitted by the messages router after a message is appended. `data`: `{message_id, public_id, session_name, peer_name}`.
- `session.summarized` — emitted by the summarizer job after writing a row to `session_summaries_delta`. `data`: `{session_name, summary_id, messages_covered}`.
- `document.derived` — emitted by the deriver after writing explicit-fact documents. `data`: `{document_ids, observer, observed, count}`.
- `document.deduced` / `document.induced` — emitted by the dreamer for higher-level documents. `data`: `{document_ids, observer, observed, level}`.
- `peer.created`, `session.created`, `workspace.created` — emitted by the relevant routers in Agent A's scope.
- `queue.empty` — honcho-compat signal that a work_unit's deriver queue has drained.

### Producer payload shape (what to insert into `queue_items.payload` with `task_type='webhook'`)

```json
{
  "event_type": "message.created",
  "workspace_name": "ws-alpha",
  "occurred_at": "2026-05-15T12:00:00+00:00",
  "data": {"message_id": "m_abc", "session_name": "s1", "peer_name": "p1"}
}
```

`occurred_at` is optional; when missing the webhook job stamps it at delivery time. `data` is forwarded verbatim. Use `work_unit_key = f"webhook:{event_type}:{some_unique_id}"` so the partial unique index on `queue_items (work_unit_key, task_type) WHERE processed=false` enforces dedup as designed.

### HMAC verification (consumer-side recipe to publish in README)

```python
import hmac, hashlib
expected = "sha256=" + hmac.new(secret.encode(), request.body, hashlib.sha256).hexdigest()
assert hmac.compare_digest(expected, request.headers["X-Foreman-Signature"])
```

Body is serialized with `sort_keys=True, separators=(",", ":")` so the signature is reproducible regardless of dict order on either side.

### New dependencies

None. `httpx` was already in `requirements.txt` and `pyproject.toml`.

### Verification

- `PYTHONPATH=src .venv/Scripts/python.exe -m pytest tests/unit/ -x` — 63 passed (12 baseline + 30 webhooks delivery + 13 webhooks main + 8 deriver from Agent C).
- `from foreman.jobs.webhooks.main import main` — imports cleanly.
- `pytest tests/integration/ -m integration` — 3 passed (mocked transport round-trip).

---

## Agent C — Deriver Job

### Files added

- `src/foreman/jobs/deriver/prompts.py` — port of honcho's `minimal_deriver_prompt` (verbatim body) plus `format_message_turn` and a JSON-output instruction block. Honcho enforces structured output via `honcho_llm_call(response_model=..., json_mode=True)`; we go through `chat_sync`, so the JSON contract has to live in the prompt.
- `src/foreman/jobs/deriver/observer_observed.py` — `MessageRow` dataclass + `build_batches` that groups messages by `(workspace, session, observed)` and resolves observers from `session_peers`. Mirrors honcho's `generate_queue_records` rule: every active session peer becomes an observer; the observed peer is hoisted to the front and never duplicated.
- `src/foreman/jobs/deriver/structured_output.py` — Pydantic `PromptRepresentation` + `parse_response` (tolerates ```json fences and surrounding prose) + `build_document_drafts` (fans one fact across all observers) + deterministic `document_id = sha256(workspace|observer|observed|content)[:24]`.
- `tests/unit/test_deriver_observer_observed.py` — 8 unit tests covering self-observation, fan-out, multi-session isolation, dedup, sorting, orphan-session fallback.
- `tests/unit/test_deriver_structured_output.py` — 14 unit tests covering JSON parsing edge cases, ID determinism, draft fan-out.
- `tests/unit/test_deriver_prompts.py` — 6 unit tests covering prompt content, custom-instructions handling, message turn formatting.
- `tests/unit/test_deriver_main.py` — 13 unit tests covering LLM response shape extraction, datetime coercion, batch orchestration via `assemble_drafts`.
- `tests/integration/test_deriver_end_to_end.py` — `@pytest.mark.integration` end-to-end test that stubs `pyspark.sql` in `sys.modules` and exercises `main()` from watermark read through Vector Search sync.

### Files modified

- `src/foreman/jobs/deriver/main.py` — replaced the stub. Implements: watermark Delta KV (`deriver_state`), JDBC read of new messages (per the plan's note that Lakebase->Delta sync latency may exceed the deriver cadence), per-(observed) LLM dispatch, MERGE into `documents_delta`, Vector Search trigger, watermark advance.

### Design choices

- **JDBC reads, not CDF streaming.** The plan flags this as a contingency under "Risks / open verification items" — Lakebase->Delta sync latency observed in practice can exceed the deriver's cadence. Direct reads from `messages` keep freshness deterministic and let us sit on serverless cron without a streaming runtime.
- **Deterministic document IDs.** `sha256(workspace|observer|observed|content)[:24]` so a re-run on the same range MERGEs as a no-op. Combined with the watermark this gives at-least-once + idempotent => effectively-once writes.
- **Per-batch LLM error isolation.** A bad LLM response for one (observed) group is logged and the group is skipped; the rest of the batch and the watermark advance still happen. Prevents one stuck group from poisoning the cadence.
- **Custom instructions wired but unused for v1.** `minimal_deriver_prompt` accepts `custom_instructions`, but `assemble_drafts` doesn't yet read `peer_configuration.deriver.custom_instructions` from Lakebase. See deferred item #2.
- **`assemble_drafts` is pure.** Spark imports stay inside `process_batch`, `build_doc_dataframe`, and `main()` so unit tests can exercise the orchestration without pyspark in the dev venv.

### Spine bugs

None found. `lakebase_read.read_query`, `delta_write.upsert_documents`, `delta_write.ensure_documents_delta`, `vector_search.trigger_documents_sync`, and `llm_dispatch.chat_sync` all worked as documented.

### Deferred items (Phase 3)

1. **`document.derived` webhook emission.** Per Agent E's payload contract, after each successful `documents_delta` write the deriver should enqueue a `webhook` queue item with `event_type='document.derived'`, `data={document_ids, observer, observed, count}`. Today the job writes Delta and triggers Vector Search but does not write to `queue_items` (no JDBC writes from Spark). Easiest landing spot: the FastAPI app exposes a small admin endpoint the job can hit, or the documents Delta -> Lakebase sync downstream triggers an enqueue.
2. **Per-peer `custom_instructions` resolution.** Honcho reads `reasoning.custom_instructions` from `peer_configuration` / `session_peer_configuration` to customise the deriver prompt per peer. Plumbing it requires a JDBC read of `peers.configuration` (and `session_peers.configuration`) keyed by the observed peer; the prompt module already accepts the field.
3. **`session_summaries` as additional context.** Honcho's deriver also pulls the most recent summary per session and injects it into the prompt as background. Our schema has `session_summaries_delta`; once the summarizer is producing rows, fetch the latest summary per session in `fetch_new_messages` and pass it as a separate prompt block.
4. **Adaptive batch size.** `DEFAULT_BATCH_LIMIT = 5000` is a flat cap. Under bursty load this can queue work behind the cron interval. Consider draining in a `while messages: ... watermark = max(...)` loop within a single invocation, capped by wall-clock budget.
5. **Telemetry parity with honcho.** Honcho records per-batch metrics (`context_preparation_ms`, `llm_call_ms`, `total_duration_ms`, `explicit_conclusion_count`) as named events. We use `@trace` on `main` and `extract_facts` only. A Phase 3 pass should add structured `mlflow.log_metric` calls inside `assemble_drafts`.
6. **Backwards-compatible source_ids reasoning chain.** Honcho's `documents.source_ids` references *document* IDs for derived levels; for `level='explicit'` it references *message* `public_id`s. We follow the same convention. The dreamer (Agent D) needs to honour this when traversing.

### Source attribution

- Prompt body: `https://github.com/plastic-labs/honcho/blob/main/src/deriver/prompts.py` (`minimal_deriver_prompt`).
- Observer/observed rule: `https://github.com/plastic-labs/honcho/blob/main/src/deriver/enqueue.py` (`generate_queue_records`).
- LLM batch shape: `https://github.com/plastic-labs/honcho/blob/main/src/deriver/deriver.py` (`process_representation_tasks_batch`).
- Pydantic schema: `https://github.com/plastic-labs/honcho/blob/main/src/utils/representation.py` (`PromptRepresentation`, explicit-only).

### New dependencies

None. Job runtime already pulls `databricks-sdk`, `psycopg`, `pydantic`, `pydantic-settings`, `mlflow`. PySpark is provided by Databricks serverless.

### Verification

- `PYTHONPATH=src .venv/Scripts/python.exe -m pytest tests/unit/ -x` — 96 passed (Agent C contributes 41 new tests; pre-existing 12 baseline + 30 + 13 from Agent E all green; Agent B's `test_dialectic_prompts.py` had 1 failure unrelated to this worktree).
- `from foreman.jobs.deriver.main import main` — imports cleanly.
- `pytest tests/integration/test_deriver_end_to_end.py -m integration` — 2 passed.
