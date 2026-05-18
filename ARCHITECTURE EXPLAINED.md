# Foreman architecture, explained

A mental model for how Lakebase Postgres, Vector Search, and UC Delta tables fit together — plus deep dives on the parts that aren't obvious.

---

## 1. The three storage layers, by purpose

Think of the system like a brain:

| Layer | Role | Latency |
|---|---|---|
| **Lakebase Postgres** | The **ledger** — system of record. Transactional truth. | Sync, durable |
| **Vector Search** (`foreman-vs`) | The **recall layer** — semantic memory. Fuzzy lookup. | ~ms reads |
| **UC Delta tables** (`foreman.memory.*`) | The **derived knowledge** — batch-processed insights. | Minute/hour latency |

**Key rule**: Lakebase is the only source of truth. Delta and Vector Search are downstream views. You can rebuild them from Lakebase + messages; you cannot rebuild Lakebase from them.

---

## 2. What lives where

**Lakebase Postgres (11 tables + 1 mirror)** — the boring durable stuff, queried only by jobs (JDBC) and by CRUD endpoints for metadata:
- Identity: `workspaces`, `peers`, `sessions`, `session_peers`
- Conversation: `messages` (raw text, append-only)
- Operational: `queue_items`, `active_queue_session`, `webhook_endpoints`
- Knowledge cache: `peer_cards`, `documents_mirror` (reverse-ETL'd from Delta)
- Config: `workspace_llm_config`

**Vector Search — two indexes, two flavors**:
- `messages_idx` — **Direct Access**. FastAPI embeds + upserts inline when a message is created. The "send-then-immediately-query-memory" path needs <1s freshness, so the app writes here itself.
- `documents_idx` — **Delta Sync** with managed embeddings, sourced from `documents_delta`. Databricks handles embedding + index refresh. Minute-level latency is fine because documents are derived async.

**UC Delta tables** — written by Spark jobs only:
- `documents_delta` ← deriver job (extracted facts: explicit, deductive, inductive)
- `session_summaries_delta` ← summarizer job
- `webhook_results_delta` ← webhooks job
- `deriver_state`, `summarizer_state_delta` — watermarks

---

## 3. The two flows that explain everything

### Flow A — write path (when a message arrives)

`POST /workspaces/{w}/sessions/{s}/messages` ([messages.py:41](src/foreman/app/routers/messages.py#L41))

```
client ──► FastAPI
            │
            ├─1─► Lakebase.messages  (INSERT, durable truth)
            │
            ├─2─► embed(content)  (Foundation Model API, 1024-dim)
            │
            └─3─► messages_idx.upsert  (Direct Access, inline)
                  ◄── 201 returned only after all three succeed
```

Async, in the background, four jobs are doing their own thing:
- **deriver** (1 min) reads new `messages` rows by watermark → LLM extracts facts → MERGE into `documents_delta` → Databricks syncs to `documents_idx` (Delta Sync) AND reverse-syncs back to Lakebase `documents_mirror`.
- **summarizer** (5 min) tails messages → LLM summarizes → APPENDs `session_summaries_delta`.
- **dreamer** (hourly) does surprisal-prioritized deduction, updates `peer_cards`.
- **webhooks** (1 min) drains `queue_items` → HTTP POST with HMAC → appends `webhook_results_delta`.

### Flow B — read path (when something queries memory)

The dialectic agent and the query endpoints **never read Lakebase for memory**. Memory = Vector Search.

```
query ──► Vector Search
            ├─ query_messages()   → messages_idx   (raw conversation recall)
            └─ query_documents()  → documents_idx  (derived facts recall)
```

Lakebase is only read by the app for **CRUD metadata** (list workspaces, get peer config, resolve LLM endpoint) and by **jobs** for watermarked tailing.

---

## 4. Why derived knowledge lives in Delta (not Lakebase, not VS directly)

Four reasons that all point the same direction:

### 4.1 Spark writes Delta natively; Lakebase via JDBC is the wrong tool
The deriver, summarizer, and dreamer are Spark jobs. Spark's first-class write target is Delta — parallel, MERGE-friendly, transactional. Writing thousands of rows per run from Spark to Postgres via JDBC is serial, brittle, and fights the cluster's parallelism. **Right tool for the writer.**

### 4.2 The `documents_idx` Delta Sync index *requires* a Delta source
`documents_idx` uses **managed embeddings** — Databricks does the embedding for you on every sync. That pipeline can only read from a UC Delta table with Change Data Feed. You literally cannot point a Delta Sync VS index at Postgres or write to it directly the way `messages_idx` does. So if you want managed embeddings (and you do — derived facts are huge volume), **Delta is mandatory upstream**.

### 4.3 Lakebase shouldn't absorb batch write pressure
Lakebase is provisioned for the **hot OLTP path** — the FastAPI app must stay responsive on message create, peer lookup, JWT auth. If the deriver dumped thousands of facts per minute straight into Lakebase, you'd contend with the request path for connections, locks, and WAL. Delta absorbs the batch load on completely separate storage and compute. **Workload isolation.**

### 4.4 Delta gives you a replayable derived ledger
- **Re-embedding**: if you swap the embedding model, you re-sync `documents_idx` from `documents_delta` cleanly. The facts themselves don't have to be re-derived.
- **Analytics**: "how many facts did the deriver extract this week, by level?" runs cheap on a SQL warehouse against Delta. Running that against Lakebase competes with OLTP.
- **Re-derivation idempotency**: deterministic sha256 PKs + MERGE means deriver can replay safely.

### The symmetry that makes it memorable

```
Raw conversation:    Lakebase.messages  ──►  messages_idx        (app writes both)
                     [truth]                  [view, fast freshness]

Derived knowledge:   documents_delta    ──►  documents_idx       (jobs write Delta, Databricks syncs)
                     [truth]                  [view, managed embeddings]
                            └──────────► documents_mirror        (Lakebase view via Lakeflow sync)
                                          [view, transactional reads]
```

**Each storage layer owns one workload it's good at.** Lakebase = OLTP. Delta = batch + analytics + immutable derived ledger. Vector Search = ANN recall. Synced tables and Direct Access upserts are the *bridges* between them, not the truth.

**One-line answer**: derived knowledge lives in Delta because that's where Spark wants to write, where Delta Sync VS needs to read, and where Lakebase doesn't want to be bothered.

---

## 5. Deep dive — `documents_delta`

`documents_delta` is the **derived knowledge ledger** — a Delta table in Unity Catalog where the deriver job writes every fact it has extracted from conversations. It's the upstream of two downstream views: `documents_idx` (Vector Search) and `documents_mirror` (Lakebase synced table).

### 5.1 The schema

From [delta_write.py:18-40](src/foreman/lib/delta_write.py#L18-L40):

```sql
CREATE TABLE documents_delta (
    id              STRING,        -- deterministic sha256 PK
    workspace_name  STRING,
    observer        STRING,        -- the peer who "knows" this fact
    observed        STRING,        -- the peer the fact is about
    level           STRING,        -- explicit | deductive | inductive | contradiction
    content         STRING,        -- the fact itself, plain text
    source_ids      ARRAY<STRING>, -- message public_ids that produced it
    times_derived   INT,           -- bumped if re-extracted later
    metadata        STRING,        -- JSON: {"session_name": "..."}
    created_at      TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
```

### 5.2 The observer/observed concept

Honcho's mental model: **every peer maintains their own private picture of every other peer**, including themselves. A "fact" is never bare — it's always *"observer X believes Y about observed Z"*.

- **observed** = the peer who *sent* the message — they're the subject of the fact.
- **observers** = every peer currently in the session, including the sender themselves (self-observation).

Example: in a session with peers **Alice**, **Bob**, **Carol**, Alice sends `"I'm vegetarian."` The deriver extracts the fact `"is vegetarian"` and writes **three rows** in `documents_delta`:

| observer | observed | content |
|---|---|---|
| Alice | Alice | is vegetarian |  ← Alice's self-knowledge
| Bob   | Alice | is vegetarian |  ← what Bob now knows about Alice
| Carol | Alice | is vegetarian |  ← what Carol now knows about Alice

This fan-out is what lets the dialectic agent answer questions like *"What does Bob know about Alice?"* — query with `observer='Bob' AND observed='Alice'`. Each peer's worldview is materialized separately, so privacy and perspective both fall out naturally.

The fan-out happens in [structured_output.py:81-96](src/foreman/jobs/deriver/structured_output.py#L81-L96) — one extracted fact times N observers = N rows.

### 5.3 The deriver pipeline — start to finish

1. **Read watermark** — `deriver_state` Delta KV table stores `last_processed_message_id` ([main.py:76-85](src/foreman/jobs/deriver/main.py#L76-L85)).
2. **Pull next message batch from Lakebase** via JDBC ([main.py:107-118](src/foreman/jobs/deriver/main.py#L107-L118)):
   ```sql
   SELECT id, public_id, session_name, peer_name, workspace_name, content, created_at
   FROM messages WHERE id > {watermark} ORDER BY id LIMIT 5000
   ```
3. **Resolve observers** per (workspace, session) by querying `session_peers` with `left_at IS NULL`.
4. **Group + call the LLM** by `(workspace, session, observed=sender)`. Each group → one `llm_dispatch.chat_sync(role="deriver", ...)` call. Response is JSON:
   ```json
   {"explicit": [{"content": "is vegetarian"}, {"content": "lives in Berlin"}]}
   ```
5. **Parse + fan out** facts across observers ([structured_output.py:67-97](src/foreman/jobs/deriver/structured_output.py#L67-L97)).
6. **Deterministic ID** — `sha256(workspace|observer|observed|content)[:24]` ([structured_output.py:100-104](src/foreman/jobs/deriver/structured_output.py#L100-L104)). Same tuple → same ID → MERGE deduplicates for free.
7. **MERGE into Delta** ([delta_write.py:59-71](src/foreman/lib/delta_write.py#L59-L71)):
   ```sql
   MERGE INTO documents_delta t
   USING _foreman_doc_upsert u
   ON t.id = u.id
   WHEN MATCHED THEN UPDATE SET *
   WHEN NOT MATCHED THEN INSERT *
   ```
8. **Trigger VS sync** — `vector_search.trigger_documents_sync()`. Best-effort; failures don't block.
9. **Advance watermark** to `max(message.id)` in batch.

### 5.4 How `documents_delta` becomes the two downstream views

```
                    deriver job (every 1 min)
                          │ MERGE
                          ▼
                  ┌─────────────────┐
                  │ documents_delta │   ◄── source of truth for derived facts
                  │   (UC Delta)    │
                  └─────────────────┘
                    │              │
       Delta Sync   │              │   Lakeflow synced table
        (managed    │              │   (reverse ETL)
        embeddings) ▼              ▼
              ┌──────────┐   ┌──────────────────┐
              │documents_│   │ documents_mirror │
              │   idx    │   │   (Lakebase)     │
              │  (VS)    │   │                  │
              └──────────┘   └──────────────────┘
               ▲                    ▲
               │ ANN query          │ app reads for FK joins,
               │ from app           │ transactional access
```

**Forward path**: Databricks Delta Sync pipeline reads `documents_delta` CDF, embeds the `content` column with the `databricks-gte-large-en` model, writes vectors to `documents_idx`. Triggered, not continuous — kicked off by the deriver after each batch.

**Reverse path** ([schema_init/main.py:94-123](src/foreman/jobs/schema_init/main.py#L94-L123)): a Lakeflow `SyncedDatabaseTable` mirrors `documents_delta` → Lakebase `documents_mirror` so the FastAPI app can join against documents transactionally without going through JDBC-to-Delta.

### 5.5 Why CDF (`delta.enableChangeDataFeed = 'true'`) matters

Without Change Data Feed, downstream consumers would have to re-scan the whole table on each refresh. CDF emits a stream of *just* the rows that changed in each commit, so:
- `documents_idx` only re-embeds new/updated rows.
- `documents_mirror` only pushes deltas to Lakebase, not the whole table.

This is what makes the "minute-level latency" budget feasible at any scale.

### 5.6 The four invariants that make it work

1. **`id` is content-addressed.** Same fact, same ID. MERGE deduplicates for free.
2. **Observer/observed are always present.** Every fact carries its perspective; queries are scoped to `(observer, observed)` pairs.
3. **`source_ids` preserves provenance.** Every fact points back to the message `public_id`s that produced it — debuggability and re-derivation are possible.
4. **Watermark advances only after a successful MERGE.** Crash mid-batch → next run reprocesses the same window → idempotent IDs make that safe.

---

## 6. Deep dive — the webhooks job

The webhooks job is the **outbound notification system** — the way foreman tells external services that something interesting happened. Think Stripe webhooks: when a payment succeeds, Stripe POSTs to your URL. Same pattern.

### 6.1 Why it exists at all

Without webhooks, external systems would have to **poll** the foreman API constantly to find out what changed. That's slow, expensive, and racy. Webhooks let foreman push the change at most ~1 minute after it happens.

### 6.2 The pieces (three tables and a job)

```
1. webhook_endpoints   (Lakebase)   ← workspace owners register URLs + secrets + event filters here
2. queue_items         (Lakebase)   ← the app appends rows when an event happens (task_type='webhook')
3. webhook_results_delta (Delta)    ← the job appends delivery outcomes here
```

### 6.3 The end-to-end flow

```
[ foreman event ]  ──►  queue_items (Lakebase)
                              │
                              ▼
                       webhooks job (every 1 min)
                              │
                              ▼
              POST /your-url   X-Foreman-Event: <type>
                               X-Foreman-Signature: sha256=<hmac>
                               body: {event_type, workspace_name, occurred_at, data}
                              │
                              ▼
                       webhook_results_delta
```

The job (Spark, every 1 min):

1. **Poll** `queue_items` for `task_type='webhook' AND processed=FALSE` ([main.py:43-47](src/foreman/jobs/webhooks/main.py#L43-L47)).
2. **Load** all `webhook_endpoints`, group by workspace.
3. **For each queued event**, find endpoints in the same workspace whose `event_types` filter matches.
4. **Build signed payload**: stable-sorted JSON body, `X-Foreman-Signature: sha256=<hmac>` using the endpoint's secret, `X-Foreman-Event: <type>` header.
5. **POST it** via `httpx` with a 30s timeout. Classify the response:
   - 2xx → success
   - 408/429/5xx → retryable
   - other 4xx → permanent failure (your endpoint is broken; don't retry)
6. **Append outcome** to `webhook_results_delta` — never raises, every attempt is recorded.

### 6.4 Two subtle but important design choices

**Retries are bounded across job runs, not per run.** The job counts prior attempts from `webhook_results_delta` ([main.py:73-81](src/foreman/jobs/webhooks/main.py#L73-L81)) and skips anything that already has ≥ 5 attempts. So even though the job runs every minute, a permanently failing endpoint isn't hammered forever.

**Spark can't write to Lakebase from this job** (frozen spine rule — Spark writes Delta, period). So `queue_items.processed` is **not** flipped here. Instead, outcomes go to `webhook_results_delta`, and a Phase-3 **reconciler** is what eventually reads those rows and updates `queue_items.processed=TRUE` back in Lakebase.

### 6.5 Real-world use cases

**Operational notifications**
- Slack alerts when high-priority messages arrive
- PagerDuty for agent failures

**CRM / customer data platform sync**
- Push inferred peer facts to Salesforce / HubSpot
- Trigger Customer.io campaigns on dreamer-derived insights

**Analytics & data warehousing**
- Mirror messages to Snowflake / BigQuery
- Real-time dashboard updates on session summaries

**Workflow automation (Zapier-style)**
- Auto-create Zendesk tickets from support intents
- Calendar booking on detected scheduling intent

**Compliance & audit**
- Immutable audit log to S3 with HMAC verification
- PII detection alerting

**AI orchestration**
- Trigger downstream agents on session summary creation
- Multi-agent handoff on fact thresholds

**Customer-facing features**
- In-app "your AI remembers you said X" notifications
- Achievement / milestone triggers

The pattern they all share: *"foreman, when X happens in my workspace, tell my system Y so I can do Z."* Foreman doesn't care what Z is — its job ends at the signed POST.

---

## 7. The memory hooks — sentences to remember

1. **Lakebase is the ledger; everything else is a view of it.** Messages land in Lakebase first, always.
2. **Two VS indexes, two write disciplines.** `messages_idx` is hand-fed by the app for freshness; `documents_idx` is Databricks-managed from a Delta source because batch is fine.
3. **Delta is where Spark jobs put derived knowledge.** It then flows two ways: forward to `documents_idx` (for app reads via VS), and reverse via synced tables back to Lakebase (for transactional access without JDBC-to-Delta from the app).
4. **`documents_delta` is foreman's long-term memory store** — perspective-tagged facts, content-addressed IDs, MERGE for free idempotency.
5. **The webhooks job is the only thing in foreman that talks to the outside world after the fact.**

The asymmetry is the whole design: **synchronous freshness path through Direct Access VS, asynchronous derivation path through Delta + Delta Sync VS.**
