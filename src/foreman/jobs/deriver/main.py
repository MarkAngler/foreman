"""Deriver job: extract explicit facts from new messages.

Cadence: scheduled (`resources/jobs.yml` cron). Each invocation:
  1. Reads the watermark from `{catalog}.{schema}.deriver_state` (a tiny Delta KV).
  2. Pulls the next batch of messages with `id > watermark` from Lakebase via JDBC.
  3. Loads `session_peers` for each (workspace, session) touched, to derive
     observers per honcho's rules.
  4. Groups messages by (workspace, session, observed) and calls the deriver
     LLM once per group.
  5. Parses the JSON response into explicit facts, fans them out across all
     observer collections, and MERGEs them into `documents_delta`.
  6. Triggers a Vector Search sync on the documents index.
  7. Advances the watermark.

Idempotency: document IDs are deterministic
(`sha256(workspace|observer|observed|content)[:24]`) so re-running the same
range produces a no-op MERGE.

Reads Lakebase directly via JDBC (no Delta intermediary) for deterministic
freshness — Lakebase is the system of record for `messages`. See
`sorted-weaving-yao.md` "Risks / open verification items".
"""

from __future__ import annotations

import logging
import sys
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

from foreman.jobs._bootstrap import apply_env_overrides

apply_env_overrides()

from foreman.jobs.deriver.observer_observed import (  # noqa: E402
    DeriverBatch,
    MessageRow,
    build_batches,
)
from foreman.jobs.deriver.prompts import format_message_turn, minimal_deriver_prompt  # noqa: E402
from foreman.jobs.deriver.structured_output import (  # noqa: E402
    build_document_drafts,
    parse_response,
)
from foreman.lib import delta_write, lakebase_read, llm_dispatch, vector_search  # noqa: E402
from foreman.lib.config import settings  # noqa: E402
from foreman.lib.tracing import init_tracing, trace  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


logger = logging.getLogger(__name__)

WATERMARK_KEY = "last_processed_message_id"
DEFAULT_BATCH_LIMIT = 5000


# ---- Watermark state ---------------------------------------------------------


def state_table() -> str:
    s = settings()
    return f"{s.catalog}.{s.schema_}.deriver_state"


def ensure_state_table(spark: SparkSession) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {state_table()} (
            key   STRING,
            value STRING
        ) USING DELTA
        """
    )


def read_watermark(spark: SparkSession) -> int:
    rows = (
        spark.sql(f"SELECT value FROM {state_table()} WHERE key = '{WATERMARK_KEY}'")
        .limit(1)
        .collect()
    )
    if not rows:
        return 0
    raw = rows[0]["value"]
    return int(raw) if raw is not None else 0


def write_watermark(spark: SparkSession, new_value: int) -> None:
    from pyspark.sql import Row

    df = spark.createDataFrame([Row(key=WATERMARK_KEY, value=str(new_value))])
    df.createOrReplaceTempView("_foreman_deriver_state_upsert")
    spark.sql(
        f"""
        MERGE INTO {state_table()} t
        USING _foreman_deriver_state_upsert u
        ON t.key = u.key
        WHEN MATCHED THEN UPDATE SET t.value = u.value
        WHEN NOT MATCHED THEN INSERT *
        """
    )


# ---- Message fetch -----------------------------------------------------------


def fetch_new_messages(spark: SparkSession, *, after_id: int, limit: int) -> list[MessageRow]:
    query = (
        "SELECT id, public_id, session_name, peer_name, workspace_name, "
        "content, created_at "
        f"FROM messages WHERE id > {int(after_id)} "
        f"ORDER BY id LIMIT {int(limit)}"
    )
    df = lakebase_read.read_query(spark, query)
    rows = df.collect()
    return [MessageRow.from_row(r.asDict()) for r in rows]


def fetch_session_peers(
    spark: SparkSession, *, sessions: Iterable[tuple[str, str]]
) -> dict[tuple[str, str], list[str]]:
    pairs = list(sessions)
    if not pairs:
        return {}
    in_clause = ",".join(f"('{ws}','{sn}')" for ws, sn in pairs)
    query = (
        "SELECT workspace_name, session_name, peer_name FROM session_peers "
        f"WHERE (workspace_name, session_name) IN ({in_clause}) "
        "AND left_at IS NULL"
    )
    df = lakebase_read.read_query(spark, query)
    out: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in df.collect():
        out[(row["workspace_name"], row["session_name"])].append(row["peer_name"])
    return dict(out)


# ---- LLM invocation ----------------------------------------------------------


@trace(name="deriver_extract_batch")
def extract_facts(batch: DeriverBatch) -> str:
    formatted = "\n".join(
        format_message_turn(m.content, _as_datetime(m.created_at), m.peer_name)
        for m in batch.messages
    )
    prompt = minimal_deriver_prompt(peer_id=batch.observed, messages=formatted)
    response = llm_dispatch.chat_sync(
        workspace_name=batch.workspace_name,
        role="deriver",
        messages=[{"role": "user", "content": prompt}],
    )
    return _extract_text(response)


def _extract_text(response: Any) -> str:
    """Pull the assistant message text out of whatever the dispatcher returned."""
    if response is None:
        raise ValueError("deriver LLM returned no response")
    if isinstance(response, str):
        return response

    payload = response.as_dict() if hasattr(response, "as_dict") else response
    if isinstance(payload, dict):
        choices = payload.get("choices") or []
        if choices:
            msg = choices[0].get("message") or {}
            content = msg.get("content")
            if isinstance(content, str) and content:
                return content
        # OpenAI-style streaming-aggregate or alternate shapes.
        if isinstance(payload.get("content"), str):
            return payload["content"]
    raise ValueError(f"deriver: unrecognised LLM response shape: {type(response).__name__}")


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError(f"unsupported created_at type: {type(value).__name__}")


# ---- Delta write -------------------------------------------------------------


def build_doc_dataframe(spark: SparkSession, drafts: list[Any]) -> DataFrame:
    from pyspark.sql import Row
    from pyspark.sql.types import (
        ArrayType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("workspace_name", StringType(), False),
            StructField("observer", StringType(), False),
            StructField("observed", StringType(), False),
            StructField("level", StringType(), False),
            StructField("content", StringType(), False),
            StructField("source_ids", ArrayType(StringType()), False),
            StructField("times_derived", IntegerType(), False),
            StructField("metadata", StringType(), False),
            StructField("created_at", TimestampType(), False),
        ]
    )
    rows = [
        Row(
            id=d.id,
            workspace_name=d.workspace_name,
            observer=d.observer,
            observed=d.observed,
            level=d.level,
            content=d.content,
            source_ids=d.source_ids,
            times_derived=d.times_derived,
            metadata=d.metadata,
            created_at=d.created_at,
        )
        for d in drafts
    ]
    return spark.createDataFrame(rows, schema=schema)


# ---- Batch orchestration -----------------------------------------------------


def assemble_drafts(
    *,
    messages: list[MessageRow],
    session_peers: Mapping[tuple[str, str], list[str]],
) -> list[Any]:
    """Pure-Python batch orchestration: messages -> document drafts.

    Spark is not touched here — the only side effect is the LLM dispatch.
    Per-batch failures are logged and skipped so a single bad LLM response
    doesn't stall the whole job.
    """
    batches = build_batches(messages, session_peers)
    drafts: list[Any] = []
    for batch in batches:
        try:
            raw = extract_facts(batch)
            representation = parse_response(raw)
        except Exception:
            logger.exception(
                "deriver: extraction failed for workspace=%s session=%s observed=%s",
                batch.workspace_name,
                batch.session_name,
                batch.observed,
            )
            continue

        latest_created_at = _as_datetime(batch.messages[-1].created_at)
        drafts.extend(
            build_document_drafts(
                representation=representation,
                workspace_name=batch.workspace_name,
                observed=batch.observed,
                observers=batch.observers,
                source_message_public_ids=[m.public_id for m in batch.messages],
                session_name=batch.session_name,
                created_at=latest_created_at,
            )
        )
    return drafts


def process_batch(
    spark: SparkSession,
    *,
    messages: list[MessageRow],
    session_peers: Mapping[tuple[str, str], list[str]],
) -> int:
    """Run one batch end-to-end. Returns the number of documents written."""
    drafts = assemble_drafts(messages=messages, session_peers=session_peers)
    if not drafts:
        return 0

    df = build_doc_dataframe(spark, drafts)
    delta_write.upsert_documents(spark, df)
    return len(drafts)


# ---- Entry point -------------------------------------------------------------


@trace(name="deriver_main")
def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_tracing()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    delta_write.ensure_documents_delta(spark)
    ensure_state_table(spark)

    watermark = read_watermark(spark)
    logger.info("deriver: starting from watermark id=%s", watermark)

    messages = fetch_new_messages(spark, after_id=watermark, limit=DEFAULT_BATCH_LIMIT)
    if not messages:
        logger.info("deriver: no new messages, exiting")
        return 0

    session_keys = {(m.workspace_name, m.session_name) for m in messages}
    session_peers = fetch_session_peers(spark, sessions=session_keys)

    written = process_batch(spark, messages=messages, session_peers=session_peers)
    new_watermark = max(m.id for m in messages)

    if written:
        try:
            vector_search.trigger_documents_sync()
        except Exception:
            logger.exception("deriver: trigger_documents_sync failed (continuing)")

    write_watermark(spark, new_watermark)
    logger.info(
        "deriver: processed %d messages, wrote %d documents, new watermark=%s",
        len(messages),
        written,
        new_watermark,
    )
    return 0


if __name__ == "__main__":
    # Databricks spark_python_task exec wrapper treats SystemExit (even with
    # code 0) as task failure. Only raise on non-zero.
    _rc = main()
    if _rc != 0:
        sys.exit(_rc)
