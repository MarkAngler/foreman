"""Summarizer job: rolling per-session summaries.

Cadence: scheduled (`resources/jobs.yml` cron, default every 5 minutes).

Per invocation:
  1. Reads `summarizer_state_delta` — last summarised `messages.id` per
     (workspace, session).
  2. Joins against Lakebase `messages` via JDBC to find sessions whose
     unsummarised tail has reached the short-summary threshold.
  3. For each such session, pulls the recent window (`MESSAGES_PER_SHORT_SUMMARY`
     by default), calls `chat_sync(role='summarizer', ...)`, appends a row to
     `session_summaries_delta`, and advances the watermark.

Idempotency: the watermark advances only after a successful append, and each
summary row carries its own UUID, so a re-run on the same window simply
produces a duplicate-free no-op (no MERGE needed because we append-only and
filter sessions by watermark).

Reads Lakebase directly via JDBC; Lakebase is the system of record for
`messages` (no Delta intermediary).
"""

from __future__ import annotations

import logging
import sys
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from foreman.jobs._bootstrap import apply_env_overrides

apply_env_overrides()

from foreman.jobs.summarizer.prompts import (  # noqa: E402
    MAX_TOKENS_SHORT,
    MESSAGES_PER_SHORT_SUMMARY,
    NO_PREVIOUS_SUMMARY,
    format_message_turn,
    short_summary_prompt,
)
from foreman.lib import delta_write, lakebase_read, llm_dispatch  # noqa: E402
from foreman.lib.config import settings  # noqa: E402
from foreman.lib.tracing import init_tracing, trace  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


logger = logging.getLogger(__name__)


# ---- Schema-bound types ------------------------------------------------------


@dataclass(frozen=True)
class SessionWindow:
    """A session that needs a new summary, with its message window resolved."""

    workspace_name: str
    session_name: str
    last_summarised_id: int  # exclusive lower bound; 0 if never summarised
    new_high_water_id: int   # the id we will advance the state watermark to
    messages_in_window: int


@dataclass(frozen=True)
class MessageTurn:
    id: int
    peer_name: str
    content: str
    created_at: object  # datetime — Spark Row keeps this loose


@dataclass(frozen=True)
class SummaryDraft:
    id: str
    workspace_name: str
    session_name: str
    content: str
    messages_covered: int
    created_at: datetime


# ---- State table (per-session watermark) -------------------------------------


def state_table() -> str:
    s = settings()
    return f"{s.catalog}.{s.schema_}.summarizer_state"


def ensure_state_table(spark: SparkSession) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {state_table()} (
            workspace_name      STRING,
            session_name        STRING,
            last_message_id     BIGINT,
            updated_at          TIMESTAMP
        ) USING DELTA
        """
    )


def read_state(spark: SparkSession) -> dict[tuple[str, str], int]:
    rows = spark.sql(
        f"SELECT workspace_name, session_name, last_message_id FROM {state_table()}"
    ).collect()
    return {
        (row["workspace_name"], row["session_name"]): int(row["last_message_id"] or 0)
        for row in rows
    }


def write_state(
    spark: SparkSession, *, advanced: Iterable[tuple[str, str, int]]
) -> None:
    """MERGE-upsert one row per (workspace, session) with the new watermark."""
    advanced = list(advanced)
    if not advanced:
        return

    from pyspark.sql import Row
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("workspace_name", StringType(), False),
            StructField("session_name", StringType(), False),
            StructField("last_message_id", LongType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )
    now = datetime.now(UTC)
    df = spark.createDataFrame(
        [
            Row(
                workspace_name=ws,
                session_name=sn,
                last_message_id=int(mid),
                updated_at=now,
            )
            for ws, sn, mid in advanced
        ],
        schema=schema,
    )
    df.createOrReplaceTempView("_foreman_summariser_state_upsert")
    spark.sql(
        f"""
        MERGE INTO {state_table()} t
        USING _foreman_summariser_state_upsert u
        ON t.workspace_name = u.workspace_name AND t.session_name = u.session_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


# ---- Session selection -------------------------------------------------------


def find_eligible_sessions(
    spark: SparkSession,
    *,
    state: dict[tuple[str, str], int],
    threshold: int = MESSAGES_PER_SHORT_SUMMARY,
) -> list[SessionWindow]:
    """Return sessions whose unsummarised tail has reached the threshold."""
    query = (
        "SELECT workspace_name, session_name, MIN(id) AS min_id, MAX(id) AS max_id, "
        "COUNT(*) AS total FROM messages "
        "GROUP BY workspace_name, session_name "
        "HAVING COUNT(*) > 0"
    )
    df = lakebase_read.read_query(spark, query)
    return select_eligible(df.collect(), state=state, threshold=threshold)


def select_eligible(
    rows: list[Any],
    *,
    state: dict[tuple[str, str], int],
    threshold: int,
) -> list[SessionWindow]:
    """Pure helper: pick sessions where unsummarised tail >= threshold.

    Splitting this from the JDBC fetch keeps the threshold/watermark logic
    unit-testable without a real Spark / Lakebase round-trip.
    """
    out: list[SessionWindow] = []
    for row in rows:
        ws = row["workspace_name"]
        sn = row["session_name"]
        max_id = int(row["max_id"])
        watermark = state.get((ws, sn), 0)
        unsummarised = _unsummarised_count(row=row, watermark=watermark)
        if unsummarised < threshold:
            continue
        out.append(
            SessionWindow(
                workspace_name=ws,
                session_name=sn,
                last_summarised_id=watermark,
                new_high_water_id=max_id,
                messages_in_window=min(unsummarised, threshold * 3),
            )
        )
    return out


def _unsummarised_count(*, row: Any, watermark: int) -> int:
    """Count messages with id > watermark in this session.

    Uses the (min_id, max_id, total) summary returned by `find_eligible_sessions`.
    Cheap upper-bound: when watermark < min_id, every row is unsummarised; once
    watermark >= min_id we conservatively assume contiguous ids, which holds
    because `messages.id` is a BIGSERIAL sequence per honcho's schema.
    """
    min_id = int(row["min_id"])
    max_id = int(row["max_id"])
    total = int(row["total"])
    if watermark < min_id:
        return total
    if watermark >= max_id:
        return 0
    # Contiguous-id assumption: how many ids fall in (watermark, max_id].
    return max_id - watermark


# ---- Message fetch -----------------------------------------------------------


def fetch_window(
    spark: SparkSession, *, window: SessionWindow
) -> list[MessageTurn]:
    """Pull the message window for one session in id order."""
    # Identifiers come from validated honcho schema (CHECK regex on names);
    # still escape single quotes defensively.
    ws = _quote(window.workspace_name)
    sn = _quote(window.session_name)
    query = (
        "SELECT id, peer_name, content, created_at FROM messages "
        f"WHERE workspace_name = '{ws}' AND session_name = '{sn}' "
        f"AND id > {int(window.last_summarised_id)} "
        f"AND id <= {int(window.new_high_water_id)} "
        f"ORDER BY id LIMIT {int(window.messages_in_window)}"
    )
    df = lakebase_read.read_query(spark, query)
    return [
        MessageTurn(
            id=int(r["id"]),
            peer_name=str(r["peer_name"]),
            content=str(r["content"]),
            created_at=r["created_at"],
        )
        for r in df.collect()
    ]


def _quote(value: str) -> str:
    return value.replace("'", "''")


# ---- Previous-summary lookup -------------------------------------------------


def fetch_previous_summary(
    spark: SparkSession, *, workspace_name: str, session_name: str
) -> str | None:
    """Return the most recent prior summary for this session, or None."""
    s = settings()
    ws = _quote(workspace_name)
    sn = _quote(session_name)
    rows = (
        spark.sql(
            f"SELECT content FROM {s.session_summaries_delta} "
            f"WHERE workspace_name = '{ws}' AND session_name = '{sn}' "
            "ORDER BY created_at DESC LIMIT 1"
        )
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    return rows[0]["content"]


# ---- LLM invocation ----------------------------------------------------------


@trace(name="summarizer_summarise_session")
def summarise(
    *,
    workspace_name: str,
    session_name: str,
    messages: list[MessageTurn],
    previous_summary: str | None,
) -> str:
    _tag_span(workspace_name=workspace_name, session_name=session_name)
    formatted = "\n".join(
        format_message_turn(m.content, _as_datetime(m.created_at), m.peer_name)
        for m in messages
    )
    output_words = int(MAX_TOKENS_SHORT * 0.75)
    prompt = short_summary_prompt(
        formatted_messages=formatted,
        output_words=output_words,
        previous_summary_text=previous_summary or NO_PREVIOUS_SUMMARY,
    )
    response = llm_dispatch.chat_sync(
        workspace_name=workspace_name,
        role="summarizer",
        messages=[{"role": "user", "content": prompt}],
    )
    text = _extract_text(response)
    if not text.strip():
        raise ValueError("summarizer LLM returned empty content")
    return text


def _tag_span(*, workspace_name: str, session_name: str) -> None:
    """Tag the active mlflow span with workspace/session — best effort."""
    try:
        import mlflow

        span = mlflow.get_current_active_span()
        if span is not None:
            span.set_attribute("workspace_name", workspace_name)
            span.set_attribute("session_name", session_name)
    except Exception:
        pass


def _extract_text(response: Any) -> str:
    """Pull assistant message text from whatever the dispatcher returned.

    Mirrors the deriver's response unwrap; kept duplicated rather than promoted
    to a shared helper because the two callers want different error messages on
    failure (rule: only extract once a third caller appears).
    """
    if response is None:
        raise ValueError("summarizer LLM returned no response")
    if isinstance(response, str):
        return response
    payload = response.as_dict() if hasattr(response, "as_dict") else response
    if isinstance(payload, dict):
        choices = payload.get("choices") or []
        if choices:
            content = (choices[0].get("message") or {}).get("content")
            if isinstance(content, str):
                return content
        if isinstance(payload.get("content"), str):
            return payload["content"]
    raise ValueError(
        f"summarizer: unrecognised LLM response shape: {type(response).__name__}"
    )


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError(f"unsupported created_at type: {type(value).__name__}")


# ---- Delta write -------------------------------------------------------------


def build_summary_dataframe(
    spark: SparkSession, drafts: list[SummaryDraft]
) -> DataFrame:
    from pyspark.sql import Row
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("session_name", StringType(), False),
            StructField("workspace_name", StringType(), False),
            StructField("content", StringType(), False),
            StructField("messages_covered", IntegerType(), False),
            StructField("created_at", TimestampType(), False),
        ]
    )
    rows = [
        Row(
            id=d.id,
            session_name=d.session_name,
            workspace_name=d.workspace_name,
            content=d.content,
            messages_covered=d.messages_covered,
            created_at=d.created_at,
        )
        for d in drafts
    ]
    return spark.createDataFrame(rows, schema=schema)


# ---- Orchestration -----------------------------------------------------------


def process_session(
    spark: SparkSession, *, window: SessionWindow
) -> SummaryDraft | None:
    messages = fetch_window(spark, window=window)
    if not messages:
        logger.info(
            "summarizer: no messages in window for %s/%s (last_id=%s)",
            window.workspace_name,
            window.session_name,
            window.last_summarised_id,
        )
        return None
    previous = fetch_previous_summary(
        spark,
        workspace_name=window.workspace_name,
        session_name=window.session_name,
    )
    content = summarise(
        workspace_name=window.workspace_name,
        session_name=window.session_name,
        messages=messages,
        previous_summary=previous,
    )
    return SummaryDraft(
        id=str(uuid.uuid4()),
        workspace_name=window.workspace_name,
        session_name=window.session_name,
        content=content,
        messages_covered=len(messages),
        created_at=datetime.now(UTC),
    )


@trace(name="summarizer_main")
def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_tracing()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    delta_write.ensure_session_summaries_delta(spark)
    ensure_state_table(spark)

    state = read_state(spark)
    eligible = find_eligible_sessions(spark, state=state)
    logger.info("summarizer: %d session(s) eligible for summary", len(eligible))
    if not eligible:
        return 0

    drafts: list[SummaryDraft] = []
    advanced: list[tuple[str, str, int]] = []
    for window in eligible:
        try:
            draft = process_session(spark, window=window)
        except Exception:
            logger.exception(
                "summarizer: failed for %s/%s", window.workspace_name, window.session_name
            )
            continue
        if draft is None:
            continue
        drafts.append(draft)
        advanced.append(
            (window.workspace_name, window.session_name, window.new_high_water_id)
        )

    if drafts:
        df = build_summary_dataframe(spark, drafts)
        delta_write.append_summaries(spark, df)
        write_state(spark, advanced=advanced)

    logger.info(
        "summarizer: wrote %d summary row(s), advanced %d watermark(s)",
        len(drafts),
        len(advanced),
    )
    return 0


# Re-export json for tests that build fake response payloads without importing twice.
__all__ = [
    "MessageTurn",
    "SessionWindow",
    "SummaryDraft",
    "build_summary_dataframe",
    "ensure_state_table",
    "fetch_previous_summary",
    "fetch_window",
    "find_eligible_sessions",
    "main",
    "process_session",
    "read_state",
    "select_eligible",
    "summarise",
    "write_state",
]


if __name__ == "__main__":
    sys.exit(main())
