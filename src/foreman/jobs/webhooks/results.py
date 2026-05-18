"""Delta table for webhook delivery results.

Per the spine rule, Spark jobs never write to Lakebase via JDBC. Webhook delivery
outcomes are appended to a Delta table here; a Phase-3 reconciler is responsible
for translating those rows into queue_items.processed updates in Lakebase
(documented in worktree_notes.md).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from foreman.lib.config import settings

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def webhook_results_table() -> str:
    s = settings()
    return f"{s.catalog}.{s.schema_}.webhook_results_delta"


def ensure_webhook_results_delta(spark: "SparkSession") -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {webhook_results_table()} (
            queue_item_id   BIGINT,
            endpoint_id     STRING,
            event_type      STRING,
            workspace_name  STRING,
            status_code     INT,
            success         BOOLEAN,
            retryable       BOOLEAN,
            error           STRING,
            attempt_count   INT,
            attempted_at    TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """
    )


def append_results(spark: "SparkSession", df: "DataFrame") -> None:
    df.write.format("delta").mode("append").saveAsTable(webhook_results_table())
