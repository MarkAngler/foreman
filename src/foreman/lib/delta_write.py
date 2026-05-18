"""Delta upsert helpers for jobs that write derived facts.

Convention: jobs write to Delta tables in UC; reverse-ETL syncs them back to
Lakebase for app-side OLTP reads. Never write to Lakebase via JDBC from Spark.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from foreman.lib.config import settings

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def ensure_documents_delta(spark: SparkSession) -> None:
    """Create the documents_delta table if it doesn't exist. Schema mirrors Lakebase."""
    s = settings()
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {s.documents_delta} (
            id              STRING,
            workspace_name  STRING,
            observer        STRING,
            observed        STRING,
            level           STRING,
            content         STRING,
            source_ids      ARRAY<STRING>,
            times_derived   INT,
            metadata        STRING,
            created_at      TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.feature.allowColumnDefaults' = 'supported'
        )
        """
    )


def ensure_session_summaries_delta(spark: SparkSession) -> None:
    s = settings()
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {s.session_summaries_delta} (
            id              STRING,
            session_name    STRING,
            workspace_name  STRING,
            content         STRING,
            messages_covered INT,
            created_at      TIMESTAMP
        ) USING DELTA
        """
    )


def upsert_documents(spark: SparkSession, df: DataFrame) -> None:
    """MERGE-by-id into documents_delta. Caller supplies a DF with the Delta schema."""
    s = settings()
    df.createOrReplaceTempView("_foreman_doc_upsert")
    spark.sql(
        f"""
        MERGE INTO {s.documents_delta} t
        USING _foreman_doc_upsert u
        ON t.id = u.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def append_summaries(spark: SparkSession, df: DataFrame) -> None:
    s = settings()
    df.write.format("delta").mode("append").saveAsTable(s.session_summaries_delta)


def all_delta_tables() -> Sequence[str]:
    s = settings()
    return [s.documents_delta, s.session_summaries_delta]
