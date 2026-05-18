"""JDBC read helpers for Spark jobs.

Spark connects to Lakebase via the Postgres JDBC driver, with the OAuth token
acquired via the Databricks SDK passed as the password. Read-only — writes go
to Delta and reverse-ETL syncs them back to Lakebase.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from databricks.sdk import WorkspaceClient

from foreman.lib.config import settings

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def jdbc_url() -> str:
    s = settings()
    instance = WorkspaceClient().database.get_database_instance(name=s.lakebase_instance)
    return f"jdbc:postgresql://{instance.read_write_dns}:5432/{s.lakebase_database}?sslmode=require"


def jdbc_options() -> dict[str, str]:
    s = settings()
    w = WorkspaceClient()
    user = s.lakebase_username or w.current_user.me().user_name
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[s.lakebase_instance],
    )
    return {
        "url": jdbc_url(),
        "driver": "org.postgresql.Driver",
        "user": user,
        "password": cred.token,
    }


def read_table(spark: "SparkSession", table: str) -> "DataFrame":
    opts = jdbc_options()
    return spark.read.format("jdbc").options(**opts, dbtable=table).load()


def read_query(spark: "SparkSession", query: str) -> "DataFrame":
    opts = jdbc_options()
    return spark.read.format("jdbc").options(**opts, query=query).load()
