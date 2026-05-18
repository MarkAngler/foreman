"""schema_init: one-shot job that completes spine setup after `bundle deploy`.

Order of operations (each step idempotent):
  1. Run Alembic migrations against Lakebase  -> creates the 11 OLTP tables.
  2. Create empty Delta tables in UC          -> documents_delta,
                                                  session_summaries_delta,
                                                  webhook_results_delta.
  3. Create Delta -> Lakebase synced tables   -> documents_delta -> documents_mirror,
                                                  webhook_results_delta -> webhook_results_mirror.
  4. Create Vector Search indexes             -> messages_idx (Direct Access),
                                                  documents_idx (Delta Sync).

Run via:  databricks bundle run schema_init
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

from foreman.jobs._bootstrap import apply_env_overrides

apply_env_overrides()

import psycopg  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service.database import (  # noqa: E402
    NewPipelineSpec,
    SyncedDatabaseTable,
    SyncedTableSchedulingPolicy,
    SyncedTableSpec,
)

from foreman.jobs.webhooks.results import ensure_webhook_results_delta  # noqa: E402
from foreman.lib.config import settings  # noqa: E402
from foreman.lib.delta_write import (  # noqa: E402
    ensure_documents_delta,
    ensure_session_summaries_delta,
)
from foreman.lib.vector_search import ensure_documents_index, ensure_messages_index  # noqa: E402


def info(msg: str) -> None:
    print(f"[schema_init] {msg}", flush=True)


# ---- Step 1 -----------------------------------------------------------------


def _resolve_app_sp(app_name: str) -> str | None:
    w = WorkspaceClient()
    try:
        app = w.apps.get(name=app_name)
    except Exception as e:
        info(f"  WARNING: could not look up app {app_name!r} ({type(e).__name__}: {e}); skipping")
        return None
    sp = getattr(app, "service_principal_client_id", None)
    if not sp:
        info(f"  WARNING: app {app_name!r} has no service_principal_client_id yet; skipping")
    return sp


def grant_app_lakebase_privileges(app_name: str) -> None:
    """GRANT table/sequence privileges to the foreman App's Postgres SP role.

    install/grant_race.py creates the Postgres role matching the app's
    service_principal_client_id when the app comes up, but never grants
    table-level privileges. Alembic creates the OLTP tables owned by the
    developer/job-runner identity, so without these grants the app gets
    `permission denied for table workspaces` on every DB-touching request.
    Idempotent — GRANT and ALTER DEFAULT PRIVILEGES are no-ops on repeat.
    """
    info(f"step 2/6: granting Lakebase privileges to app '{app_name}'")
    sp = _resolve_app_sp(app_name)
    if not sp:
        return

    s = settings()
    w = WorkspaceClient()
    inst = w.database.get_database_instance(name=s.lakebase_instance)
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[s.lakebase_instance],
    )
    user = w.current_user.me().user_name
    conn = psycopg.connect(
        host=inst.read_write_dns,
        port=5432,
        dbname=s.lakebase_database,
        user=user,
        password=cred.token,
        sslmode="require",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'GRANT USAGE ON SCHEMA public TO "{sp}";')
            cur.execute(
                f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{sp}";'
            )
            cur.execute(f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{sp}";')
            cur.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                f'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{sp}";'
            )
            cur.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                f'GRANT USAGE, SELECT ON SEQUENCES TO "{sp}";'
            )
    finally:
        conn.close()
    info(f"  Lakebase privileges granted to role {sp!r}")


def grant_app_uc_privileges(app_name: str) -> None:
    """GRANT Unity Catalog privileges to the foreman App's service principal.

    The inline vector-search upsert on message-create and the workspace search
    endpoint both hit the UC-managed VS indexes (messages_idx, documents_idx).
    Without explicit UC grants the app SP gets `PermissionDenied: Insufficient
    permissions for UC entity foreman.memory.messages_idx` and the request
    surfaces as a 500. Runs LAST in schema_init so the indexes exist before we
    grant on them. Uses spark.sql GRANT — the SDK's grants.update mis-
    serializes the SecurableType enum on the deployed runtime, but spark.sql
    talks directly to the Unity Catalog command path. Idempotent — repeated
    grants are no-ops.
    """
    info(f"step 6/6: granting Unity Catalog privileges to app '{app_name}'")
    sp = _resolve_app_sp(app_name)
    if not sp:
        return

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    s = settings()

    statements = [
        f"GRANT USE CATALOG ON CATALOG `{s.catalog}` TO `{sp}`",
        f"GRANT USE SCHEMA ON SCHEMA `{s.catalog}`.`{s.schema_}` TO `{sp}`",
        # messages_idx is Direct Access — the app reads AND writes inline.
        f"GRANT SELECT, MODIFY ON TABLE `{s.catalog}`.`{s.schema_}`.`messages_idx` TO `{sp}`",
        # documents_idx is Delta Sync — the app only reads it.
        f"GRANT SELECT ON TABLE `{s.catalog}`.`{s.schema_}`.`documents_idx` TO `{sp}`",
        # documents_delta is the source for documents_idx — SELECT covers any
        # fallback reads from the source table.
        f"GRANT SELECT ON TABLE `{s.catalog}`.`{s.schema_}`.`documents_delta` TO `{sp}`",
    ]
    for stmt in statements:
        try:
            spark.sql(stmt)
            info(f"  {stmt}")
        except Exception as e:
            info(f"  WARNING: '{stmt}' failed ({type(e).__name__}: {e})")


def run_migrations() -> None:
    info("step 1/5: running Alembic migrations")
    # When installed via wheel, the schema/ directory is no longer a sibling of
    # this module. Walk up from CWD looking for alembic.ini — the bundle deploy
    # uploads the repo root as the Spark task's working directory.
    schema_dir = None
    for parent in [Path.cwd(), *Path.cwd().parents]:
        candidate = parent / "schema" / "alembic.ini"
        if candidate.exists():
            schema_dir = candidate.parent
            break
    if schema_dir is None:
        raise RuntimeError("could not find schema/alembic.ini under CWD or ancestors")

    os.environ["FOREMAN_LAKEBASE_INSTANCE"] = settings().lakebase_instance
    os.environ["FOREMAN_LAKEBASE_DATABASE"] = settings().lakebase_database

    # Run alembic in-process. Spawning a subprocess loses the Databricks runtime
    # auth context (IPython) that WorkspaceClient() in schema/env.py needs to
    # construct the Lakebase Postgres URL.
    cwd_before = os.getcwd()
    os.chdir(str(schema_dir))
    try:
        from alembic import command as alembic_command
        from alembic.config import Config as AlembicConfig

        cfg = AlembicConfig(str(schema_dir / "alembic.ini"))
        alembic_command.upgrade(cfg, "head")
    finally:
        os.chdir(cwd_before)


# ---- Step 2 -----------------------------------------------------------------


def create_delta_tables() -> None:
    info("step 3/6: creating Delta tables in UC")
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    ensure_documents_delta(spark)
    ensure_session_summaries_delta(spark)
    ensure_webhook_results_delta(spark)


# ---- Step 3 -----------------------------------------------------------------


def create_delta_to_lakebase_sync() -> None:
    """documents_delta (UC) -> documents_mirror (Lakebase). For app-side reads."""
    info("step 4a/6: creating Delta -> Lakebase synced table for documents")
    s = settings()
    w = WorkspaceClient()

    sync_name = f"{s.catalog}.{s.schema_}.documents_mirror_sync"
    try:
        existing = w.database.get_synced_database_table(name=sync_name)
        info(
            f"  documents_mirror sync already exists (state={existing.data_synchronization_status})"
        )
        return
    except Exception:
        pass

    w.database.create_synced_database_table(
        synced_table=SyncedDatabaseTable(
            name=sync_name,
            spec=SyncedTableSpec(
                source_table_full_name=s.documents_delta,
                primary_key_columns=["id"],
                scheduling_policy=SyncedTableSchedulingPolicy.TRIGGERED,
                new_pipeline_spec=NewPipelineSpec(
                    storage_catalog=s.catalog,
                    storage_schema=s.schema_,
                ),
            ),
            database_instance_name=s.lakebase_instance,
            logical_database_name=s.lakebase_database,
        )
    )


def create_webhook_results_sync() -> None:
    """webhook_results_delta (UC) -> webhook_results_mirror (Lakebase).

    The webhook reconciler in the FastAPI app reads webhook_results_mirror to
    mark queue_items as processed (Spark cannot write back to Lakebase per the
    spine contract).
    """
    info("step 4b/6: creating Delta -> Lakebase synced table for webhook_results")
    s = settings()
    w = WorkspaceClient()

    sync_name = f"{s.catalog}.{s.schema_}.webhook_results_mirror_sync"
    try:
        existing = w.database.get_synced_database_table(name=sync_name)
        info(
            f"  webhook_results_mirror sync already exists (state={existing.data_synchronization_status})"
        )
        return
    except Exception:
        pass

    source = f"{s.catalog}.{s.schema_}.webhook_results_delta"
    w.database.create_synced_database_table(
        synced_table=SyncedDatabaseTable(
            name=sync_name,
            spec=SyncedTableSpec(
                source_table_full_name=source,
                primary_key_columns=["queue_item_id", "endpoint_id", "attempt_count"],
                scheduling_policy=SyncedTableSchedulingPolicy.TRIGGERED,
                new_pipeline_spec=NewPipelineSpec(
                    storage_catalog=s.catalog,
                    storage_schema=s.schema_,
                ),
            ),
            database_instance_name=s.lakebase_instance,
            logical_database_name=s.lakebase_database,
        )
    )


# ---- Step 4 -----------------------------------------------------------------


def create_vector_indexes() -> None:
    info("step 5/6: creating Vector Search indexes")
    ensure_messages_index()
    ensure_documents_index()


def main() -> int:
    app_name = os.environ.get("FOREMAN_APP_NAME", "foreman-dev")
    run_migrations()
    grant_app_lakebase_privileges(app_name)
    create_delta_tables()
    # Delta->Lakebase synced tables depend on the source Delta tables being
    # visible in UC, which is async (minute-scale). Best-effort here so the
    # rest of schema_init proceeds; re-running schema_init picks them up.
    try:
        create_delta_to_lakebase_sync()
    except Exception as e:
        info(f"WARNING: skipping documents_mirror sync ({type(e).__name__}: {e})")
    try:
        create_webhook_results_sync()
    except Exception as e:
        info(f"WARNING: skipping webhook_results_mirror sync ({type(e).__name__}: {e})")
    create_vector_indexes()
    # UC grants must run AFTER index creation — the VS indexes must exist as
    # UC securables before we can grant on them.
    grant_app_uc_privileges(app_name)
    info("schema_init complete")
    return 0


if __name__ == "__main__":
    # Databricks spark_python_task exec wrapper treats SystemExit (even with
    # code 0) as task failure. Only raise on non-zero.
    _rc = main()
    if _rc != 0:
        sys.exit(_rc)
