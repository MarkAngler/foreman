"""Alembic environment for foreman.

Resolves the Lakebase connection at runtime via the Databricks SDK so the same
migration code works locally (with `DATABRICKS_CONFIG_PROFILE`) and inside the
schema_init job (with workspace-injected credentials).
"""

from __future__ import annotations

import os
import uuid
from logging.config import fileConfig

from alembic import context
from databricks.sdk import WorkspaceClient
from sqlalchemy import engine_from_config, pool

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def _build_url() -> str:
    instance_name = os.environ["FOREMAN_LAKEBASE_INSTANCE"]
    db_name = os.environ.get("FOREMAN_LAKEBASE_DATABASE", "memory")

    w = WorkspaceClient()
    instance = w.database.get_database_instance(name=instance_name)
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name],
    )
    user = w.current_user.me().user_name
    return (
        f"postgresql+psycopg://{user}:{cred.token}"
        f"@{instance.read_write_dns}:5432/{db_name}?sslmode=require"
    )


def run_migrations_offline() -> None:
    context.configure(url=_build_url(), literal_binds=True, dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    config.set_main_option("sqlalchemy.url", _build_url())
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
