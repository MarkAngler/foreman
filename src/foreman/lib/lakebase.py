"""Lakebase async connection pool with rolling 50-min OAuth token refresh.

Pattern lifted from databricks-app-python skill (5-lakebase.md). Tokens expire
after 1h; we refresh every 50m to leave a 10m safety margin.

Usage from FastAPI:

    from contextlib import asynccontextmanager
    from foreman.lib.lakebase import init_engine, dispose_engine, session

    @asynccontextmanager
    async def lifespan(app):
        await init_engine()
        try:
            yield
        finally:
            await dispose_engine()

    async def handler():
        async with session() as s:
            await s.execute(text("SELECT 1"))
"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator

from databricks.sdk import WorkspaceClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from foreman.lib.config import settings

TOKEN_REFRESH_INTERVAL_SECONDS = 50 * 60

_engine: AsyncEngine | None = None
_session_factory: sessionmaker | None = None
_current_token: str | None = None
_refresh_task: asyncio.Task | None = None


def _generate_token(instance_name: str) -> str:
    w = WorkspaceClient()
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name],
    )
    return cred.token


async def _refresh_loop(instance_name: str) -> None:
    global _current_token
    while True:
        await asyncio.sleep(TOKEN_REFRESH_INTERVAL_SECONDS)
        try:
            _current_token = await asyncio.to_thread(_generate_token, instance_name)
        except Exception as exc:  # noqa: BLE001 — log but keep loop alive
            # If refresh fails, the next connection attempt will fail loudly;
            # keep trying so transient SDK issues recover.
            print(f"[lakebase] token refresh failed: {exc!r}", flush=True)


async def init_engine() -> AsyncEngine:
    """Initialize the global async engine. Call once at app startup."""
    global _engine, _session_factory, _current_token, _refresh_task

    if _engine is not None:
        return _engine

    s = settings()
    w = WorkspaceClient()
    instance = w.database.get_database_instance(name=s.lakebase_instance)
    user = s.lakebase_username or w.current_user.me().user_name

    _current_token = await asyncio.to_thread(_generate_token, s.lakebase_instance)

    url = (
        f"postgresql+psycopg://{user}@{instance.read_write_dns}:5432/{s.lakebase_database}"
    )
    engine = create_async_engine(
        url,
        pool_size=5,
        max_overflow=10,
        pool_recycle=3600,
        pool_pre_ping=True,
        connect_args={"sslmode": "require"},
    )

    @event.listens_for(engine.sync_engine, "do_connect")
    def _provide_token(dialect, conn_rec, cargs, cparams):  # noqa: ANN001
        cparams["password"] = _current_token

    _engine = engine
    _session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    _refresh_task = asyncio.create_task(_refresh_loop(s.lakebase_instance))
    return engine


async def dispose_engine() -> None:
    """Tear down the engine. Call once at app shutdown."""
    global _engine, _session_factory, _refresh_task

    if _refresh_task is not None:
        _refresh_task.cancel()
        try:
            await _refresh_task
        except (asyncio.CancelledError, Exception):
            pass
        _refresh_task = None

    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None


def engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("lakebase engine not initialized — call init_engine() first")
    return _engine


@asynccontextmanager
async def session() -> AsyncIterator[AsyncSession]:
    if _session_factory is None:
        raise RuntimeError("lakebase engine not initialized — call init_engine() first")
    async with _session_factory() as s:
        yield s
