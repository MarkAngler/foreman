"""FastAPI entrypoint for foreman."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi_pagination import add_pagination

from foreman.app import webhook_reconciler
from foreman.app.routers import all_routers
from foreman.app.routers.chat import router as chat_router
from foreman.lib.lakebase import dispose_engine, init_engine
from foreman.lib.tracing import init_tracing


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_tracing()
    await init_engine()
    reconciler_task: asyncio.Task | None = None
    try:
        reconciler_task = webhook_reconciler.start(app)
        yield
    finally:
        if reconciler_task is not None:
            reconciler_task.cancel()
            try:
                await reconciler_task
            except (asyncio.CancelledError, Exception):
                pass
        await dispose_engine()


app = FastAPI(title="foreman", version="0.1.0", lifespan=lifespan)

for router in all_routers():
    app.include_router(router)
app.include_router(chat_router)
add_pagination(app)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
async def root() -> dict[str, str]:
    return {"name": "foreman", "version": "0.1.0"}
