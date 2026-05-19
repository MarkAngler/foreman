"""FastAPI host for the OBO MCP. Mounts FastMCP's Streamable HTTP transport
under `/` and reuses the lakebase engine lifespan from foreman.lib.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from foreman.lib.lakebase import dispose_engine, init_engine
from foreman.lib.tracing import init_tracing
from foreman.mcp_http.server import _workspace, mcp


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_tracing()
    _workspace()  # fail fast if FOREMAN_MCP_WORKSPACE is unset
    await init_engine()
    try:
        yield
    finally:
        await dispose_engine()


app = FastAPI(title="foreman-mcp-http", version="0.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


app.mount("/", mcp.streamable_http_app())
