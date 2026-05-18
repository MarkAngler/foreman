"""Foreman REST routers.

Phase-3 wires these into the FastAPI app:

    from foreman.app.routers import all_routers
    for r in all_routers():
        app.include_router(r)

The dialectic chat router (foreman.app.routers.chat) is owned by Agent B and
mounted by Phase 3 separately — it intentionally is NOT returned by all_routers().
"""

from __future__ import annotations

from fastapi import APIRouter

from foreman.app.routers import (
    conclusions,
    keys,
    messages,
    peers,
    sessions,
    webhooks,
    workspaces,
)


def all_routers() -> list[APIRouter]:
    """Return every router in mount order (longest path-prefix last is fine
    here — FastAPI does longest-match, but ordering keeps the OpenAPI grouping
    predictable: top-level resources first, then nested)."""
    return [
        workspaces.router,
        peers.router,
        sessions.router,
        messages.router,
        conclusions.router,
        webhooks.router,
        keys.router,
    ]


__all__ = ["all_routers"]
