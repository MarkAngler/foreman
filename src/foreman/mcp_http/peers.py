"""Idempotent peer upsert for the OBO Genie surface.

Auto-creates the peer in the configured workspace on first contact. Uses the
same INSERT ... ON CONFLICT shape the peers router uses, but does not depend
on or import any router code (the spine stays untouched).
"""

from __future__ import annotations

from sqlalchemy import text

from foreman.lib.lakebase import session


async def ensure_peer(workspace_name: str, peer_name: str) -> None:
    async with session() as s:
        await s.execute(
            text(
                "INSERT INTO peers (name, workspace_name) "
                "VALUES (:n, :w) "
                "ON CONFLICT (name, workspace_name) DO NOTHING"
            ),
            {"n": peer_name, "w": workspace_name},
        )
        await s.commit()
