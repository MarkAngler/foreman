"""Peer CRUD + peer-scoped message search.

The dialectic chat endpoint lives in foreman.app.routers.chat (Agent B). This
router intentionally does not include it.
"""

from __future__ import annotations

import json
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path, Response, status
from fastapi_pagination import Page, Params, paginate
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from foreman.app import schemas
from foreman.app.routers._common import assert_path_name, not_found
from foreman.lib import auth, vector_search
from foreman.lib.lakebase import session
from foreman.lib.tracing import trace

router = APIRouter(prefix="/workspaces/{workspace_name}/peers", tags=["peers"])


@router.post("", response_model=schemas.Peer)
@trace
async def create_peer(
    response: Response,
    workspace_name: Annotated[str, Path()],
    body: Annotated[schemas.PeerCreate, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> schemas.Peer:
    """Get-or-create a peer in this workspace."""
    assert_path_name(workspace_name, kind="workspace")
    auth.require_peer(workspace_name, body.name, scope)
    await _ensure_workspace(workspace_name)

    async with session() as s:
        existing = (
            await s.execute(
                text(
                    "SELECT name, workspace_name, metadata, configuration, created_at "
                    "FROM peers WHERE name = :n AND workspace_name = :w"
                ),
                {"n": body.name, "w": workspace_name},
            )
        ).mappings().first()
        if existing is not None:
            response.status_code = status.HTTP_200_OK
            return schemas.Peer.model_validate(dict(existing))

        try:
            row = (
                await s.execute(
                    text(
                        "INSERT INTO peers (name, workspace_name, metadata, configuration) "
                        "VALUES (:n, :w, :m, :c) "
                        "RETURNING name, workspace_name, metadata, configuration, created_at"
                    ),
                    {
                        "n": body.name,
                        "w": workspace_name,
                        "m": json.dumps(body.metadata),
                        "c": json.dumps(body.configuration),
                    },
                )
            ).mappings().one()
            await s.commit()
        except IntegrityError:
            await s.rollback()
            row = (
                await s.execute(
                    text(
                        "SELECT name, workspace_name, metadata, configuration, created_at "
                        "FROM peers WHERE name = :n AND workspace_name = :w"
                    ),
                    {"n": body.name, "w": workspace_name},
                )
            ).mappings().one()
            response.status_code = status.HTTP_200_OK
            return schemas.Peer.model_validate(dict(row))

    response.status_code = status.HTTP_201_CREATED
    return schemas.Peer.model_validate(dict(row))


@router.post("/list", response_model=Page[schemas.Peer])
@trace
async def list_peers(
    workspace_name: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
    params: Annotated[Params, Depends()],
    body: Annotated[schemas.PeerList | None, Body()] = None,
) -> Page[schemas.Peer]:
    assert_path_name(workspace_name, kind="workspace")
    auth.require_workspace(workspace_name, scope)

    where_sql, bind = _filter_clause(workspace_name, body.filters if body else None)
    async with session() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT name, workspace_name, metadata, configuration, created_at "
                    f"FROM peers {where_sql} "
                    "ORDER BY created_at DESC"
                ),
                bind,
            )
        ).mappings().all()

    items = [schemas.Peer.model_validate(dict(r)) for r in rows]
    return paginate(items, params)


@router.put("/{peer_name}", response_model=schemas.Peer)
@trace
async def update_peer(
    workspace_name: Annotated[str, Path()],
    peer_name: Annotated[str, Path()],
    body: Annotated[schemas.PeerUpdate, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> schemas.Peer:
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(peer_name, kind="peer")
    auth.require_peer(workspace_name, peer_name, scope)

    sets, bind = _update_sets(body.metadata, body.configuration)
    if not sets:
        return await _read_peer_or_404(workspace_name, peer_name)

    bind["n"] = peer_name
    bind["w"] = workspace_name
    async with session() as s:
        row = (
            await s.execute(
                text(
                    f"UPDATE peers SET {', '.join(sets)} "
                    "WHERE name = :n AND workspace_name = :w "
                    "RETURNING name, workspace_name, metadata, configuration, created_at"
                ),
                bind,
            )
        ).mappings().first()
        if row is None:
            raise not_found(f"peer {peer_name!r} not found in workspace {workspace_name!r}")
        await s.commit()

    return schemas.Peer.model_validate(dict(row))


@router.post("/{peer_name}/search", response_model=list[schemas.MessageHit])
@trace
async def search_peer_messages(
    workspace_name: Annotated[str, Path()],
    peer_name: Annotated[str, Path()],
    body: Annotated[schemas.MessageSearch, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> list[schemas.MessageHit]:
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(peer_name, kind="peer")
    auth.require_peer(workspace_name, peer_name, scope)

    hits = await vector_search.query_messages(
        query_text=body.query,
        workspace_name=workspace_name,
        peer_name=peer_name,
        session_name=body.session_name,
        num_results=body.limit,
        hybrid=True,
    )
    return [
        schemas.MessageHit(
            public_id=h.public_id,
            workspace_name=h.workspace_name,
            peer_name=h.peer_name,
            session_name=h.session_name,
            score=h.score,
        )
        for h in hits
    ]


# ---- internals --------------------------------------------------------------


async def _ensure_workspace(name: str) -> None:
    async with session() as s:
        row = (
            await s.execute(text("SELECT 1 FROM workspaces WHERE name = :n"), {"n": name})
        ).first()
    if row is None:
        raise not_found(f"workspace {name!r} not found")


async def _read_peer_or_404(workspace_name: str, peer_name: str) -> schemas.Peer:
    async with session() as s:
        row = (
            await s.execute(
                text(
                    "SELECT name, workspace_name, metadata, configuration, created_at "
                    "FROM peers WHERE name = :n AND workspace_name = :w"
                ),
                {"n": peer_name, "w": workspace_name},
            )
        ).mappings().first()
    if row is None:
        raise not_found(f"peer {peer_name!r} not found in workspace {workspace_name!r}")
    return schemas.Peer.model_validate(dict(row))


def _filter_clause(workspace_name: str, filters: dict | None) -> tuple[str, dict]:
    bind: dict = {"w": workspace_name}
    where = "WHERE workspace_name = :w"
    if filters:
        where += " AND metadata @> :f::jsonb"
        bind["f"] = json.dumps(filters)
    return where, bind


def _update_sets(
    metadata: dict | None, configuration: dict | None
) -> tuple[list[str], dict]:
    sets: list[str] = []
    bind: dict = {}
    if metadata is not None:
        sets.append("metadata = :m")
        bind["m"] = json.dumps(metadata)
    if configuration is not None:
        sets.append("configuration = :c")
        bind["c"] = json.dumps(configuration)
    return sets, bind
