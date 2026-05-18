"""Workspace CRUD + workspace-wide message search.

Honcho parity (semantics from src/routers/workspaces.py upstream) with a clean
URL shape: /workspaces, /workspaces/{workspace_name}, etc.
"""

from __future__ import annotations

import json
from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Response, status
from fastapi_pagination import Page, Params, paginate
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from foreman.app import schemas
from foreman.app.routers._common import assert_path_name, not_found
from foreman.lib import auth, vector_search
from foreman.lib.lakebase import session
from foreman.lib.tracing import trace

router = APIRouter(prefix="/workspaces", tags=["workspaces"])


@router.post("", response_model=schemas.Workspace)
@trace
async def create_workspace(
    response: Response,
    body: Annotated[schemas.WorkspaceCreate, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> schemas.Workspace:
    """Get-or-create a workspace by name. 201 if created, 200 if it already existed."""
    auth.require_workspace(body.name, scope)

    async with session() as s:
        existing = (
            await s.execute(
                text(
                    "SELECT name, metadata, configuration, created_at "
                    "FROM workspaces WHERE name = :n"
                ),
                {"n": body.name},
            )
        ).mappings().first()

        if existing is not None:
            response.status_code = status.HTTP_200_OK
            return schemas.Workspace.model_validate(dict(existing))

        try:
            row = (
                await s.execute(
                    text(
                        "INSERT INTO workspaces (id, name, metadata, configuration) "
                        "VALUES (:id, :n, :m, :c) "
                        "RETURNING name, metadata, configuration, created_at"
                    ),
                    {
                        "id": body.name,
                        "n": body.name,
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
                        "SELECT name, metadata, configuration, created_at "
                        "FROM workspaces WHERE name = :n"
                    ),
                    {"n": body.name},
                )
            ).mappings().one()
            response.status_code = status.HTTP_200_OK
            return schemas.Workspace.model_validate(dict(row))

    response.status_code = status.HTTP_201_CREATED
    return schemas.Workspace.model_validate(dict(row))


@router.post("/list", response_model=Page[schemas.Workspace])
@trace
async def list_workspaces(
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
    params: Annotated[Params, Depends()],
    body: Annotated[schemas.WorkspaceList | None, Body()] = None,
) -> Page[schemas.Workspace]:
    """Paginated list of workspaces. Admin-only — workspace tokens cannot enumerate."""
    if not scope.admin:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "admin token required")

    where_sql, bind = _filter_clause(body.filters if body else None)
    async with session() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT name, metadata, configuration, created_at "
                    "FROM workspaces "
                    f"{where_sql} "
                    "ORDER BY created_at DESC"
                ),
                bind,
            )
        ).mappings().all()

    items = [schemas.Workspace.model_validate(dict(r)) for r in rows]
    return paginate(items, params)


@router.put("/{workspace_name}", response_model=schemas.Workspace)
@trace
async def update_workspace(
    workspace_name: Annotated[str, Path()],
    body: Annotated[schemas.WorkspaceUpdate, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> schemas.Workspace:
    assert_path_name(workspace_name, kind="workspace")
    auth.require_workspace(workspace_name, scope)

    sets, bind = _update_sets(body.metadata, body.configuration)
    if not sets:
        return await _read_workspace_or_404(workspace_name)

    bind["n"] = workspace_name
    async with session() as s:
        row = (
            await s.execute(
                text(
                    f"UPDATE workspaces SET {', '.join(sets)} WHERE name = :n "
                    "RETURNING name, metadata, configuration, created_at"
                ),
                bind,
            )
        ).mappings().first()
        if row is None:
            raise not_found(f"workspace {workspace_name!r} not found")
        await s.commit()

    return schemas.Workspace.model_validate(dict(row))


@router.post("/{workspace_name}/search", response_model=list[schemas.MessageHit])
@trace
async def search_workspace_messages(
    workspace_name: Annotated[str, Path()],
    body: Annotated[schemas.MessageSearch, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> list[schemas.MessageHit]:
    """Hybrid search over messages_idx scoped to this workspace (+ optional peer/session)."""
    assert_path_name(workspace_name, kind="workspace")
    auth.require_workspace(workspace_name, scope)

    hits = await vector_search.query_messages(
        query_text=body.query,
        workspace_name=workspace_name,
        peer_name=body.peer_name,
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
            content=h.content,
            score=h.score,
        )
        for h in hits
    ]


# ---- internals --------------------------------------------------------------


async def _read_workspace_or_404(name: str) -> schemas.Workspace:
    async with session() as s:
        row = (
            await s.execute(
                text(
                    "SELECT name, metadata, configuration, created_at "
                    "FROM workspaces WHERE name = :n"
                ),
                {"n": name},
            )
        ).mappings().first()
    if row is None:
        raise not_found(f"workspace {name!r} not found")
    return schemas.Workspace.model_validate(dict(row))


def _filter_clause(filters: dict | None) -> tuple[str, dict]:
    """JSONB containment WHERE clause. Honcho-style filter passthrough."""
    if not filters:
        return "", {}
    return "WHERE metadata @> :f::jsonb", {"f": json.dumps(filters)}


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
