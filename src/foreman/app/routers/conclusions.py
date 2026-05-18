"""Conclusions = honcho's documents API (collections, observations, derived facts).

Manual creates write directly to the `documents` table; semantic queries hit
documents_idx (Delta Sync, managed embeddings) via vector_search.query_documents.
The deriver/dreamer jobs write to documents_delta separately — those land in
documents_mirror via the Delta -> Lakebase sync.
"""

from __future__ import annotations

import json
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path, Response, status
from fastapi_pagination import Page, Params, paginate
from nanoid import generate as nanoid
from sqlalchemy import text

from foreman.app import schemas
from foreman.app.routers._common import assert_path_name, not_found
from foreman.lib import auth, vector_search
from foreman.lib.lakebase import session
from foreman.lib.tracing import trace

router = APIRouter(
    prefix="/workspaces/{workspace_name}/conclusions", tags=["conclusions"]
)

_DOCUMENT_COLUMNS = (
    "id, workspace_name, observer, observed, level, content, "
    "source_ids, times_derived, metadata, created_at"
)
_DOC_ID_LEN = 21


@router.post(
    "", response_model=list[schemas.Conclusion], status_code=status.HTTP_201_CREATED
)
@trace
async def create_conclusions(
    workspace_name: Annotated[str, Path()],
    body: Annotated[schemas.ConclusionBatchCreate, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> list[schemas.Conclusion]:
    assert_path_name(workspace_name, kind="workspace")
    auth.require_workspace(workspace_name, scope)

    rows: list[dict] = []
    async with session() as s:
        for c in body.conclusions:
            row = (
                await s.execute(
                    text(
                        "INSERT INTO documents "
                        "(id, workspace_name, observer, observed, level, "
                        " content, source_ids, metadata) "
                        "VALUES (:id, :w, :ob, :od, :lv, :ct, :src, :m) "
                        f"RETURNING {_DOCUMENT_COLUMNS}"
                    ),
                    {
                        "id": nanoid(size=_DOC_ID_LEN),
                        "w": workspace_name,
                        "ob": c.observer,
                        "od": c.observed,
                        "lv": c.level,
                        "ct": c.content,
                        "src": c.source_ids,
                        "m": json.dumps(c.metadata),
                    },
                )
            ).mappings().one()
            rows.append(dict(row))
        await s.commit()

    return [schemas.Conclusion.model_validate(r) for r in rows]


@router.post("/list", response_model=Page[schemas.Conclusion])
@trace
async def list_conclusions(
    workspace_name: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
    params: Annotated[Params, Depends()],
    body: Annotated[schemas.ConclusionList | None, Body()] = None,
) -> Page[schemas.Conclusion]:
    assert_path_name(workspace_name, kind="workspace")
    auth.require_workspace(workspace_name, scope)

    bind: dict = {"w": workspace_name}
    where = "WHERE workspace_name = :w AND deleted_at IS NULL"
    if body and body.filters:
        # promote known filter keys into typed equality predicates; everything
        # else falls through to JSONB containment on metadata.
        promoted_keys = {"observer", "observed", "level"}
        promoted = {k: v for k, v in body.filters.items() if k in promoted_keys}
        residual = {k: v for k, v in body.filters.items() if k not in promoted_keys}
        for k, v in promoted.items():
            where += f" AND {k} = :{k}"
            bind[k] = v
        if residual:
            where += " AND metadata @> :f::jsonb"
            bind["f"] = json.dumps(residual)

    async with session() as s:
        rows = (
            await s.execute(
                text(
                    f"SELECT {_DOCUMENT_COLUMNS} FROM documents {where} "
                    "ORDER BY created_at DESC"
                ),
                bind,
            )
        ).mappings().all()

    items = [schemas.Conclusion.model_validate(dict(r)) for r in rows]
    return paginate(items, params)


@router.post("/query", response_model=list[schemas.Conclusion])
@trace
async def query_conclusions(
    workspace_name: Annotated[str, Path()],
    body: Annotated[schemas.ConclusionQuery, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> list[schemas.Conclusion]:
    """Semantic search over documents_idx, then hydrate full rows from documents."""
    assert_path_name(workspace_name, kind="workspace")
    auth.require_workspace(workspace_name, scope)

    hits = await vector_search.query_documents(
        query_text=body.query,
        workspace_name=workspace_name,
        observer=body.observer,
        observed=body.observed,
        level=body.level,
        num_results=body.top_k,
        hybrid=False,
    )
    if not hits:
        return []

    ids = [h.id for h in hits]
    async with session() as s:
        rows = (
            await s.execute(
                text(
                    f"SELECT {_DOCUMENT_COLUMNS} FROM documents "
                    "WHERE workspace_name = :w AND id = ANY(:ids) AND deleted_at IS NULL"
                ),
                {"w": workspace_name, "ids": ids},
            )
        ).mappings().all()

    by_id = {r["id"]: dict(r) for r in rows}
    return [schemas.Conclusion.model_validate(by_id[i]) for i in ids if i in by_id]


@router.delete(
    "/{conclusion_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    response_model=None,
)
@trace
async def delete_conclusion(
    workspace_name: Annotated[str, Path()],
    conclusion_id: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> Response:
    assert_path_name(workspace_name, kind="workspace")
    auth.require_workspace(workspace_name, scope)

    async with session() as s:
        row = (
            await s.execute(
                text(
                    "UPDATE documents SET deleted_at = CURRENT_DATE "
                    "WHERE workspace_name = :w AND id = :id AND deleted_at IS NULL "
                    "RETURNING id"
                ),
                {"w": workspace_name, "id": conclusion_id},
            )
        ).first()
        if row is None:
            raise not_found(f"conclusion {conclusion_id!r} not found")
        await s.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
