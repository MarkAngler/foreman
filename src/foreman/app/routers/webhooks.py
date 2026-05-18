"""Webhook endpoint registration + manual test trigger.

Delivery is owned by foreman.jobs.webhooks; this router only manages the
endpoint roster and enqueues a `webhook` queue_item for the test trigger.
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
from foreman.lib import auth
from foreman.lib.lakebase import session
from foreman.lib.tracing import trace

router = APIRouter(prefix="/workspaces/{workspace_name}/webhooks", tags=["webhooks"])

_ENDPOINT_COLUMNS = (
    "id, workspace_name, url, event_types, metadata, created_at"
)
_ID_LEN = 21


@router.post(
    "", response_model=schemas.WebhookEndpoint, status_code=status.HTTP_201_CREATED
)
@trace
async def register_endpoint(
    workspace_name: Annotated[str, Path()],
    body: Annotated[schemas.WebhookEndpointCreate, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> schemas.WebhookEndpoint:
    assert_path_name(workspace_name, kind="workspace")
    auth.require_workspace(workspace_name, scope)
    await _ensure_workspace(workspace_name)

    async with session() as s:
        row = (
            await s.execute(
                text(
                    "INSERT INTO webhook_endpoints "
                    "(id, workspace_name, url, secret, event_types, metadata) "
                    "VALUES (:id, :w, :u, :s, :e, :m) "
                    f"RETURNING {_ENDPOINT_COLUMNS}"
                ),
                {
                    "id": nanoid(size=_ID_LEN),
                    "w": workspace_name,
                    "u": body.url,
                    "s": body.secret,
                    "e": body.event_types,
                    "m": json.dumps(body.metadata),
                },
            )
        ).mappings().one()
        await s.commit()
    return schemas.WebhookEndpoint.model_validate(dict(row))


@router.get("", response_model=Page[schemas.WebhookEndpoint])
@trace
async def list_endpoints(
    workspace_name: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
    params: Annotated[Params, Depends()],
) -> Page[schemas.WebhookEndpoint]:
    assert_path_name(workspace_name, kind="workspace")
    auth.require_workspace(workspace_name, scope)

    async with session() as s:
        rows = (
            await s.execute(
                text(
                    f"SELECT {_ENDPOINT_COLUMNS} FROM webhook_endpoints "
                    "WHERE workspace_name = :w ORDER BY created_at DESC"
                ),
                {"w": workspace_name},
            )
        ).mappings().all()

    items = [schemas.WebhookEndpoint.model_validate(dict(r)) for r in rows]
    return paginate(items, params)


@router.delete(
    "/{endpoint_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    response_model=None,
)
@trace
async def delete_endpoint(
    workspace_name: Annotated[str, Path()],
    endpoint_id: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> Response:
    assert_path_name(workspace_name, kind="workspace")
    auth.require_workspace(workspace_name, scope)

    async with session() as s:
        row = (
            await s.execute(
                text(
                    "DELETE FROM webhook_endpoints "
                    "WHERE workspace_name = :w AND id = :id RETURNING id"
                ),
                {"w": workspace_name, "id": endpoint_id},
            )
        ).first()
        if row is None:
            raise not_found(f"webhook endpoint {endpoint_id!r} not found")
        await s.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/test", status_code=status.HTTP_202_ACCEPTED)
@trace
async def trigger_test_event(
    workspace_name: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> dict[str, str]:
    """Enqueue a `test` webhook event for this workspace. The webhooks job will
    deliver it on its next poll. Returns the queue work-unit key for tracing."""
    assert_path_name(workspace_name, kind="workspace")
    auth.require_workspace(workspace_name, scope)

    work_unit_key = f"webhook:test:{workspace_name}:{nanoid(size=_ID_LEN)}"
    payload = {
        "event_type": "test",
        "workspace_name": workspace_name,
        "data": {"message": "test webhook from /webhooks/test"},
    }
    async with session() as s:
        await s.execute(
            text(
                "INSERT INTO queue_items (task_type, work_unit_key, payload) "
                "VALUES ('webhook', :wuk, :p)"
            ),
            {"wuk": work_unit_key, "p": json.dumps(payload)},
        )
        await s.commit()
    return {"work_unit_key": work_unit_key, "status": "queued"}


# ---- internals --------------------------------------------------------------


async def _ensure_workspace(name: str) -> None:
    async with session() as s:
        row = (
            await s.execute(text("SELECT 1 FROM workspaces WHERE name = :n"), {"n": name})
        ).first()
    if row is None:
        raise not_found(f"workspace {name!r} not found")
