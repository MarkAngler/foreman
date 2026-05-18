"""Admin-only JWT issuance.

A separate /keys router rather than /workspaces/{w}/keys because keys can be
admin-scoped (cross-workspace), and the admin scope is the prerequisite for any
issuance — putting it under a workspace prefix would lie about the trust model.
"""

from __future__ import annotations

import datetime
from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, status

from foreman.app import schemas
from foreman.lib import auth
from foreman.lib.tracing import trace

router = APIRouter(prefix="/keys", tags=["keys"])


@router.post(
    "", response_model=schemas.IssuedKey, status_code=status.HTTP_201_CREATED
)
@trace
async def issue_key(
    body: Annotated[schemas.KeyIssue, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> schemas.IssuedKey:
    if not scope.admin:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "admin token required")
    if body.peer_name and body.session_name:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "key cannot be both peer-scoped and session-scoped",
        )

    try:
        token = auth.issue(
            admin=body.admin,
            workspace=body.workspace_name,
            peer=body.peer_name,
            session=body.session_name,
            ttl_seconds=body.ttl_seconds,
        )
    except ValueError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e)) from e

    expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=body.ttl_seconds
    )
    return schemas.IssuedKey(token=token, expires_at=expires_at)
