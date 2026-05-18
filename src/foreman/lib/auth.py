"""Hierarchical JWT auth ported from honcho.

Token payload (all optional except t/exp):
    ad?: bool        — admin (full access across workspaces)
    w?:  str         — workspace_name
    p?:  str         — peer_name (requires w)
    s?:  str         — session_name (requires w)
    t:   int         — issued-at (epoch seconds)
    exp: int         — expiry (epoch seconds)

Validation rules:
    - admin token: all access
    - workspace token: must match path workspace
    - peer token: must match path workspace AND peer
    - session token: must match path workspace AND session

Signing key is loaded from a Databricks secret (resolved at import time, cached).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Annotated

import jwt
from databricks.sdk import WorkspaceClient
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer

from foreman.lib.config import settings

ALGORITHM = "HS256"


@dataclass(frozen=True)
class Scope:
    admin: bool = False
    workspace: str | None = None
    peer: str | None = None
    session: str | None = None


@lru_cache(maxsize=1)
def signing_key() -> str:
    s = settings()
    w = WorkspaceClient()
    secret = w.secrets.get_secret(scope=s.jwt_secret_scope, key=s.jwt_secret_key)
    # SDK returns base64-encoded bytes; decode and use raw text as the HMAC key.
    import base64

    return base64.b64decode(secret.value).decode("utf-8")


def issue(
    *,
    admin: bool = False,
    workspace: str | None = None,
    peer: str | None = None,
    session: str | None = None,
    ttl_seconds: int = 60 * 60 * 24 * 30,
) -> str:
    """Mint a scoped JWT. At least one of (admin, workspace) must be set."""
    if not admin and not workspace:
        raise ValueError("must specify admin=True or workspace=...")
    if peer and not workspace:
        raise ValueError("peer scope requires workspace")
    if session and not workspace:
        raise ValueError("session scope requires workspace")

    now = int(time.time())
    payload: dict = {"t": now, "exp": now + ttl_seconds}
    if admin:
        payload["ad"] = True
    if workspace:
        payload["w"] = workspace
    if peer:
        payload["p"] = peer
    if session:
        payload["s"] = session

    return jwt.encode(payload, signing_key(), algorithm=ALGORITHM)


def decode(token: str) -> Scope:
    try:
        claims = jwt.decode(token, signing_key(), algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError as e:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "token expired") from e
    except jwt.InvalidTokenError as e:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid token") from e

    return Scope(
        admin=bool(claims.get("ad", False)),
        workspace=claims.get("w"),
        peer=claims.get("p"),
        session=claims.get("s"),
    )


_bearer = HTTPBearer(auto_error=False)


async def current_scope(request: Request) -> Scope:
    # Databricks Apps OAuth proxy terminates the Authorization header for its
    # own use; clients shuttle the foreman JWT under X-Foreman-Token. Check
    # the custom header first, fall back to Authorization for in-process tests
    # and pure-foreman deployments where the proxy isn't in front.
    custom = request.headers.get("x-foreman-token")
    if custom:
        return decode(custom)
    creds = await _bearer(request)
    if creds is None or not creds.credentials:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing bearer token")
    return decode(creds.credentials)


def require_workspace(workspace_name: str, scope: Scope) -> None:
    if scope.admin:
        return
    if scope.workspace != workspace_name:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "token not scoped to this workspace")


def require_peer(workspace_name: str, peer_name: str, scope: Scope) -> None:
    require_workspace(workspace_name, scope)
    if scope.admin or scope.workspace == workspace_name and scope.peer is None:
        # admin or workspace-level token has implicit access to all peers
        return
    if scope.peer != peer_name:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "token not scoped to this peer")


def require_session(workspace_name: str, session_name: str, scope: Scope) -> None:
    require_workspace(workspace_name, scope)
    if scope.admin or (
        scope.workspace == workspace_name and scope.session is None and scope.peer is None
    ):
        return
    if scope.session != session_name:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "token not scoped to this session")


# Convenience FastAPI dependency factories.


def workspace_guard(workspace_name: str):
    async def _dep(scope: Annotated[Scope, Depends(current_scope)]) -> Scope:
        require_workspace(workspace_name, scope)
        return scope

    return _dep


def request_workspace_guard():
    """Use when workspace_name comes from path params; pulls from request.path_params."""

    async def _dep(
        request: Request,
        scope: Annotated[Scope, Depends(current_scope)],
    ) -> Scope:
        ws = request.path_params.get("workspace_name") or request.path_params.get("workspace_id")
        if ws is None:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "workspace not in path")
        require_workspace(ws, scope)
        return scope

    return _dep
