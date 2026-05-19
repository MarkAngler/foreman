"""OBO identity resolver for the Genie-facing MCP.

Databricks Apps forwards the calling user's OAuth token in the
`x-forwarded-access-token` header once OBO authorization is enabled and the
app has been fully stopped and restarted. We exchange that token via SCIM
`/Me` to obtain the user's email, then map email -> a Foreman peer_name.

There is no fallback: missing header -> 401, SCIM rejection -> 401. The
peer-isolation invariant of the Genie surface depends on this being strict.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass

import httpx
from databricks.sdk import WorkspaceClient
from fastapi import HTTPException, Request, status

HEADER_NAME = "x-forwarded-access-token"
_CACHE_TTL_SECONDS = 60


@dataclass(frozen=True)
class _CacheEntry:
    email: str
    expires_at: float


_email_cache: dict[str, _CacheEntry] = {}


def _cache_key(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _workspace_host() -> str:
    host = WorkspaceClient().config.host
    if not host:
        raise RuntimeError("databricks workspace host not configured")
    return host.rstrip("/")


async def _fetch_email(token: str) -> str:
    url = f"{_workspace_host()}/api/2.0/preview/scim/v2/Me"
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
    if resp.status_code in (401, 403):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "OBO token rejected by SCIM /Me",
        )
    if resp.status_code >= 400:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            f"SCIM /Me returned {resp.status_code}",
        )
    body = resp.json()
    emails = body.get("emails") or []
    if not emails or not emails[0].get("value"):
        user_name = body.get("userName")
        if not user_name:
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY,
                "SCIM /Me returned no email",
            )
        return str(user_name)
    return str(emails[0]["value"])


async def current_user_email(request: Request) -> str:
    token = request.headers.get(HEADER_NAME)
    if not token:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            f"missing {HEADER_NAME}; OBO not enabled or token not forwarded",
        )
    key = _cache_key(token)
    now = time.time()
    cached = _email_cache.get(key)
    if cached is not None and cached.expires_at > now:
        return cached.email
    email = await asyncio.shield(_fetch_email(token))
    _email_cache[key] = _CacheEntry(email=email, expires_at=now + _CACHE_TTL_SECONDS)
    return email


def peer_name_for(email: str) -> str:
    """Map an email to a Foreman peer name.

    The peers table enforces `^[A-Za-z0-9_\\-]+$` and length 1..256. Lower-case
    the email, replace `@` with `-at-`, dots with `-`, and collapse any other
    non-conforming char to `-`.
    """
    normalized = email.strip().lower().replace("@", "-at-").replace(".", "-")
    cleaned = "".join(c if c.isalnum() or c in {"-", "_"} else "-" for c in normalized)
    cleaned = cleaned.strip("-")
    if not cleaned:
        raise ValueError(f"cannot derive peer_name from email {email!r}")
    return cleaned[:256]
