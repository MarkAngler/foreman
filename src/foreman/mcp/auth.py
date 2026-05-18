"""Credential resolution for the foreman MCP server.

The server runs on the client machine (where Claude Code runs) and needs
(base_url, bearer_token) to call the deployed foreman API.

Resolution order — first match wins so power users / CI can bypass OAuth:
  - FOREMAN_BASE_URL env var, else databricks-sdk apps.get(foreman-{target}).url
  - FOREMAN_TOKEN env var (used verbatim as Bearer), else mint a short-TTL
    admin JWT in-process using the foreman jwt_signing_key secret. The latter
    requires the calling Databricks user to have read access to the secret
    scope — same trust boundary as the FastAPI app itself.

Two-layer auth when targeting a Databricks Apps URL:
  - Authorization: Bearer <databricks-oauth>  — satisfies the Apps OAuth proxy
  - X-Foreman-Token: <foreman-jwt>            — carries the foreman scope past the proxy
The Databricks OAuth headers come from WorkspaceClient.config.authenticate()
(the SDK handles refresh); they're empty when FOREMAN_TOKEN+FOREMAN_BASE_URL
bypass the SDK entirely (in-process / pure-foreman deployments).
"""

from __future__ import annotations

import base64
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Protocol

import jwt

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

JWT_ALG = "HS256"
DEFAULT_JWT_TTL_SECONDS = 600


class CredentialsError(RuntimeError):
    pass


class _TokenProvider(Protocol):
    def __call__(self) -> str: ...


def _no_databricks_headers() -> dict[str, str]:
    return {}


@dataclass
class Credentials:
    base_url: str
    _provider: _TokenProvider
    _databricks_headers: Callable[[], dict[str, str]] = field(default=_no_databricks_headers)

    def bearer_token(self) -> str:
        return self._provider()

    def databricks_auth_headers(self) -> dict[str, str]:
        return self._databricks_headers()


@lru_cache(maxsize=1)
def resolve_credentials() -> Credentials:
    env_token = os.environ.get("FOREMAN_TOKEN")
    env_url = os.environ.get("FOREMAN_BASE_URL")
    target = os.environ.get("FOREMAN_TARGET", "dev")
    secret_scope = os.environ.get("FOREMAN_SECRET_SCOPE", "foreman")
    signing_key_name = os.environ.get("FOREMAN_JWT_SECRET_KEY", "jwt_signing_key")

    if env_token and env_url:
        token = env_token
        return Credentials(base_url=env_url.rstrip("/"), _provider=lambda: token)

    w = _workspace_client()
    base_url = (env_url or _fetch_app_url(w, target)).rstrip("/")
    databricks_headers = _databricks_headers_provider(w)

    if env_token:
        token = env_token
        return Credentials(
            base_url=base_url,
            _provider=lambda: token,
            _databricks_headers=databricks_headers,
        )

    signing_key = _fetch_signing_key(w, secret_scope, signing_key_name)
    return Credentials(
        base_url=base_url,
        _provider=lambda: _mint_admin_jwt(signing_key),
        _databricks_headers=databricks_headers,
    )


def _databricks_headers_provider(w: WorkspaceClient) -> Callable[[], dict[str, str]]:
    def provide() -> dict[str, str]:
        try:
            return dict(w.config.authenticate())
        except Exception as e:
            raise CredentialsError(
                "could not obtain Databricks OAuth headers — run `databricks auth login`. "
                f"underlying: {e}"
            ) from e

    return provide


def _workspace_client() -> WorkspaceClient:
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as e:
        raise CredentialsError(
            "databricks-sdk not installed and FOREMAN_TOKEN/FOREMAN_BASE_URL not both set"
        ) from e
    try:
        return WorkspaceClient()
    except Exception as e:
        raise CredentialsError(
            "could not initialize WorkspaceClient — run `databricks auth login` or "
            f"set FOREMAN_TOKEN + FOREMAN_BASE_URL. underlying: {e}"
        ) from e


def _fetch_app_url(w: WorkspaceClient, target: str) -> str:
    name = f"foreman-{target}"
    try:
        app = w.apps.get(name=name)
    except Exception as e:
        raise CredentialsError(
            f"could not fetch Databricks app {name!r} — set FOREMAN_BASE_URL or "
            f"check FOREMAN_TARGET. underlying: {e}"
        ) from e
    url = getattr(app, "url", None)
    if not url:
        raise CredentialsError(f"Databricks app {name!r} has no url; is it deployed?")
    return url


def _fetch_signing_key(w: WorkspaceClient, scope: str, key: str) -> str:
    try:
        resp = w.secrets.get_secret(scope=scope, key=key)
    except Exception as e:
        raise CredentialsError(
            f"could not read secret {scope}/{key} — set FOREMAN_TOKEN or grant "
            f"access to the foreman secret scope. underlying: {e}"
        ) from e
    encoded = getattr(resp, "value", None)
    if not encoded:
        raise CredentialsError(f"secret {scope}/{key} is empty")
    return base64.b64decode(encoded).decode("utf-8")


def _mint_admin_jwt(signing_key: str, ttl_seconds: int = DEFAULT_JWT_TTL_SECONDS) -> str:
    now = int(time.time())
    return jwt.encode(
        {"ad": True, "t": now, "exp": now + ttl_seconds},
        signing_key,
        algorithm=JWT_ALG,
    )
