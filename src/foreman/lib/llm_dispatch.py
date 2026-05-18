"""Per-workspace LLM endpoint dispatcher.

Resolution order for endpoint name:
    1. workspace_llm_config table row for (workspace_name, role)  — Lakebase
    2. install-default per role from settings()

Provider routing:
    - 'fmapi' / 'external' / 'custom': use the Databricks SDK serving endpoints client.
    - 'openai-compatible': use the OpenAI client pointed at the workspace's serving URL.

Same module imported by both the FastAPI app (async path via asyncpg/SQLAlchemy)
and Spark Jobs (sync path via psycopg). Each entrypoint gets its own resolver.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncIterator, Literal

from databricks.sdk import WorkspaceClient
from sqlalchemy import text

from foreman.lib.config import settings

Role = Literal["dialectic", "deriver", "summarizer", "dreamer", "embeddings"]
Provider = Literal["fmapi", "external", "openai-compatible", "custom"]


@dataclass(frozen=True)
class EndpointConfig:
    endpoint_name: str
    provider: Provider
    params: dict[str, Any]


# ---- Config resolution -------------------------------------------------------


async def resolve_async(workspace_name: str, role: Role) -> EndpointConfig:
    """Resolve endpoint config from Lakebase. Used by the FastAPI app."""
    from foreman.lib.lakebase import session

    async with session() as s:
        row = (await s.execute(
            text(
                "SELECT endpoint_name, provider, params FROM workspace_llm_config "
                "WHERE workspace_name = :w AND role = :r"
            ),
            {"w": workspace_name, "r": role},
        )).mappings().first()

    if row is None:
        return EndpointConfig(
            endpoint_name=settings().default_endpoint_for(role),
            provider="fmapi",
            params={},
        )
    return EndpointConfig(
        endpoint_name=row["endpoint_name"],
        provider=row["provider"],
        params=row["params"] or {},
    )


def resolve_sync(workspace_name: str, role: Role) -> EndpointConfig:
    """Resolve endpoint config from Lakebase via psycopg. Used by Spark Jobs."""
    import psycopg
    import uuid

    s = settings()
    w = WorkspaceClient()
    instance = w.database.get_database_instance(name=s.lakebase_instance)
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[s.lakebase_instance],
    )
    user = s.lakebase_username or w.current_user.me().user_name
    conn_str = (
        f"host={instance.read_write_dns} port=5432 dbname={s.lakebase_database} "
        f"user={user} password={cred.token} sslmode=require"
    )
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT endpoint_name, provider, params FROM workspace_llm_config "
                "WHERE workspace_name = %s AND role = %s",
                (workspace_name, role),
            )
            row = cur.fetchone()

    if row is None:
        return EndpointConfig(
            endpoint_name=s.default_endpoint_for(role),
            provider="fmapi",
            params={},
        )
    return EndpointConfig(endpoint_name=row[0], provider=row[1], params=row[2] or {})


# ---- Invocation --------------------------------------------------------------


async def chat(
    *,
    workspace_name: str,
    role: Role,
    messages: list[dict[str, Any]],
    stream: bool = False,
    **overrides: Any,
) -> Any | AsyncIterator[Any]:
    """Issue a chat completion against the resolved endpoint.

    Returns either a single response object (stream=False) or an async iterator
    of streaming chunks (stream=True). Caller is responsible for shape parsing —
    this is a thin uniform call helper.
    """
    cfg = await resolve_async(workspace_name, role)
    return await _invoke(cfg, messages=messages, stream=stream, **overrides)


def chat_sync(
    *,
    workspace_name: str,
    role: Role,
    messages: list[dict[str, Any]],
    **overrides: Any,
) -> Any:
    """Sync variant for Spark jobs (non-streaming)."""
    cfg = resolve_sync(workspace_name, role)
    return _invoke_sync(cfg, messages=messages, **overrides)


async def _invoke(cfg: EndpointConfig, *, messages, stream: bool, **kwargs) -> Any:
    # The Databricks SDK's serving_endpoints.query has no `tools` parameter, so
    # any kwargs containing tool definitions must go through the OpenAI-compatible
    # path against the same endpoint. Honored regardless of declared provider.
    if cfg.provider == "openai-compatible" or "tools" in kwargs:
        return await _invoke_openai(cfg, messages=messages, stream=stream, **kwargs)
    payload = {"messages": messages, **(cfg.params or {}), **kwargs}
    if stream:
        return _sdk_stream(cfg.endpoint_name, payload)
    return await asyncio.to_thread(
        WorkspaceClient().serving_endpoints.query,
        name=cfg.endpoint_name,
        messages=[_to_sdk_message(m) for m in messages],
        **{k: v for k, v in (cfg.params or {}).items() if k not in {"messages"}},
        **{k: v for k, v in kwargs.items() if k not in {"messages"}},
    )


def _invoke_sync(cfg: EndpointConfig, *, messages, **kwargs) -> Any:
    if cfg.provider == "openai-compatible" or "tools" in kwargs:
        from openai import OpenAI

        client = _openai_client()
        return client.chat.completions.create(
            model=cfg.endpoint_name, messages=messages, **(cfg.params or {}), **kwargs
        )
    return WorkspaceClient().serving_endpoints.query(
        name=cfg.endpoint_name,
        messages=[_to_sdk_message(m) for m in messages],
        **{k: v for k, v in (cfg.params or {}).items() if k not in {"messages"}},
        **{k: v for k, v in kwargs.items() if k not in {"messages"}},
    )


def _to_sdk_message(m: dict[str, Any]):
    """Convert plain {role, content} dicts to the SDK's ChatMessage if needed."""
    from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

    if isinstance(m, ChatMessage):
        return m
    return ChatMessage(role=ChatMessageRole(m["role"]), content=m["content"])


async def _sdk_stream(endpoint_name: str, payload: dict) -> AsyncIterator[Any]:
    """Wrap the SDK streaming generator in an async iterator."""
    def _gen():
        return WorkspaceClient().serving_endpoints.query(
            name=endpoint_name, stream=True, **payload
        )

    iterator = await asyncio.to_thread(_gen)
    loop = asyncio.get_running_loop()
    while True:
        try:
            chunk = await loop.run_in_executor(None, next, iterator)
        except StopIteration:
            return
        yield chunk


async def _invoke_openai(cfg: EndpointConfig, *, messages, stream: bool, **kwargs):
    client = _openai_client(async_=True)
    return await client.chat.completions.create(
        model=cfg.endpoint_name,
        messages=messages,
        stream=stream,
        **(cfg.params or {}),
        **kwargs,
    )


def _openai_client(async_: bool = False):
    """Build an OpenAI-compatible client pointed at the current workspace's serving URL."""
    from openai import AsyncOpenAI, OpenAI

    w = WorkspaceClient()
    base_url = f"{w.config.host}/serving-endpoints"
    token = w.config.token or w.config.oauth_token().access_token
    cls = AsyncOpenAI if async_ else OpenAI
    return cls(api_key=token, base_url=base_url)
