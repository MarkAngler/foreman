"""Session CRUD + peer membership + clone + context + summaries + search.

Honcho parity (semantics from src/routers/sessions.py upstream).
"""

from __future__ import annotations

import json
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path, Query, Response, status
from fastapi_pagination import Page, Params, paginate
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from foreman.app import schemas
from foreman.app.routers._common import (
    assert_path_name,
    bad_request,
    conflict,
    not_found,
)
from foreman.lib import auth, vector_search
from foreman.lib.lakebase import session
from foreman.lib.tracing import trace

router = APIRouter(prefix="/workspaces/{workspace_name}/sessions", tags=["sessions"])

DEFAULT_CONTEXT_TOKENS = 4_000


@router.post("", response_model=schemas.Session)
@trace
async def create_session(
    response: Response,
    workspace_name: Annotated[str, Path()],
    body: Annotated[schemas.SessionCreate, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> schemas.Session:
    assert_path_name(workspace_name, kind="workspace")
    auth.require_session(workspace_name, body.name, scope)
    await _ensure_workspace(workspace_name)

    async with session() as s:
        existing = (
            (
                await s.execute(
                    text(_SELECT_SESSION),
                    {"n": body.name, "w": workspace_name},
                )
            )
            .mappings()
            .first()
        )

        if existing is not None:
            row = existing
            response.status_code = status.HTTP_200_OK
        else:
            try:
                row = (
                    (
                        await s.execute(
                            text(
                                "INSERT INTO sessions (name, workspace_name, metadata, configuration) "
                                "VALUES (:n, :w, :m, :c) "
                                f"RETURNING {_SESSION_COLUMNS}"
                            ),
                            {
                                "n": body.name,
                                "w": workspace_name,
                                "m": json.dumps(body.metadata),
                                "c": json.dumps(body.configuration),
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                response.status_code = status.HTTP_201_CREATED
            except IntegrityError:
                await s.rollback()
                row = (
                    (
                        await s.execute(
                            text(_SELECT_SESSION),
                            {"n": body.name, "w": workspace_name},
                        )
                    )
                    .mappings()
                    .one()
                )
                response.status_code = status.HTTP_200_OK

        if body.peers:
            await _upsert_session_peers(s, workspace_name, body.name, body.peers)
        await s.commit()

    return _row_to_session(row)


@router.post("/list", response_model=Page[schemas.Session])
@trace
async def list_sessions(
    workspace_name: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
    params: Annotated[Params, Depends()],
    body: Annotated[schemas.SessionList | None, Body()] = None,
) -> Page[schemas.Session]:
    assert_path_name(workspace_name, kind="workspace")
    auth.require_workspace(workspace_name, scope)

    where, bind = _filter_clause(workspace_name, body.filters if body else None)
    async with session() as s:
        rows = (
            (
                await s.execute(
                    text(
                        f"SELECT {_SESSION_COLUMNS} FROM sessions {where} ORDER BY created_at DESC"
                    ),
                    bind,
                )
            )
            .mappings()
            .all()
        )

    items = [_row_to_session(r) for r in rows]
    return paginate(items, params)


@router.put("/{session_name}", response_model=schemas.Session)
@trace
async def update_session(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    body: Annotated[schemas.SessionUpdate, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> schemas.Session:
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)

    sets, bind = _update_sets(body.metadata, body.configuration)
    if not sets:
        return await _read_session_or_404(workspace_name, session_name)

    bind["n"] = session_name
    bind["w"] = workspace_name
    async with session() as s:
        row = (
            (
                await s.execute(
                    text(
                        f"UPDATE sessions SET {', '.join(sets)} "
                        "WHERE name = :n AND workspace_name = :w "
                        f"RETURNING {_SESSION_COLUMNS}"
                    ),
                    bind,
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise not_found(f"session {session_name!r} not found")
        await s.commit()

    return _row_to_session(row)


@router.delete(
    "/{session_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    response_model=None,
)
@trace
async def delete_session(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> Response:
    """Delete a session. Returns 409 if any peers are still active in it."""
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)

    async with session() as s:
        row = (
            (
                await s.execute(
                    text(_SELECT_SESSION),
                    {"n": session_name, "w": workspace_name},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise not_found(f"session {session_name!r} not found")

        active = (
            (
                await s.execute(
                    text(
                        "SELECT COUNT(*) AS c FROM session_peers "
                        "WHERE session_name = :n AND workspace_name = :w AND left_at IS NULL"
                    ),
                    {"n": session_name, "w": workspace_name},
                )
            )
            .mappings()
            .one()
        )
        if active["c"] > 0:
            raise conflict(
                f"session {session_name!r} has {active['c']} active peer(s); "
                "remove them before deleting"
            )

        await s.execute(
            text("DELETE FROM sessions WHERE name = :n AND workspace_name = :w"),
            {"n": session_name, "w": workspace_name},
        )
        await s.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/{session_name}/clone", response_model=schemas.Session, status_code=status.HTTP_201_CREATED
)
@trace
async def clone_session(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    body: Annotated[schemas.SessionClone, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> schemas.Session:
    """Copy a session and its messages (optionally up to a cutoff message public_id)."""
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)
    assert_path_name(body.target_name, kind="session")

    async with session() as s:
        src = (
            (
                await s.execute(
                    text(_SELECT_SESSION),
                    {"n": session_name, "w": workspace_name},
                )
            )
            .mappings()
            .first()
        )
        if src is None:
            raise not_found(f"session {session_name!r} not found")

        cutoff_id: int | None = None
        if body.cutoff_message_id:
            cutoff = (
                (
                    await s.execute(
                        text(
                            "SELECT id FROM messages WHERE public_id = :pid "
                            "AND workspace_name = :w AND session_name = :n"
                        ),
                        {"pid": body.cutoff_message_id, "w": workspace_name, "n": session_name},
                    )
                )
                .mappings()
                .first()
            )
            if cutoff is None:
                raise not_found(f"cutoff message {body.cutoff_message_id!r} not found")
            cutoff_id = cutoff["id"]

        try:
            new_row = (
                (
                    await s.execute(
                        text(
                            "INSERT INTO sessions (name, workspace_name, metadata, configuration) "
                            "VALUES (:n, :w, :m, :c) "
                            f"RETURNING {_SESSION_COLUMNS}"
                        ),
                        {
                            "n": body.target_name,
                            "w": workspace_name,
                            "m": json.dumps(dict(src["metadata"])),
                            "c": json.dumps(dict(src["configuration"])),
                        },
                    )
                )
                .mappings()
                .one()
            )
        except IntegrityError as e:
            await s.rollback()
            raise conflict(f"session {body.target_name!r} already exists") from e

        message_filter = "AND id <= :cid" if cutoff_id is not None else ""
        bind: dict = {"src": session_name, "w": workspace_name, "tgt": body.target_name}
        if cutoff_id is not None:
            bind["cid"] = cutoff_id

        await s.execute(
            text(
                "INSERT INTO messages "
                "(public_id, session_name, peer_name, workspace_name, content, "
                " token_count, metadata, internal_metadata) "
                "SELECT public_id || '_' || :tgt, :tgt, peer_name, workspace_name, content, "
                "       token_count, metadata, internal_metadata "
                f"FROM messages WHERE session_name = :src AND workspace_name = :w {message_filter}"
            ),
            bind,
        )
        await s.execute(
            text(
                "INSERT INTO session_peers "
                "(session_name, peer_name, workspace_name, configuration, joined_at, left_at) "
                "SELECT :tgt, peer_name, workspace_name, configuration, NOW(), NULL "
                "FROM session_peers WHERE session_name = :src AND workspace_name = :w"
            ),
            {"src": session_name, "w": workspace_name, "tgt": body.target_name},
        )
        await s.commit()

    return _row_to_session(new_row)


@router.post("/{session_name}/peers", response_model=list[schemas.SessionPeerMembership])
@trace
async def add_session_peers(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    body: Annotated[schemas.SessionPeersBody, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> list[schemas.SessionPeerMembership]:
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)

    async with session() as s:
        await _assert_session_exists(s, workspace_name, session_name)
        await _upsert_session_peers(s, workspace_name, session_name, body.peers)
        rows = await _select_active_memberships(s, workspace_name, session_name)
        await s.commit()

    return [schemas.SessionPeerMembership.model_validate(dict(r)) for r in rows]


@router.put("/{session_name}/peers", response_model=list[schemas.SessionPeerMembership])
@trace
async def set_session_peers(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    body: Annotated[schemas.SessionPeersBody, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> list[schemas.SessionPeerMembership]:
    """Replace the active peer set in a session. Existing memberships are left_at-stamped."""
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)

    async with session() as s:
        await _assert_session_exists(s, workspace_name, session_name)
        await s.execute(
            text(
                "UPDATE session_peers SET left_at = NOW() "
                "WHERE session_name = :n AND workspace_name = :w AND left_at IS NULL"
            ),
            {"n": session_name, "w": workspace_name},
        )
        await _upsert_session_peers(s, workspace_name, session_name, body.peers)
        rows = await _select_active_memberships(s, workspace_name, session_name)
        await s.commit()

    return [schemas.SessionPeerMembership.model_validate(dict(r)) for r in rows]


@router.delete(
    "/{session_name}/peers",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    response_model=None,
)
@trace
async def remove_session_peers(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    body: Annotated[schemas.SessionPeersRemove, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> Response:
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)

    async with session() as s:
        await _assert_session_exists(s, workspace_name, session_name)
        await s.execute(
            text(
                "UPDATE session_peers SET left_at = NOW() "
                "WHERE session_name = :n AND workspace_name = :w "
                "AND peer_name = ANY(:peers) AND left_at IS NULL"
            ),
            {"n": session_name, "w": workspace_name, "peers": body.peers},
        )
        await s.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/{session_name}/context", response_model=schemas.SessionContext)
@trace
async def get_session_context(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
    tokens: Annotated[int, Query(ge=1, le=128_000)] = DEFAULT_CONTEXT_TOKENS,
    include_summary: Annotated[bool, Query(alias="summary")] = True,
) -> schemas.SessionContext:
    """Prompt-ready window: latest messages (oldest-first) + optional rolling summary."""
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)

    summary: schemas.SessionSummary | None = None
    if include_summary:
        summary = await _read_short_summary(workspace_name, session_name)

    summary_tokens = summary.token_count if summary else 0
    remaining = max(tokens - summary_tokens, 0)

    async with session() as s:
        await _assert_session_exists(s, workspace_name, session_name)
        rows = (
            (
                await s.execute(
                    text(
                        "SELECT public_id, session_name, peer_name, workspace_name, content, "
                        "token_count, metadata, created_at "
                        "FROM messages "
                        "WHERE workspace_name = :w AND session_name = :n "
                        "ORDER BY id DESC"
                    ),
                    {"w": workspace_name, "n": session_name},
                )
            )
            .mappings()
            .all()
        )

    selected: list[schemas.Message] = []
    used = 0
    for r in rows:
        cost = max(int(r["token_count"]), 1)
        if used + cost > remaining and selected:
            break
        selected.append(schemas.Message.model_validate(dict(r)))
        used += cost
    selected.reverse()

    return schemas.SessionContext(name=session_name, messages=selected, summary=summary)


@router.get("/{session_name}/summaries", response_model=schemas.SessionSummaries)
@trace
async def get_session_summaries(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> schemas.SessionSummaries:
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)

    async with session() as s:
        rows = (
            (
                await s.execute(
                    text(
                        "SELECT internal_metadata FROM sessions "
                        "WHERE workspace_name = :w AND name = :n"
                    ),
                    {"w": workspace_name, "n": session_name},
                )
            )
            .mappings()
            .first()
        )
    if rows is None:
        raise not_found(f"session {session_name!r} not found")

    meta = dict(rows["internal_metadata"] or {})
    return schemas.SessionSummaries(
        name=session_name,
        short=_summary_from_meta(meta.get("short_summary")),
        long=_summary_from_meta(meta.get("long_summary")),
    )


@router.post("/{session_name}/search", response_model=list[schemas.MessageHit])
@trace
async def search_session_messages(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    body: Annotated[schemas.MessageSearch, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> list[schemas.MessageHit]:
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)

    hits = await vector_search.query_messages(
        query_text=body.query,
        workspace_name=workspace_name,
        peer_name=body.peer_name,
        session_name=session_name,
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

_SESSION_COLUMNS = (
    "name, workspace_name, metadata, configuration, "
    "(deleted_at IS NULL) AS is_active, created_at, internal_metadata"
)
_SELECT_SESSION = f"SELECT {_SESSION_COLUMNS} FROM sessions WHERE name = :n AND workspace_name = :w"


def _row_to_session(row) -> schemas.Session:
    d = dict(row)
    d.pop("internal_metadata", None)
    return schemas.Session.model_validate(d)


async def _ensure_workspace(name: str) -> None:
    async with session() as s:
        row = (
            await s.execute(text("SELECT 1 FROM workspaces WHERE name = :n"), {"n": name})
        ).first()
    if row is None:
        raise not_found(f"workspace {name!r} not found")


async def _assert_session_exists(s, workspace_name: str, session_name: str) -> None:
    row = (
        await s.execute(
            text("SELECT 1 FROM sessions WHERE name = :n AND workspace_name = :w"),
            {"n": session_name, "w": workspace_name},
        )
    ).first()
    if row is None:
        raise not_found(f"session {session_name!r} not found")


async def _read_session_or_404(workspace_name: str, session_name: str) -> schemas.Session:
    async with session() as s:
        row = (
            (await s.execute(text(_SELECT_SESSION), {"n": session_name, "w": workspace_name}))
            .mappings()
            .first()
        )
    if row is None:
        raise not_found(f"session {session_name!r} not found")
    return _row_to_session(row)


async def _upsert_session_peers(
    s, workspace_name: str, session_name: str, peers: dict[str, schemas.SessionPeerConfig]
) -> None:
    if not peers:
        return
    for peer_name, cfg in peers.items():
        try:
            schemas.assert_name(peer_name, kind="peer")
        except ValueError as e:
            raise bad_request(str(e)) from e
        # Auto-create the peer row if it doesn't exist (honcho semantics).
        await s.execute(
            text(
                "INSERT INTO peers (name, workspace_name) VALUES (:p, :w) "
                "ON CONFLICT (name, workspace_name) DO NOTHING"
            ),
            {"p": peer_name, "w": workspace_name},
        )
        await s.execute(
            text(
                "INSERT INTO session_peers "
                "(session_name, peer_name, workspace_name, configuration) "
                "VALUES (:s, :p, :w, :c) "
                "ON CONFLICT (session_name, peer_name, workspace_name) DO UPDATE "
                "SET configuration = EXCLUDED.configuration, left_at = NULL"
            ),
            {
                "s": session_name,
                "p": peer_name,
                "w": workspace_name,
                "c": json.dumps(cfg.model_dump()),
            },
        )


async def _select_active_memberships(s, workspace_name: str, session_name: str) -> list:
    return (
        (
            await s.execute(
                text(
                    "SELECT session_name, peer_name, workspace_name, configuration, "
                    "joined_at, left_at FROM session_peers "
                    "WHERE session_name = :n AND workspace_name = :w AND left_at IS NULL "
                    "ORDER BY joined_at ASC"
                ),
                {"n": session_name, "w": workspace_name},
            )
        )
        .mappings()
        .all()
    )


async def _read_short_summary(
    workspace_name: str, session_name: str
) -> schemas.SessionSummary | None:
    """Try a real summarizer record first; fall back to internal_metadata.short_summary."""
    async with session() as s:
        row = (
            (
                await s.execute(
                    text(
                        "SELECT internal_metadata FROM sessions "
                        "WHERE workspace_name = :w AND name = :n"
                    ),
                    {"w": workspace_name, "n": session_name},
                )
            )
            .mappings()
            .first()
        )
    if row is None:
        return None
    return _summary_from_meta(dict(row["internal_metadata"] or {}).get("short_summary"))


def _summary_from_meta(value) -> schemas.SessionSummary | None:
    if not isinstance(value, dict):
        return None
    try:
        return schemas.SessionSummary.model_validate(value)
    except Exception:
        return None


def _filter_clause(workspace_name: str, filters: dict | None) -> tuple[str, dict]:
    bind: dict = {"w": workspace_name}
    where = "WHERE workspace_name = :w"
    if filters:
        where += " AND metadata @> :f::jsonb"
        bind["f"] = json.dumps(filters)
    return where, bind


def _update_sets(metadata: dict | None, configuration: dict | None) -> tuple[list[str], dict]:
    sets: list[str] = []
    bind: dict = {}
    if metadata is not None:
        sets.append("metadata = :m")
        bind["m"] = json.dumps(metadata)
    if configuration is not None:
        sets.append("configuration = :c")
        bind["c"] = json.dumps(configuration)
    return sets, bind
