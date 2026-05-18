"""Message batch-create / upload / list / read / update.

POST creates messages and writes embeddings to messages_idx INLINE before
returning, so /search results are fresh on the next request (the architecture
constraint that drove the Direct Access index choice).
"""

from __future__ import annotations

import json
from typing import Annotated

from fastapi import APIRouter, Body, Depends, File, Form, Path, UploadFile, status
from fastapi_pagination import Page, Params, paginate
from nanoid import generate as nanoid
from sqlalchemy import text

from foreman.app import schemas
from foreman.app.routers._common import (
    assert_path_name,
    bad_request,
    not_found,
)
from foreman.lib import auth, vector_search
from foreman.lib.lakebase import session
from foreman.lib.tracing import trace

router = APIRouter(
    prefix="/workspaces/{workspace_name}/sessions/{session_name}/messages",
    tags=["messages"],
)

PUBLIC_ID_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
PUBLIC_ID_LEN = 21
_MESSAGE_COLUMNS = (
    "public_id, session_name, peer_name, workspace_name, content, "
    "token_count, metadata, created_at"
)


@router.post("", response_model=list[schemas.Message], status_code=status.HTTP_201_CREATED)
@trace
async def create_messages(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    body: Annotated[schemas.MessageBatchCreate, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> list[schemas.Message]:
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)

    inserted = await _insert_messages(workspace_name, session_name, body.messages)
    await _index_messages(inserted)
    return [schemas.Message.model_validate(dict(r)) for r in inserted]


@router.post("/upload", response_model=list[schemas.Message], status_code=status.HTTP_201_CREATED)
@trace
async def upload_messages_from_file(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
    peer_name: Annotated[str, Form()],
    file: Annotated[UploadFile, File()],
    metadata: Annotated[str | None, Form()] = None,
) -> list[schemas.Message]:
    """Read a UTF-8 text file and split it into MessageCreate chunks (8000 chars each)."""
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    assert_path_name(peer_name, kind="peer")
    auth.require_session(workspace_name, session_name, scope)

    contents = await file.read()
    if not contents:
        raise bad_request("uploaded file is empty")
    if len(contents) > schemas.MAX_FILE_BYTES:
        raise bad_request(
            f"file size {len(contents)} exceeds limit {schemas.MAX_FILE_BYTES}"
        )

    try:
        text_body = contents.decode("utf-8")
    except UnicodeDecodeError as e:
        raise bad_request(f"file must be utf-8 encoded: {e}") from e

    parsed_meta: dict = {}
    if metadata:
        try:
            parsed_meta = json.loads(metadata)
        except json.JSONDecodeError as e:
            raise bad_request(f"metadata must be valid json: {e}") from e
        if not isinstance(parsed_meta, dict):
            raise bad_request("metadata must decode to an object")

    chunks = [
        text_body[i : i + schemas.MESSAGE_CHUNK_CHARS]
        for i in range(0, len(text_body), schemas.MESSAGE_CHUNK_CHARS)
    ]
    creates = [
        schemas.MessageCreate(peer_name=peer_name, content=chunk, metadata=parsed_meta)
        for chunk in chunks
    ]
    if not creates:
        raise bad_request("no message chunks produced from file")
    if len(creates) > 100:
        raise bad_request(
            f"file produced {len(creates)} chunks; max 100 per upload — split file"
        )

    inserted = await _insert_messages(workspace_name, session_name, creates)
    await _index_messages(inserted)
    return [schemas.Message.model_validate(dict(r)) for r in inserted]


@router.post("/list", response_model=Page[schemas.Message])
@trace
async def list_messages(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
    params: Annotated[Params, Depends()],
    body: Annotated[schemas.MessageList | None, Body()] = None,
) -> Page[schemas.Message]:
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)

    bind: dict = {"w": workspace_name, "n": session_name}
    where = "WHERE workspace_name = :w AND session_name = :n"
    if body and body.filters:
        where += " AND metadata @> :f::jsonb"
        bind["f"] = json.dumps(body.filters)

    async with session() as s:
        rows = (
            await s.execute(
                text(
                    f"SELECT {_MESSAGE_COLUMNS} FROM messages {where} "
                    "ORDER BY id DESC"
                ),
                bind,
            )
        ).mappings().all()

    items = [schemas.Message.model_validate(dict(r)) for r in rows]
    return paginate(items, params)


@router.get("/{message_id}", response_model=schemas.Message)
@trace
async def get_message(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    message_id: Annotated[str, Path()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> schemas.Message:
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)

    async with session() as s:
        row = (
            await s.execute(
                text(
                    f"SELECT {_MESSAGE_COLUMNS} FROM messages "
                    "WHERE workspace_name = :w AND session_name = :n AND public_id = :pid"
                ),
                {"w": workspace_name, "n": session_name, "pid": message_id},
            )
        ).mappings().first()
    if row is None:
        raise not_found(f"message {message_id!r} not found")
    return schemas.Message.model_validate(dict(row))


@router.put("/{message_id}", response_model=schemas.Message)
@trace
async def update_message(
    workspace_name: Annotated[str, Path()],
    session_name: Annotated[str, Path()],
    message_id: Annotated[str, Path()],
    body: Annotated[schemas.MessageUpdate, Body()],
    scope: Annotated[auth.Scope, Depends(auth.current_scope)],
) -> schemas.Message:
    assert_path_name(workspace_name, kind="workspace")
    assert_path_name(session_name, kind="session")
    auth.require_session(workspace_name, session_name, scope)

    async with session() as s:
        row = (
            await s.execute(
                text(
                    "UPDATE messages SET metadata = :m "
                    "WHERE workspace_name = :w AND session_name = :n AND public_id = :pid "
                    f"RETURNING {_MESSAGE_COLUMNS}"
                ),
                {
                    "m": json.dumps(body.metadata),
                    "w": workspace_name,
                    "n": session_name,
                    "pid": message_id,
                },
            )
        ).mappings().first()
        if row is None:
            raise not_found(f"message {message_id!r} not found")
        await s.commit()

    return schemas.Message.model_validate(dict(row))


# ---- internals --------------------------------------------------------------


async def _insert_messages(
    workspace_name: str, session_name: str, creates: list[schemas.MessageCreate]
) -> list[dict]:
    """Insert into Lakebase and return the freshly-inserted rows as dicts."""
    bind_rows = []
    for c in creates:
        bind_rows.append(
            {
                "public_id": nanoid(PUBLIC_ID_ALPHABET, PUBLIC_ID_LEN),
                "session_name": session_name,
                "peer_name": c.peer_name,
                "workspace_name": workspace_name,
                "content": c.content,
                "token_count": c.token_count or _approx_tokens(c.content),
                "metadata": json.dumps(c.metadata),
            }
        )

    inserted: list[dict] = []
    async with session() as s:
        for r in bind_rows:
            row = (
                await s.execute(
                    text(
                        "INSERT INTO messages "
                        "(public_id, session_name, peer_name, workspace_name, "
                        " content, token_count, metadata) "
                        "VALUES (:public_id, :session_name, :peer_name, :workspace_name, "
                        "        :content, :token_count, :metadata) "
                        f"RETURNING {_MESSAGE_COLUMNS}"
                    ),
                    r,
                )
            ).mappings().one()
            inserted.append(dict(row))
        await s.commit()
    return inserted


@trace
async def _index_messages(rows: list[dict]) -> None:
    """Compute embeddings + upsert each row to messages_idx. Sequential by design.

    Honcho fanout is per-message: the deriver reads CDF later, but the inline
    index has to be queryable on the next request.
    """
    for r in rows:
        vec = await vector_search.embed(r["content"])
        await vector_search.upsert_message(
            public_id=r["public_id"],
            workspace_name=r["workspace_name"],
            peer_name=r["peer_name"],
            session_name=r["session_name"],
            content=r["content"],
            embedding=vec,
            created_at=r["created_at"].isoformat(),
        )


def _approx_tokens(content: str) -> int:
    """Cheap token estimate — 4 chars per token. Real count comes from tiktoken
    in the deriver job; the API-time number is only used for context budgeting."""
    return max(1, len(content) // 4)
