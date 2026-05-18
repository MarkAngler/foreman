"""Phase-1 spine acceptance test.

Runs against the deployed bundle. Validates the contracts that all Phase-2
agents will depend on.

Usage:
    databricks auth login --profile DEFAULT
    python -m pytest tests/spine_smoke.py -s
"""

from __future__ import annotations

import asyncio
import time
import uuid

import pytest
from sqlalchemy import text

from foreman.lib import auth, llm_dispatch, vector_search
from foreman.lib.config import settings
from foreman.lib.lakebase import dispose_engine, init_engine, session


EXPECTED_TABLES = {
    "workspaces",
    "peers",
    "sessions",
    "session_peers",
    "messages",
    "documents",
    "queue_items",
    "active_queue_session",
    "webhook_endpoints",
    "peer_cards",
    "workspace_llm_config",
    "documents_mirror",
}


@pytest.fixture(scope="session")
async def _engine():
    await init_engine()
    yield
    await dispose_engine()


async def test_lakebase_connection_and_schema(_engine):
    """Schema_init must have created all 11 honcho tables + the new ones."""
    async with session() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public'"
                )
            )
        ).all()
    present = {r[0] for r in rows}
    missing = EXPECTED_TABLES - present
    assert not missing, f"missing tables: {missing}"


async def test_lakebase_transaction_round_trip(_engine):
    """A round-trip insert/select inside a transaction proves the OAuth pool works."""
    workspace_name = f"smoke-{uuid.uuid4().hex[:8]}"
    workspace_id = uuid.uuid4().hex
    async with session() as s:
        await s.execute(
            text("INSERT INTO workspaces (id, name) VALUES (:id, :name)"),
            {"id": workspace_id, "name": workspace_name},
        )
        await s.commit()

        row = (
            await s.execute(
                text("SELECT name FROM workspaces WHERE id = :id"),
                {"id": workspace_id},
            )
        ).first()
        assert row is not None and row[0] == workspace_name

        await s.execute(text("DELETE FROM workspaces WHERE id = :id"), {"id": workspace_id})
        await s.commit()


async def test_messages_index_direct_access_round_trip():
    """Write a message vector, immediately query it back. Validates Direct Access freshness."""
    s = settings()
    public_id = f"smoke-msg-{uuid.uuid4().hex[:10]}"
    embedding = [0.0] * s.embedding_dim
    embedding[0] = 1.0  # one-hot so the search is deterministic

    await vector_search.upsert_message(
        public_id=public_id,
        workspace_name="smoke-ws",
        peer_name="smoke-peer",
        session_name="smoke-session",
        content="smoke test",
        embedding=embedding,
        created_at="2026-05-15T00:00:00Z",
    )

    # Direct Access is supposed to be near-immediate; allow a short retry.
    deadline = time.time() + 30
    hits: list = []
    while time.time() < deadline:
        hits = await vector_search.query_messages(
            query_text="smoke test",
            workspace_name="smoke-ws",
            num_results=5,
        )
        if any(h.public_id == public_id for h in hits):
            break
        await asyncio.sleep(2)

    assert any(h.public_id == public_id for h in hits), (
        f"upserted message not found in messages_idx within 30s. hits: {hits}"
    )

    await vector_search.delete_messages([public_id])


def test_llm_dispatch_default_endpoint_callable():
    """Sync chat call to the default deriver endpoint must succeed."""
    resp = llm_dispatch.chat_sync(
        workspace_name="smoke-ws",  # falls back to default since no row exists
        role="deriver",
        messages=[{"role": "user", "content": "Reply with the single word: ok"}],
        max_tokens=8,
    )
    text_out = _extract_text(resp)
    assert isinstance(text_out, str) and text_out.strip()


def test_jwt_round_trip():
    token = auth.issue(workspace="smoke-ws", peer="smoke-peer", ttl_seconds=120)
    scope = auth.decode(token)
    assert scope.workspace == "smoke-ws"
    assert scope.peer == "smoke-peer"
    assert scope.session is None
    assert scope.admin is False


def _extract_text(resp) -> str:
    """Best-effort extraction of generated text from a Model Serving response."""
    d = resp.as_dict() if hasattr(resp, "as_dict") else resp
    if isinstance(d, dict):
        choices = d.get("choices")
        if choices:
            msg = choices[0].get("message") or {}
            content = msg.get("content")
            if content:
                return content
    return str(resp)
