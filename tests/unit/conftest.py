"""Shared fixtures for router unit tests.

We don't spin up a real Lakebase. Instead we replace `foreman.lib.lakebase.session`
with an asynccontextmanager that yields a `FakeSession` whose `execute()` returns
canned `FakeResult`s programmed by each test.
"""

from __future__ import annotations

import datetime
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from foreman.app.routers import all_routers
from foreman.lib import auth


@dataclass
class FakeResult:
    rows: list[dict] = field(default_factory=list)

    def mappings(self) -> "FakeResult":
        return self

    def first(self) -> dict | None:
        return self.rows[0] if self.rows else None

    def one(self) -> dict:
        if not self.rows:
            raise LookupError("no rows")
        return self.rows[0]

    def all(self) -> list[dict]:
        return self.rows


@dataclass
class FakeSession:
    """Stand-in for AsyncSession.

    queue: a list of (predicate, result) pairs. The predicate is a callable
    `(sql: str, params: dict) -> bool`. First match wins AND is consumed —
    repeated identical statements pick up the next matching entry. If no
    predicate matches, returns FakeResult([]).
    """

    queue: list[tuple] = field(default_factory=list)
    executed: list[tuple[str, dict]] = field(default_factory=list)
    committed: int = 0
    rolled_back: int = 0

    async def execute(self, statement, params: dict | None = None) -> FakeResult:
        sql = str(statement)
        params = params or {}
        self.executed.append((sql, params))
        for i, (pred, result) in enumerate(self.queue):
            if pred(sql, params):
                self.queue.pop(i)
                if isinstance(result, Exception):
                    raise result
                return result
        return FakeResult([])

    async def commit(self) -> None:
        self.committed += 1

    async def rollback(self) -> None:
        self.rolled_back += 1


def queue_match(*needles: str):
    """Build a predicate that matches if every needle appears in the SQL."""

    def _pred(sql: str, params: dict) -> bool:
        return all(n in sql for n in needles)

    return _pred


@pytest.fixture
def fake_session() -> FakeSession:
    return FakeSession()


@pytest.fixture
def patch_session(fake_session):
    """Replace lakebase.session() with one that yields the shared FakeSession."""

    @asynccontextmanager
    async def _ctx():
        yield fake_session

    with patch("foreman.lib.lakebase.session", _ctx):
        # patches in router modules too — they imported `session` by-name.
        with patch("foreman.app.routers.workspaces.session", _ctx), patch(
            "foreman.app.routers.peers.session", _ctx
        ), patch("foreman.app.routers.sessions.session", _ctx), patch(
            "foreman.app.routers.messages.session", _ctx
        ), patch(
            "foreman.app.routers.conclusions.session", _ctx
        ), patch(
            "foreman.app.routers.webhooks.session", _ctx
        ):
            yield fake_session


@pytest.fixture(autouse=True)
def stub_signing_key():
    auth.signing_key.cache_clear()
    with patch.object(auth, "signing_key", return_value="unit-test-router-key"):
        yield


@pytest.fixture
def admin_token() -> str:
    return auth.issue(admin=True)


@pytest.fixture
def workspace_token() -> str:
    return auth.issue(workspace="ws-a")


@pytest.fixture
def peer_token() -> str:
    return auth.issue(workspace="ws-a", peer="peer-1")


@pytest.fixture
def session_token() -> str:
    return auth.issue(workspace="ws-a", session="sess-1")


@pytest.fixture
def app() -> FastAPI:
    """A FastAPI app with all routers mounted, mirroring what Phase-3 will do."""
    a = FastAPI()
    for r in all_routers():
        a.include_router(r)
    return a


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


def auth_header(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def now() -> datetime.datetime:
    return datetime.datetime(2026, 5, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)


def workspace_row(name: str = "ws-a", **overrides: Any) -> dict:
    base = {
        "name": name,
        "metadata": {},
        "configuration": {},
        "created_at": now(),
    }
    base.update(overrides)
    return base


def peer_row(name: str = "peer-1", workspace: str = "ws-a", **overrides: Any) -> dict:
    base = {
        "name": name,
        "workspace_name": workspace,
        "metadata": {},
        "configuration": {},
        "created_at": now(),
    }
    base.update(overrides)
    return base


def session_row(name: str = "sess-1", workspace: str = "ws-a", **overrides: Any) -> dict:
    base = {
        "name": name,
        "workspace_name": workspace,
        "metadata": {},
        "configuration": {},
        "is_active": True,
        "created_at": now(),
        "internal_metadata": {},
    }
    base.update(overrides)
    return base


def message_row(public_id: str = "msg-1", **overrides: Any) -> dict:
    base = {
        "public_id": public_id,
        "session_name": "sess-1",
        "peer_name": "peer-1",
        "workspace_name": "ws-a",
        "content": "hello",
        "token_count": 1,
        "metadata": {},
        "created_at": now(),
    }
    base.update(overrides)
    return base


def conclusion_row(doc_id: str = "doc-1", **overrides: Any) -> dict:
    base = {
        "id": doc_id,
        "workspace_name": "ws-a",
        "observer": "peer-1",
        "observed": "peer-2",
        "level": "explicit",
        "content": "an observation",
        "source_ids": [],
        "times_derived": 0,
        "metadata": {},
        "created_at": now(),
    }
    base.update(overrides)
    return base


def webhook_row(eid: str = "wh-1", **overrides: Any) -> dict:
    base = {
        "id": eid,
        "workspace_name": "ws-a",
        "url": "https://example.com/hook",
        "event_types": [],
        "metadata": {},
        "created_at": now(),
    }
    base.update(overrides)
    return base
