"""Derive (observer, observed) pairs for a batch of messages.

Honcho's rule (from `src/deriver/enqueue.py::generate_queue_records`):

* The *observed* peer is always `message.peer_name` — the sender.
* The *observers* are:
    1. The sender themselves (self-observation), unless the sender's
       configuration disables `observe_me`.
    2. Every other peer currently in the same session, unless that peer's
       session-level configuration has `observe_others = False` or they have
       left the session.

For v1 we mirror the *default* behaviour: every active session peer (sender
included) becomes an observer. Per-peer `observe_me` / `observe_others`
overrides will land when the routers expose those configuration fields.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class MessageRow:
    id: int
    public_id: str
    workspace_name: str
    session_name: str
    peer_name: str
    content: str
    created_at: object  # datetime — kept loose so Spark Row works directly

    @classmethod
    def from_row(cls, row: Mapping[str, object]) -> "MessageRow":
        return cls(
            id=int(row["id"]),  # type: ignore[arg-type]
            public_id=str(row["public_id"]),
            workspace_name=str(row["workspace_name"]),
            session_name=str(row["session_name"]),
            peer_name=str(row["peer_name"]),
            content=str(row["content"]),
            created_at=row["created_at"],
        )


@dataclass
class DeriverBatch:
    """All messages from one (workspace, session, observed) plus the resolved observers."""

    workspace_name: str
    session_name: str
    observed: str
    observers: list[str]
    messages: list[MessageRow] = field(default_factory=list)


def build_batches(
    messages: Iterable[MessageRow],
    session_peers: Mapping[tuple[str, str], list[str]],
) -> list[DeriverBatch]:
    """Group messages by (workspace, session, observed) and resolve observers.

    `session_peers` maps `(workspace_name, session_name) -> [active peer names]`.
    A session that has no recorded peers (e.g. the message arrived before
    `session_peers` was populated) falls back to a self-observation only —
    matches honcho's behaviour when `peers_with_configuration` is empty.
    """
    grouped: dict[tuple[str, str, str], DeriverBatch] = {}
    for msg in messages:
        key = (msg.workspace_name, msg.session_name, msg.peer_name)
        batch = grouped.get(key)
        if batch is None:
            observers = _resolve_observers(
                workspace_name=msg.workspace_name,
                session_name=msg.session_name,
                observed=msg.peer_name,
                session_peers=session_peers,
            )
            batch = DeriverBatch(
                workspace_name=msg.workspace_name,
                session_name=msg.session_name,
                observed=msg.peer_name,
                observers=observers,
            )
            grouped[key] = batch
        batch.messages.append(msg)

    for batch in grouped.values():
        batch.messages.sort(key=lambda m: m.id)

    return list(grouped.values())


def _resolve_observers(
    *,
    workspace_name: str,
    session_name: str,
    observed: str,
    session_peers: Mapping[tuple[str, str], list[str]],
) -> list[str]:
    peers = session_peers.get((workspace_name, session_name), [])
    if not peers:
        return [observed]
    seen: set[str] = set()
    observers: list[str] = []
    for name in [observed, *peers]:
        if name in seen:
            continue
        seen.add(name)
        observers.append(name)
    return observers
