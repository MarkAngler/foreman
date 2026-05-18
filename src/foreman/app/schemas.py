"""Request/response schemas for foreman's REST API.

Semantics ported from honcho's src/schemas/api.py (workspaces / peers / sessions /
messages / conclusions / keys / webhooks). API surface is a clean redesign — names
are used directly (no `id` aliasing), `extra='forbid'` on every request body, and
list endpoints take an explicit `filters: dict | None` body.
"""

from __future__ import annotations

import datetime
import ipaddress
import re
from typing import Annotated, Any, Literal
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator

RESOURCE_NAME_PATTERN = r"^[A-Za-z0-9_\-]+$"
NAME_MAX = 256

_REQUEST = ConfigDict(extra="forbid")
_RESPONSE = ConfigDict(from_attributes=True)


# ---------------------------------------------------------------------------
# Reusable name + metadata types
# ---------------------------------------------------------------------------

ResourceName = Annotated[
    str,
    Field(min_length=1, max_length=NAME_MAX, pattern=RESOURCE_NAME_PATTERN),
]
Metadata = dict[str, Any]
Configuration = dict[str, Any]
Filters = dict[str, Any]

REASONING_LEVELS = ("minimal", "low", "medium", "high", "max")
ReasoningLevel = Literal["minimal", "low", "medium", "high", "max"]
DOCUMENT_LEVELS = ("explicit", "deductive", "inductive", "contradiction")
DocumentLevel = Literal["explicit", "deductive", "inductive", "contradiction"]


def _strip_nul(v: str) -> str:
    return v.replace("\x00", "")


# ---------------------------------------------------------------------------
# Workspace
# ---------------------------------------------------------------------------


class WorkspaceCreate(BaseModel):
    model_config = _REQUEST
    name: ResourceName
    metadata: Metadata = Field(default_factory=dict)
    configuration: Configuration = Field(default_factory=dict)


class WorkspaceUpdate(BaseModel):
    model_config = _REQUEST
    metadata: Metadata | None = None
    configuration: Configuration | None = None


class WorkspaceList(BaseModel):
    model_config = _REQUEST
    filters: Filters | None = None


class Workspace(BaseModel):
    model_config = _RESPONSE
    name: str
    metadata: Metadata = Field(default_factory=dict)
    configuration: Configuration = Field(default_factory=dict)
    created_at: datetime.datetime


class MessageSearch(BaseModel):
    """Hybrid search over messages, scoped by the path workspace."""

    model_config = _REQUEST
    query: Annotated[str, Field(min_length=1, max_length=10_000)]
    peer_name: str | None = None
    session_name: str | None = None
    limit: int = Field(default=10, ge=1, le=100)

    @field_validator("query", mode="after")
    @classmethod
    def _strip(cls, v: str) -> str:
        return _strip_nul(v)


class MessageHit(BaseModel):
    public_id: str
    workspace_name: str
    peer_name: str
    session_name: str
    score: float


# ---------------------------------------------------------------------------
# Peer
# ---------------------------------------------------------------------------


class PeerCreate(BaseModel):
    model_config = _REQUEST
    name: ResourceName
    metadata: Metadata = Field(default_factory=dict)
    configuration: Configuration = Field(default_factory=dict)


class PeerUpdate(BaseModel):
    model_config = _REQUEST
    metadata: Metadata | None = None
    configuration: Configuration | None = None


class PeerList(BaseModel):
    model_config = _REQUEST
    filters: Filters | None = None


class Peer(BaseModel):
    model_config = _RESPONSE
    name: str
    workspace_name: str
    metadata: Metadata = Field(default_factory=dict)
    configuration: Configuration = Field(default_factory=dict)
    created_at: datetime.datetime


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


class SessionPeerConfig(BaseModel):
    model_config = _REQUEST
    observe_others: bool = True
    observe_me: bool = True


class SessionCreate(BaseModel):
    model_config = _REQUEST
    name: ResourceName
    metadata: Metadata = Field(default_factory=dict)
    configuration: Configuration = Field(default_factory=dict)
    peers: dict[str, SessionPeerConfig] | None = None


class SessionUpdate(BaseModel):
    model_config = _REQUEST
    metadata: Metadata | None = None
    configuration: Configuration | None = None


class SessionList(BaseModel):
    model_config = _REQUEST
    filters: Filters | None = None


class Session(BaseModel):
    model_config = _RESPONSE
    name: str
    workspace_name: str
    metadata: Metadata = Field(default_factory=dict)
    configuration: Configuration = Field(default_factory=dict)
    is_active: bool = True
    created_at: datetime.datetime


class SessionPeerMembership(BaseModel):
    model_config = _RESPONSE
    peer_name: str
    session_name: str
    workspace_name: str
    configuration: Configuration = Field(default_factory=dict)
    joined_at: datetime.datetime
    left_at: datetime.datetime | None = None


class SessionPeersBody(BaseModel):
    model_config = _REQUEST
    peers: dict[str, SessionPeerConfig]


class SessionPeersRemove(BaseModel):
    model_config = _REQUEST
    peers: list[str] = Field(..., min_length=1)


class SessionClone(BaseModel):
    model_config = _REQUEST
    target_name: ResourceName
    cutoff_message_id: str | None = None


class SessionSummary(BaseModel):
    content: str
    message_public_id: str
    summary_type: Literal["short", "long"]
    token_count: int
    created_at: datetime.datetime


class SessionSummaries(BaseModel):
    name: str
    short: SessionSummary | None = None
    long: SessionSummary | None = None


# ---------------------------------------------------------------------------
# Message
# ---------------------------------------------------------------------------

MAX_MESSAGE_CONTENT = 65_535
MAX_FILE_BYTES = 25 * 1024 * 1024
MESSAGE_CHUNK_CHARS = 8_000


class MessageCreate(BaseModel):
    model_config = _REQUEST
    peer_name: ResourceName
    content: Annotated[str, Field(min_length=1, max_length=MAX_MESSAGE_CONTENT)]
    metadata: Metadata = Field(default_factory=dict)
    token_count: int | None = None

    @field_validator("content", mode="after")
    @classmethod
    def _strip(cls, v: str) -> str:
        return _strip_nul(v)


class MessageBatchCreate(BaseModel):
    model_config = _REQUEST
    messages: list[MessageCreate] = Field(..., min_length=1, max_length=100)


class MessageUpdate(BaseModel):
    model_config = _REQUEST
    metadata: Metadata


class MessageList(BaseModel):
    model_config = _REQUEST
    filters: Filters | None = None


class Message(BaseModel):
    model_config = _RESPONSE
    public_id: str
    session_name: str
    peer_name: str
    workspace_name: str
    content: str
    token_count: int = 0
    metadata: Metadata = Field(default_factory=dict)
    created_at: datetime.datetime


class SessionContext(BaseModel):
    name: str
    messages: list[Message]
    summary: SessionSummary | None = None


# ---------------------------------------------------------------------------
# Conclusions (collections / documents API)
# ---------------------------------------------------------------------------


class ConclusionCreate(BaseModel):
    model_config = _REQUEST
    observer: ResourceName
    observed: ResourceName
    content: Annotated[str, Field(min_length=1, max_length=MAX_MESSAGE_CONTENT)]
    level: DocumentLevel = "explicit"
    source_ids: list[str] = Field(default_factory=list)
    metadata: Metadata = Field(default_factory=dict)

    @field_validator("content", mode="after")
    @classmethod
    def _strip(cls, v: str) -> str:
        return _strip_nul(v)


class ConclusionBatchCreate(BaseModel):
    model_config = _REQUEST
    conclusions: list[ConclusionCreate] = Field(..., min_length=1, max_length=100)


class ConclusionList(BaseModel):
    model_config = _REQUEST
    filters: Filters | None = None


class ConclusionQuery(BaseModel):
    model_config = _REQUEST
    query: Annotated[str, Field(min_length=1, max_length=10_000)]
    observer: str | None = None
    observed: str | None = None
    level: DocumentLevel | None = None
    top_k: int = Field(default=10, ge=1, le=100)


class Conclusion(BaseModel):
    model_config = _RESPONSE
    id: str
    workspace_name: str
    observer: str
    observed: str
    level: str
    content: str
    source_ids: list[str] = Field(default_factory=list)
    times_derived: int = 0
    metadata: Metadata = Field(default_factory=dict)
    created_at: datetime.datetime


# ---------------------------------------------------------------------------
# Keys (JWT issuance)
# ---------------------------------------------------------------------------


class KeyIssue(BaseModel):
    """Admin-only request to mint a scoped JWT.

    Exactly one scope of (workspace, workspace+peer, workspace+session) must be
    set, OR admin=true. Validation rules are enforced by foreman.lib.auth.issue.
    """

    model_config = _REQUEST
    admin: bool = False
    workspace_name: str | None = None
    peer_name: str | None = None
    session_name: str | None = None
    ttl_seconds: int = Field(default=60 * 60 * 24 * 30, ge=60, le=60 * 60 * 24 * 365)


class IssuedKey(BaseModel):
    token: str
    expires_at: datetime.datetime


# ---------------------------------------------------------------------------
# Webhooks
# ---------------------------------------------------------------------------

_PRIVATE_HOSTS = ("localhost", "127.0.0.1", "0.0.0.0", "::1")


def _validate_public_url(v: str) -> str:
    parsed = urlparse(v)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("only http(s) urls are allowed")
    if not parsed.netloc:
        raise ValueError("url must include a host")
    host = (parsed.hostname or "").lower()
    if host in _PRIVATE_HOSTS:
        raise ValueError("loopback urls are not allowed")
    try:
        if ipaddress.ip_address(host).is_private:
            raise ValueError("private IP addresses are not allowed")
    except ValueError:
        # not an IP literal — accept the hostname
        pass
    return v


class WebhookEndpointCreate(BaseModel):
    model_config = _REQUEST
    url: str
    secret: str | None = None
    event_types: list[str] = Field(default_factory=list)
    metadata: Metadata = Field(default_factory=dict)

    @field_validator("url", mode="after")
    @classmethod
    def _check_url(cls, v: str) -> str:
        return _validate_public_url(v)


class WebhookEndpoint(BaseModel):
    model_config = _RESPONSE
    id: str
    workspace_name: str
    url: str
    event_types: list[str] = Field(default_factory=list)
    metadata: Metadata = Field(default_factory=dict)
    created_at: datetime.datetime


# ---------------------------------------------------------------------------
# Helper: validate path-resource names
# ---------------------------------------------------------------------------

_NAME_RE = re.compile(RESOURCE_NAME_PATTERN)


def assert_name(value: str, *, kind: str) -> None:
    """Raise ValueError if the path-supplied name violates the schema constraint.

    Use at the top of handlers — keeps the rejection close to the boundary so a
    malicious path component can't slip into a bound query.
    """
    if not value or len(value) > NAME_MAX or not _NAME_RE.fullmatch(value):
        raise ValueError(f"invalid {kind} name: {value!r}")
