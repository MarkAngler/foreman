"""POST /workspaces/{workspace_name}/peers/{peer_name}/chat — SSE endpoint
that drives the dialectic agent and streams events to the client."""

from __future__ import annotations

import json
from typing import Annotated, AsyncIterator, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from foreman.app.dialectic import AgentEvent, run_agent_stream
from foreman.app.dialectic.agent import AgentRequest
from foreman.lib.auth import Scope, current_scope, require_peer

router = APIRouter(tags=["chat"])

ReasoningLevel = Literal["minimal", "low", "medium", "high", "max"]


class ChatRequest(BaseModel):
    query: str = Field(min_length=1, max_length=8192)
    session_name: str | None = None
    reasoning_level: ReasoningLevel = "low"
    include_session_history: bool = False
    session_history_max_tokens: int = Field(default=2000, ge=0, le=32000)


@router.post("/workspaces/{workspace_name}/peers/{peer_name}/chat")
async def chat(
    workspace_name: str,
    peer_name: str,
    body: ChatRequest,
    scope: Annotated[Scope, Depends(current_scope)],
) -> EventSourceResponse:
    require_peer(workspace_name, peer_name, scope)
    req = AgentRequest(
        workspace_name=workspace_name,
        peer_name=peer_name,
        query=body.query,
        session_name=body.session_name,
        reasoning_level=body.reasoning_level,
        include_session_history=body.include_session_history,
        session_history_max_tokens=body.session_history_max_tokens,
    )
    return EventSourceResponse(_stream(req))


async def _stream(req: AgentRequest) -> AsyncIterator[dict[str, str]]:
    """Translate AgentEvent objects into sse-starlette frame dicts."""
    async for event in run_agent_stream(req):
        yield _to_frame(event)


def _to_frame(event: AgentEvent) -> dict[str, str]:
    return {"event": event.kind, "data": json.dumps(event.data)}
