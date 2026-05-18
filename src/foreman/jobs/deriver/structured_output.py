"""Parse the deriver LLM response into Document rows.

Honcho's upstream wraps `honcho_llm_call(..., response_model=PromptRepresentation,
json_mode=True)` which guarantees a typed object back. Here the LLM call goes
through the generic `chat_sync` dispatcher, so we parse JSON ourselves and
validate it with the same Pydantic shape (`{"explicit": [{"content": "..."}]}`).

Document IDs are deterministic — `sha256(workspace|observer|observed|content)`
truncated to 24 hex chars — so re-running the same batch is idempotent under
the `MERGE INTO documents_delta ON id` semantics in `delta_write.upsert_documents`.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator


class ExplicitFact(BaseModel):
    content: str = Field(min_length=1)


class PromptRepresentation(BaseModel):
    """Mirrors honcho's `PromptRepresentation` — explicit observations only."""

    explicit: list[ExplicitFact] = Field(default_factory=list)

    @field_validator("explicit", mode="before")
    @classmethod
    def _none_to_empty(cls, v: Any) -> Any:
        return [] if v is None else v


@dataclass(frozen=True)
class DocumentDraft:
    id: str
    workspace_name: str
    observer: str
    observed: str
    level: str
    content: str
    source_ids: list[str]
    times_derived: int
    metadata: str
    created_at: datetime


def parse_response(raw: str) -> PromptRepresentation:
    """Parse an LLM response into a `PromptRepresentation`.

    Tolerates ```json fences and leading/trailing prose because not every
    foundation model honours strict-JSON instructions.
    """
    payload = _extract_json_object(raw)
    try:
        return PromptRepresentation.model_validate(payload)
    except ValidationError as e:
        raise ValueError(f"deriver LLM response failed schema validation: {e}") from e


def build_document_drafts(
    *,
    representation: PromptRepresentation,
    workspace_name: str,
    observed: str,
    observers: list[str],
    source_message_public_ids: list[str],
    session_name: str,
    created_at: datetime | None = None,
) -> list[DocumentDraft]:
    """Fan out one explicit fact across all observer collections."""
    ts = created_at or datetime.now(timezone.utc)
    metadata = json.dumps({"session_name": session_name})
    drafts: list[DocumentDraft] = []
    for fact in representation.explicit:
        for observer in observers:
            drafts.append(
                DocumentDraft(
                    id=document_id(workspace_name, observer, observed, fact.content),
                    workspace_name=workspace_name,
                    observer=observer,
                    observed=observed,
                    level="explicit",
                    content=fact.content,
                    source_ids=list(source_message_public_ids),
                    times_derived=1,
                    metadata=metadata,
                    created_at=ts,
                )
            )
    return drafts


def document_id(workspace_name: str, observer: str, observed: str, content: str) -> str:
    h = hashlib.sha256(
        "|".join((workspace_name, observer, observed, content)).encode("utf-8")
    )
    return h.hexdigest()[:24]


_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


def _extract_json_object(raw: str) -> dict[str, Any]:
    text = raw.strip()
    if not text:
        raise ValueError("deriver LLM returned empty response")

    # Strip a markdown fence if the model wrapped its JSON.
    fence = _FENCE_RE.search(text)
    if fence:
        text = fence.group(1).strip()

    # Some models prefix prose. Locate the first {...} balanced object.
    if not text.startswith("{"):
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError(f"deriver LLM response not JSON-shaped: {raw!r}")
        text = text[start : end + 1]

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"deriver LLM response is not valid JSON: {e}") from e
    if not isinstance(parsed, dict):
        raise ValueError("deriver LLM response top-level value must be an object")
    return parsed
