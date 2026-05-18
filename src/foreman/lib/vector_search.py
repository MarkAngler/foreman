"""Typed wrappers around Databricks Vector Search for foreman's two indexes.

- messages_idx (Direct Access): app writes embeddings inline at message-create.
- documents_idx (Delta Sync, managed embeddings): synced from documents_delta.

Both indexes are queried via the `databricks-vectorsearch` package (which works
against both Standard and Storage-Optimized endpoints).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    DirectAccessVectorIndexSpec,
    EmbeddingSourceColumn,
    EmbeddingVectorColumn,
    PipelineType,
    VectorIndexType,
)

from foreman.lib.config import settings


@dataclass(frozen=True)
class MessageHit:
    public_id: str
    workspace_name: str
    peer_name: str
    session_name: str
    content: str
    score: float


@dataclass(frozen=True)
class DocumentHit:
    id: str
    workspace_name: str
    observer: str
    observed: str
    level: str
    content: str
    score: float


def _client() -> WorkspaceClient:
    return WorkspaceClient()


# ---- Index lifecycle --------------------------------------------------------


def ensure_messages_index() -> None:
    """Create the messages Direct Access index if it does not exist.

    Schema includes filter columns (workspace_name, peer_name, session_name) and
    the embedding vector. Caller-supplied embeddings (we compute via FMAPI in
    the app and pass the vector at upsert time).
    """
    s = settings()
    w = _client()
    try:
        w.vector_search_indexes.get_index(index_name=s.messages_index)
        return
    except Exception:
        pass

    schema = {
        "public_id": "string",
        "workspace_name": "string",
        "peer_name": "string",
        "session_name": "string",
        "content": "string",
        "embedding": "array<float>",
        "created_at": "timestamp",
    }
    w.vector_search_indexes.create_index(
        name=s.messages_index,
        endpoint_name=s.vs_endpoint,
        primary_key="public_id",
        index_type=VectorIndexType.DIRECT_ACCESS,
        direct_access_index_spec=DirectAccessVectorIndexSpec(
            embedding_vector_columns=[
                EmbeddingVectorColumn(name="embedding", embedding_dimension=s.embedding_dim)
            ],
            schema_json=json.dumps(schema),
        ),
    )


def ensure_documents_index(source_table: str | None = None) -> None:
    """Create the documents Delta-Sync index over `documents_delta` if absent."""
    s = settings()
    w = _client()
    try:
        w.vector_search_indexes.get_index(index_name=s.documents_index)
        return
    except Exception:
        pass

    w.vector_search_indexes.create_index(
        name=s.documents_index,
        endpoint_name=s.vs_endpoint,
        primary_key="id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=source_table or s.documents_delta,
            embedding_source_columns=[
                EmbeddingSourceColumn(
                    name="content",
                    embedding_model_endpoint_name=s.llm_default_embeddings,
                )
            ],
            pipeline_type=PipelineType.TRIGGERED,
        ),
    )


def trigger_documents_sync() -> None:
    s = settings()
    _client().vector_search_indexes.sync_index(index_name=s.documents_index)


# ---- Upsert (messages) -------------------------------------------------------


async def upsert_message(
    *,
    public_id: str,
    workspace_name: str,
    peer_name: str,
    session_name: str,
    content: str,
    embedding: list[float],
    created_at: str,
) -> None:
    """Insert/update a single message vector. Inline at message-create time."""
    s = settings()
    payload = [
        {
            "public_id": public_id,
            "workspace_name": workspace_name,
            "peer_name": peer_name,
            "session_name": session_name,
            "content": content,
            "embedding": embedding,
            "created_at": created_at,
        }
    ]
    await asyncio.to_thread(
        _client().vector_search_indexes.upsert_data_vector_index,
        index_name=s.messages_index,
        inputs_json=json.dumps(payload),
    )


async def delete_messages(public_ids: list[str]) -> None:
    if not public_ids:
        return
    s = settings()
    await asyncio.to_thread(
        _client().vector_search_indexes.delete_data_vector_index,
        index_name=s.messages_index,
        primary_keys=public_ids,
    )


# ---- Query -------------------------------------------------------------------


async def query_messages(
    *,
    query_text: str,
    workspace_name: str,
    peer_name: str | None = None,
    session_name: str | None = None,
    num_results: int = 10,
    hybrid: bool = True,
) -> list[MessageHit]:
    s = settings()
    filters: dict[str, Any] = {"workspace_name": workspace_name}
    if peer_name:
        filters["peer_name"] = peer_name
    if session_name:
        filters["session_name"] = session_name

    # messages_idx is a Direct Access index — the API requires query_vector;
    # query_text alone is rejected with InvalidParameterValue. Embed the
    # query upfront; for HYBRID also send the original text so BM25 can
    # contribute to scoring.
    query_vector = await embed(query_text)
    raw = await asyncio.to_thread(
        _client().vector_search_indexes.query_index,
        index_name=s.messages_index,
        columns=["public_id", "workspace_name", "peer_name", "session_name", "content"],
        query_vector=query_vector,
        query_text=query_text if hybrid else None,
        query_type="HYBRID" if hybrid else "ANN",
        num_results=num_results,
        filters_json=json.dumps(filters),
    )
    return _parse_message_hits(raw)


async def query_documents(
    *,
    query_text: str,
    workspace_name: str,
    observer: str | None = None,
    observed: str | None = None,
    level: str | None = None,
    num_results: int = 10,
    hybrid: bool = False,
) -> list[DocumentHit]:
    s = settings()
    filters: dict[str, Any] = {"workspace_name": workspace_name}
    if observer:
        filters["observer"] = observer
    if observed:
        filters["observed"] = observed
    if level:
        filters["level"] = level

    raw = await asyncio.to_thread(
        _client().vector_search_indexes.query_index,
        index_name=s.documents_index,
        columns=["id", "workspace_name", "observer", "observed", "level", "content"],
        query_text=query_text,
        query_type="HYBRID" if hybrid else "ANN",
        num_results=num_results,
        filters_json=json.dumps(filters),
    )
    return _parse_document_hits(raw)


def _parse_message_hits(raw: Any) -> list[MessageHit]:
    out: list[MessageHit] = []
    for row in (raw.result.data_array if raw and raw.result else []) or []:
        out.append(
            MessageHit(
                public_id=row[0],
                workspace_name=row[1],
                peer_name=row[2],
                session_name=row[3],
                content=row[4] or "",
                score=float(row[-1]),
            )
        )
    return out


def _parse_document_hits(raw: Any) -> list[DocumentHit]:
    out: list[DocumentHit] = []
    for row in (raw.result.data_array if raw and raw.result else []) or []:
        out.append(
            DocumentHit(
                id=row[0],
                workspace_name=row[1],
                observer=row[2],
                observed=row[3],
                level=row[4],
                content=row[5],
                score=float(row[-1]),
            )
        )
    return out


# ---- Embeddings (FMAPI) ------------------------------------------------------


async def embed(text: str, *, endpoint: str | None = None) -> list[float]:
    """Compute a single embedding via Foundation Model APIs."""
    s = settings()
    name = endpoint or s.llm_default_embeddings
    resp = await asyncio.to_thread(
        _client().serving_endpoints.query, name=name, input=[text]
    )
    # Response shape: {"data": [{"embedding": [...]}], ...}
    data = resp.as_dict() if hasattr(resp, "as_dict") else resp
    return list(data["data"][0]["embedding"])
