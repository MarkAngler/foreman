"""Webhooks worker.

Polls Lakebase queue_items for unprocessed webhook tasks, fans them out to all
matching webhook_endpoints in the same workspace, POSTs the signed payload, and
records delivery outcomes to a Delta results table.

Spark cannot write to Lakebase from this job (per spine contract), so
queue_items.processed is updated by a Phase-3 reconciler that reads
webhook_results_delta. See worktree_notes.md.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx

from foreman.jobs._bootstrap import apply_env_overrides

apply_env_overrides()

from foreman.jobs.webhooks.delivery import (  # noqa: E402
    MAX_ATTEMPTS,
    DeliveryResult,
    build_headers,
    build_payload,
    deliver,
    event_matches,
    serialize_body,
)
from foreman.jobs.webhooks.results import (  # noqa: E402
    append_results,
    ensure_webhook_results_delta,
    webhook_results_table,
)
from foreman.lib.lakebase_read import read_query  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import Row, SparkSession

QUEUE_BATCH_SIZE = 1000
QUEUE_QUERY = (
    "SELECT id, payload, created_at FROM queue_items "
    "WHERE task_type = 'webhook' AND processed = FALSE "
    f"ORDER BY created_at LIMIT {QUEUE_BATCH_SIZE}"
)
ENDPOINTS_QUERY = (
    "SELECT id, workspace_name, url, secret, event_types FROM webhook_endpoints"
)


def info(msg: str) -> None:
    print(f"[webhooks] {msg}", flush=True)


def _parse_payload(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    return json.loads(raw)


def _group_endpoints(rows: Iterable[Row]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        d = row.asDict()
        grouped[d["workspace_name"]].append(d)
    return grouped


def _attempt_counts(spark: SparkSession) -> dict[tuple[int, str], int]:
    """Count prior delivery attempts per (queue_item_id, endpoint_id) from the
    Delta results table so retries are bounded across job runs."""
    table = webhook_results_table()
    df = spark.sql(
        f"SELECT queue_item_id, endpoint_id, COUNT(*) AS n FROM {table} "
        "GROUP BY queue_item_id, endpoint_id"
    )
    return {(r["queue_item_id"], r["endpoint_id"]): int(r["n"]) for r in df.collect()}


def _result_row(
    item_id: int,
    endpoint_id: str,
    event_type: str,
    workspace_name: str,
    attempt_count: int,
    result: DeliveryResult,
    attempted_at: datetime,
) -> dict[str, Any]:
    return {
        "queue_item_id": int(item_id),
        "endpoint_id": endpoint_id,
        "event_type": event_type,
        "workspace_name": workspace_name,
        "status_code": result.status_code,
        "success": result.success,
        "retryable": result.retryable,
        "error": result.error,
        "attempt_count": attempt_count,
        "attempted_at": attempted_at,
    }


def _process_item(
    item: dict[str, Any],
    endpoints_by_workspace: dict[str, list[dict[str, Any]]],
    prior_attempts: dict[tuple[int, str], int],
    client: httpx.Client,
) -> list[dict[str, Any]]:
    payload = _parse_payload(item.get("payload"))
    event_type = payload.get("event_type")
    workspace_name = payload.get("workspace_name")
    if not event_type or not workspace_name:
        info(f"skipping queue_item {item['id']}: missing event_type or workspace_name")
        return []

    occurred_at = payload.get("occurred_at")
    parsed_occurred_at = (
        datetime.fromisoformat(occurred_at) if isinstance(occurred_at, str) else None
    )

    endpoints = endpoints_by_workspace.get(workspace_name, [])
    if not endpoints:
        return []

    body_payload = build_payload(
        event_type=event_type,
        workspace_name=workspace_name,
        data=payload.get("data") or {},
        occurred_at=parsed_occurred_at,
    )
    body = serialize_body(body_payload)

    rows: list[dict[str, Any]] = []
    item_id = int(item["id"])
    for endpoint in endpoints:
        if not event_matches(endpoint.get("event_types"), event_type):
            continue
        prior = prior_attempts.get((item_id, endpoint["id"]), 0)
        if prior >= MAX_ATTEMPTS:
            continue
        attempt = prior + 1
        headers = build_headers(event_type, body, endpoint.get("secret"))
        result = deliver(endpoint["url"], body, headers, client=client)
        rows.append(
            _result_row(
                item_id=item_id,
                endpoint_id=endpoint["id"],
                event_type=event_type,
                workspace_name=workspace_name,
                attempt_count=attempt,
                result=result,
                attempted_at=datetime.now(tz=UTC),
            )
        )
    return rows


def run(spark: SparkSession) -> int:
    ensure_webhook_results_delta(spark)

    queue_df = read_query(spark, QUEUE_QUERY)
    queue_rows = [r.asDict() for r in queue_df.collect()]
    if not queue_rows:
        info("no pending webhook items")
        return 0

    endpoints_df = read_query(spark, ENDPOINTS_QUERY)
    endpoints_by_workspace = _group_endpoints(endpoints_df.collect())
    prior_attempts = _attempt_counts(spark)

    info(f"processing {len(queue_rows)} queue items across {len(endpoints_by_workspace)} workspaces")

    all_rows: list[dict[str, Any]] = []
    with httpx.Client(timeout=30.0) as client:
        for item in queue_rows:
            all_rows.extend(
                _process_item(item, endpoints_by_workspace, prior_attempts, client)
            )

    if not all_rows:
        info("no deliveries attempted (no matching endpoints)")
        return 0

    results_df = spark.createDataFrame(all_rows)
    append_results(spark, results_df)
    successes = sum(1 for r in all_rows if r["success"])
    info(f"recorded {len(all_rows)} delivery results ({successes} succeeded)")
    return 0


def main() -> int:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    return run(spark)


if __name__ == "__main__":
    sys.exit(main())
