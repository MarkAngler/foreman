"""Webhook delivery reconciler — Phase 3 integration component.

The webhooks job (Phase 2 Agent E) writes per-attempt delivery outcomes to a
Delta table `webhook_results_delta`. Per the spine contract, Spark cannot write
back to Lakebase, so the `queue_items.processed` flag isn't updated by the job.

This module runs in the FastAPI app (which DOES have Lakebase write access via
the asyncpg pool). It periodically reads `webhook_results_mirror` (the
Delta -> Lakebase synced table that Phase 3 Step 5a sets up) and marks
queue_items processed where any successful delivery exists for that item.

Dependency: requires `webhook_results_mirror` Lakebase table populated by the
synced-table mechanism. Started as a background task by `app.main:lifespan`.
"""

from __future__ import annotations

import asyncio
import logging
import time

from sqlalchemy import text

from foreman.lib.lakebase import session
from foreman.lib.tracing import trace

logger = logging.getLogger(__name__)

RECONCILE_INTERVAL_SECONDS = 30

_MIRROR_TABLE = "webhook_results_mirror"
_MISSING_LOG_INTERVAL_SECONDS = 3600
_last_missing_log_at: float = 0.0


@trace(name="webhook_reconcile_batch")
async def reconcile_once() -> int:
    """Mark webhook queue items processed when any successful delivery exists.

    Returns the count of items marked.
    """
    global _last_missing_log_at
    async with session() as s:
        exists = await s.scalar(text("SELECT to_regclass(:t)"), {"t": _MIRROR_TABLE})
        if exists is None:
            now = time.monotonic()
            if now - _last_missing_log_at >= _MISSING_LOG_INTERVAL_SECONDS:
                logger.info(
                    "webhook reconciler: %s not present yet — "
                    "run `databricks bundle run schema_init` to create the synced table",
                    _MIRROR_TABLE,
                )
                _last_missing_log_at = now
            return 0

        result = await s.execute(
            text(
                """
                UPDATE queue_items q
                SET processed = TRUE, processed_at = NOW()
                FROM webhook_results_mirror w
                WHERE q.id = w.queue_item_id
                  AND q.task_type = 'webhook'
                  AND q.processed = FALSE
                  AND w.success = TRUE
                """
            )
        )
        await s.commit()
        return result.rowcount or 0


async def run_forever() -> None:
    while True:
        try:
            n = await reconcile_once()
            if n:
                logger.info("webhook reconciler: marked %d items processed", n)
        except Exception:
            logger.exception("webhook reconciler iteration failed; retrying after interval")
        await asyncio.sleep(RECONCILE_INTERVAL_SECONDS)


def start(app) -> asyncio.Task:
    """Start the reconciler as a background task. Returns the task handle."""
    task = asyncio.create_task(run_forever(), name="webhook_reconciler")
    return task
