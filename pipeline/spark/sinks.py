"""MongoDB writes from the Spark driver, with retries (RF-2 / PL-2).

rt_* aggregates are idempotent _id upserts recomputed on every window update, so after the retries are
exhausted the batch logs the failure and continues: the Delta (Gold) tables stay correct and the next update
of the window repairs the dashboard copy. The remote outbox is durable state, so its failure is raised and the
query restarts from its checkpoint (no event is skipped).
"""
from __future__ import annotations

import logging
import threading
import time

from common import settings
from common.db import local_mongo
from common.timeutil import window_key  # noqa: F401  (re-exported: rt_* document ids)

log = logging.getLogger("sinks")
_failures = {"count": 0, "last": None}
_lock = threading.Lock()


def failures() -> dict:
    with _lock:
        return dict(_failures)


def _retry(fn, what: str, raise_on_failure: bool):
    attempts = max(1, settings.get().sink_retry_attempts)
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:  # network blips, primary stepdown, container restart
            if i == attempts - 1:
                with _lock:
                    _failures["count"] += 1
                    _failures["last"] = f"{what}: {type(e).__name__}"
                if raise_on_failure:
                    raise
                log.error("mongo write failed after %d attempts (%s): %s", attempts, what, type(e).__name__)
                return None
            time.sleep(min(8, 2 ** i))


def upsert(collection: str, docs: list[dict]) -> bool:
    """Replace documents by _id (rt_* aggregates). False when the write failed after retries."""
    if not docs:
        return True
    from pymongo import ReplaceOne
    ops = [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs]
    return _retry(lambda: local_mongo()[collection].bulk_write(ops, ordered=False), "upsert " + collection,
                  False) is not None


def delete_missing(collection: str, keep_ids: list) -> bool:
    return _retry(lambda: local_mongo()[collection].delete_many({"_id": {"$nin": keep_ids}}), "prune " + collection,
                  False) is not None


def stage_outbox(records) -> None:
    """Durable remote export staging. Raises after retries so the batch is replayed, never skipped."""
    from remote.outbox import stage
    if not settings.get().silver_optimization_enabled:
        rows = list(records)
        _retry(lambda: stage(rows), "outbox", True)
        return
    from datetime import datetime, timezone
    from itertools import islice
    iterator = iter(records)
    queued_at = datetime.now(timezone.utc)  # same timestamp across all chunks and retries
    size = max(1, settings.get().outbox_stage_chunk)
    while rows := list(islice(iterator, size)):
        _retry(lambda: stage(rows, queued_at=queued_at), "outbox", True)
