"""Small-file compaction and VACUUM for the streaming tables (Silver MERGE scaling review, 2026-11-21).

Why not Delta auto compaction: with spark.databricks.delta.autoCompact.enabled it ran after almost every commit
(MERGE 20 / auto OPTIMIZE 20 in the last 40 Silver commits) and rewrote the whole date partition each time
(~30 MB for 2 files). Live Silver data was ~30 MB but the table used 4.9 GB on disk (Bronze 7.5 GB), and the
rewrite time grew with the partition size, inside every micro-batch.

Instead:
  * compact_after_write(): called by the single writer of a table right after its own write, every
    COMPACT_EVERY_BATCHES batches, for the partitions it touched. Same thread as the MERGE, so it can never
    conflict with that MERGE. OPTIMIZE only rewrites files smaller than optimize.minFileSize (set in
    tables.spark_session), so files that already reached that size are sealed and never rewritten again.
  * compact_append_only(): Bronze is append-only (blind inserts never conflict with OPTIMIZE), so the driver
    timer compacts today's and yesterday's ingest_date every COMPACT_BRONZE_MINUTES.
  * vacuum(): removes files that are no longer part of the table after VACUUM_RETAIN_HOURS (default 168 h =
    Delta's default, longer than the Kafka retention and the dedup window), every VACUUM_INTERVAL_HOURS.
OPTIMIZE commits dataChange=false, so streams reading these tables (Silver -> Gold, Bronze -> Silver) skip them.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import date, timedelta

import tables as tb
from common import settings

log = logging.getLogger("compaction")
_lock = threading.Lock()
_pending: dict = {}  # table -> {"batches": n, "parts": set of partition values}


def _optimize(spark, table: str, where: str = "") -> dict:
    started = time.monotonic()
    sql = f"OPTIMIZE delta.`{tb.path(table)}`" + (f" WHERE {where}" if where else "")
    rows = spark.sql(sql).collect()
    metrics = rows[0].metrics.asDict() if rows and rows[0].metrics is not None else {}
    removed, added = metrics.get("numFilesRemoved", 0), metrics.get("numFilesAdded", 0)
    log.info("compact %s%s: %s files -> %s in %.1fs", table, f" ({where})" if where else "", removed, added,
             time.monotonic() - started)
    return {"removed": removed, "added": added}


def compact_after_write(spark, table: str, partition_col: str | None = None, values=()) -> None:
    """Count one write of `table`; every COMPACT_EVERY_BATCHES writes, compact the touched partitions."""
    every = settings.get().compact_every_batches
    if every <= 0:
        return
    with _lock:
        state = _pending.setdefault(table, {"batches": 0, "parts": set()})
        state["batches"] += 1
        state["parts"].update(str(v) for v in values if v is not None)
        if state["batches"] < every:
            return
        parts, state["batches"], state["parts"] = sorted(state["parts"]), 0, set()
    try:
        if partition_col and parts:
            quoted = ", ".join(f"DATE '{date.fromisoformat(p).isoformat()}'" for p in parts)
            _optimize(spark, table, f"{partition_col} IN ({quoted})")
        elif not partition_col:
            _optimize(spark, table)
    except Exception:  # compaction is housekeeping: never fail the batch that triggered it
        log.exception("compaction of %s failed; retried after the next %d writes", table, every)


def compact_append_only(spark) -> None:
    today = date.today()
    days = ", ".join(f"DATE '{d.isoformat()}'" for d in (today - timedelta(days=1), today))
    _optimize(spark, "bronze.game_events", f"ingest_date IN ({days})")


def vacuum(spark) -> None:
    hours = settings.get().vacuum_retain_hours
    for table in tb.SCHEMAS:
        started = time.monotonic()
        spark.sql(f"VACUUM delta.`{tb.path(table)}` RETAIN {hours} HOURS")
        log.info("vacuum %s (retain %dh) in %.1fs", table, hours, time.monotonic() - started)


def vacuum_safely(spark) -> None:
    try:
        vacuum(spark)
    except Exception:
        log.exception("vacuum failed; retried at the next interval")
