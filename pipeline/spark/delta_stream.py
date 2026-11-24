"""NODEVERSE Delta pipeline: Kafka -> Bronze -> Silver -> Gold -> MongoDB (P2, P3, P4, P4-2).

One Spark application, one streaming query per stage, each with its own checkpoint:
  bronze         Kafka (learning, runtime, update)  -> bronze.game_events            (append, 5 s)   bronze.py
  silver_*       bronze -> silver.learning_events / silver.update_events (dedup, MERGE)  1 s     silver.py
  silver_audit   bronze -> silver.quarantine (invalid_json, invalid_schema, id_conflict)  30 s   silver.py
  gold_1m        silver.learning_events -> gold.minigame_1m   -> rt_minigame_1m                  gold.py
  gold_alert     silver.learning_events -> gold.failure_alerts -> rt_alerts                      gold.py
  gold_update    silver.update_events   -> gold.update_progress_5m -> rt_updates                gold.py
  remote_runtime bronze runtime events -> remote outbox (only with REMOTE_EXPORT_ENABLED=1)
A driver timer refreshes rt_summary and rt_versions (periodic.py). MongoDB writes retry (sinks.py).

Supervision (PL-2): a failed query is restarted on its own from its checkpoint, with backoff, while the
others keep running. After QUERY_RESTART_LIMIT restarts of one query within 10 minutes the app exits and
Docker restarts the whole container (restart: unless-stopped).
"""
from __future__ import annotations

import logging
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

sys.path.insert(0, "/opt/nodeverse")  # common/ and remote/ are mounted there (compose volumes)

import bronze  # noqa: E402
import gold  # noqa: E402
import periodic  # noqa: E402
import silver  # noqa: E402
import tables as tb  # noqa: E402
from common import settings  # noqa: E402

log = logging.getLogger("delta_stream")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@dataclass
class QuerySpec:
    name: str
    pool: str  # FAIR scheduler pool
    start: Callable
    restarts: list = field(default_factory=list)  # monotonic times of recent restarts


def start_runtime_export(spark):
    import json
    from pyspark.sql import functions as F
    import sinks
    rows = (spark.readStream.format("delta").load(tb.path("bronze.game_events"))
            .where(F.col("topic") == "nv.runtime.v1"))

    def write(batch, batch_id):
        sinks.stage_outbox(json.loads(row.value) for row in batch.select("value").toLocalIterator())
    return (rows.writeStream.queryName("remote_runtime").foreachBatch(write)
            .option("checkpointLocation", tb.checkpoint("remote-runtime-v1"))
            .trigger(processingTime=settings.get().stage_trigger).start())


def query_specs() -> list[QuerySpec]:
    specs = [QuerySpec("bronze", "bronze", bronze.start_bronze),
             QuerySpec("silver_learning_events", "silver", silver.start_silver_learning),
             QuerySpec("silver_update_events", "silver", silver.start_silver_update),
             QuerySpec("silver_audit", "audit", silver.start_silver_audit),
             QuerySpec("gold_1m", "gold", gold.start_gold_1m),
             QuerySpec("gold_alert", "gold", gold.start_gold_alert),
             QuerySpec("gold_update", "update", gold.start_gold_update)]
    if settings.get().remote_export_enabled:
        specs.append(QuerySpec("remote_runtime", "export", start_runtime_export))
    return specs


def _start(spark, spec: QuerySpec):
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", spec.pool)  # FAIR: one pool per stage
    return spec.start(spark)


def supervise(spark, specs: list[QuerySpec], stop: threading.Event, restarts: dict) -> None:
    """Restart failed queries one by one; give up (exit) when one keeps failing."""
    limit = settings.get().query_restart_limit
    running = {spec.name: _start(spark, spec) for spec in specs}
    log.info("started queries: %s", list(running))
    while not stop.is_set():
        try:
            spark.streams.awaitAnyTermination(timeout=5)
        except Exception:
            pass  # the failure is read from the query below
        for spec in specs:
            q = running[spec.name]
            if q.isActive:
                continue
            error = q.exception()
            now = time.monotonic()
            spec.restarts = [t for t in spec.restarts if now - t < 600] + [now]
            restarts[spec.name] = restarts.get(spec.name, 0) + 1
            log.error("query %s stopped (%s); restart %d in the last 10 min",
                      spec.name, type(error).__name__ if error else "no error", len(spec.restarts))
            if len(spec.restarts) > limit:
                raise SystemExit(f"query {spec.name} failed {len(spec.restarts)} times in 10 minutes")
            time.sleep(min(30, 2 ** len(spec.restarts)))
            running[spec.name] = _start(spark, spec)
        spark.streams.resetTerminated()


def main() -> None:
    spark = tb.spark_session("nodeverse-delta")
    tb.ensure_tables(spark)
    stop = threading.Event()
    restarts: dict = {}
    specs = query_specs()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "periodic")
    timer = threading.Thread(target=periodic.run, args=(spark, stop, restarts), daemon=True, name="periodic")
    try:
        timer.start()
        supervise(spark, specs, stop, restarts)
    finally:
        stop.set()


if __name__ == "__main__":
    main()
