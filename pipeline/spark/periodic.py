"""Driver timers: rt_summary (10 s), gold.version_adoption -> rt_versions (60 s), rt_rejects (60 s), progress file."""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import timedelta
from pathlib import Path

from pyspark.sql import functions as F

import compaction
import gold
import sinks
import tables as tb
from common import settings
from common.observability import stages
from common.lateness import LATE_SECONDS
from common.timeutil import iso_ms, utc_now

log = logging.getLogger("periodic")


def refresh_summary(spark) -> None:
    recent = (spark.read.format("delta").load(tb.path("silver.learning_events"))
              .where(F.col("event_date") >= F.date_sub(F.current_date(), 1))  # partition pruning
              # only the last 5 minutes feed either metric; filter before aggregating (CPU review, item 2)
              .where(F.col("received_at") >= F.expr("current_timestamp() - INTERVAL 5 MINUTES"))
              .select("received_at", "occurred_at", "session_id"))
    last_1m = F.col("received_at") >= F.expr("current_timestamp() - INTERVAL 1 MINUTE")
    late = F.abs(F.col("received_at").cast("long") - F.col("occurred_at").cast("long")) > LATE_SECONDS
    r = recent.agg(
        F.sum(F.when(last_1m, 1).otherwise(0)).alias("events_last_1m"),
        F.sum(F.when(last_1m & late, 1).otherwise(0)).alias("late_last_1m"),  # 7th round: counted, shown apart
        F.countDistinct("session_id").alias("active_sessions_5m")).first()
    sinks.upsert("rt_summary", [{"_id": "global", "events_last_1m": int(r.events_last_1m or 0),
                                 "late_last_1m": int(r.late_last_1m or 0),
                                 "active_sessions_5m": int(r.active_sessions_5m or 0),
                                 "updated_at": utc_now()}])


_last_adoption: dict = {"signature": None}


def refresh_version_adoption(spark) -> bool:
    """Recomputed every minute so players older than 24 h drop out even without new input, but the Delta MERGE
    and the MongoDB rewrite run only when (kind, version, installs) changed. Returns True when it wrote."""
    from delta.tables import DeltaTable
    tb.register_names(spark)
    latest = spark.sql("""
        SELECT player_id, build_version, content_version,
               row_number() OVER (PARTITION BY player_id ORDER BY play_ts DESC, build_version DESC) AS rn
        FROM (SELECT player_id, build_version, content_version, event_time,
                     -- 7th round: "current version" = newest by play time (clamped device time), not by receive
                     -- time, so an old-version event that arrives late does not look like a downgrade
                     least(greatest(occurred_at, received_at - INTERVAL 6 DAYS), received_at) AS play_ts
              FROM (SELECT player_id, build_version, content_version, event_time, occurred_at, received_at
                    FROM silver.learning_events WHERE event_date >= date_sub(current_date(), 1)
                    UNION ALL
                    SELECT player_id, build_version, content_version, event_time, occurred_at, received_at
                    FROM silver.update_events WHERE event_date >= date_sub(current_date(), 1)))
        WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS""").where("rn = 1")
    latest = latest.persist()
    try:
        total = latest.count()
        per = (latest.select(F.lit("build").alias("kind"), F.col("build_version").alias("version"))
               .unionByName(latest.select(F.lit("content").alias("kind"), F.col("content_version").alias("version")))
               .groupBy("kind", "version").agg(F.count(F.lit(1)).alias("installs"))
               .withColumn("share", F.col("installs") / F.lit(total if total else 1))
               .withColumn("updated_at", F.current_timestamp())).persist()
        try:
            rows = per.collect()
            signature = frozenset((r.kind, r.version, int(r.installs)) for r in rows)
            if signature == _last_adoption["signature"]:
                return False  # share = installs / total, so it is unchanged too
            (DeltaTable.forPath(spark, tb.path("gold.version_adoption")).alias("t")
             .merge(per.alias("s"), "t.kind = s.kind AND t.version = s.version")
             .whenMatchedUpdateAll().whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute())
            docs = [dict(r.asDict(), _id=f"{r.kind}:{r.version}") for r in rows]
            written = sinks.upsert("rt_versions", docs)
            # versions with no install in the last 24 h are removed in Delta and in MongoDB (D12)
            pruned = sinks.delete_missing("rt_versions", [d["_id"] for d in docs])
            if written and pruned:  # a failed Mongo write is retried on the next minute
                _last_adoption["signature"] = signature
            return True
        finally:
            per.unpersist()
    finally:
        latest.unpersist()


REJECT_SAMPLES = 5


def _reason_counts(df, since) -> list[dict]:
    return [{"reason": r["reason"] or "unknown", "n": int(r["n"])}
            for r in df.where(F.col("at") >= F.lit(since)).groupBy("reason").agg(F.count(F.lit(1)).alias("n"))
            .orderBy(F.desc("n")).limit(10).collect()]


def refresh_rejects(spark) -> None:
    """PL-8: what the pipeline rejected, for the server live page (rt_rejects/global).

    dlq        collector 422 rejects in Kafka nv.dlq.v1 (invalid_json, unknown_event_type, schema_error, too_old)
    quarantine silver.quarantine rows (id_conflict, invalid_json, invalid_schema found after Kafka)
    Counts for the last 60 minutes and 24 hours, top reasons (60 min), and a few recent DLQ samples without the
    raw body (it can hold player data; read it with tools/dlq_peek.py when needed)."""
    now = utc_now()
    h1, h24 = now - timedelta(hours=1), now - timedelta(hours=24)
    dlq = (spark.read.format("kafka").option("kafka.bootstrap.servers", tb.KAFKA).option("subscribe", "nv.dlq.v1")
           .option("startingTimestamp", str(int(h24.timestamp() * 1000)))
           .option("startingOffsetsByTimestampStrategy", "latest")  # no record in 24 h -> empty, not an error
           .option("endingOffsets", "latest").load()
           .select(F.col("timestamp").alias("at"),
                   F.get_json_object(F.col("value").cast("string"), "$.reason").alias("reason"),
                   F.get_json_object(F.col("value").cast("string"), "$.endpoint").alias("endpoint"),
                   F.get_json_object(F.col("value").cast("string"), "$.errors[0].loc").alias("loc"),
                   F.get_json_object(F.col("value").cast("string"), "$.errors[0].msg").alias("msg"))
           .where(F.col("at") >= F.lit(h24))).persist()
    quarantine = (spark.read.format("delta").load(tb.path("silver.quarantine"))
                  .where(F.col("detected_at") >= F.lit(h24))
                  .select(F.col("detected_at").alias("at"), "reason")).persist()
    try:
        samples = [{"at": r["at"], "reason": r["reason"], "endpoint": r["endpoint"],
                    "field": r["loc"], "message": (r["msg"] or "")[:160]}
                   for r in dlq.orderBy(F.desc("at")).limit(REJECT_SAMPLES).collect()]
        doc = {"_id": "global", "updated_at": now,
               "dlq": {"last_60m": dlq.where(F.col("at") >= F.lit(h1)).count(), "last_24h": dlq.count(),
                       "reasons_60m": _reason_counts(dlq, h1), "recent": samples},
               "quarantine": {"last_60m": quarantine.where(F.col("at") >= F.lit(h1)).count(),
                              "last_24h": quarantine.count(), "reasons_60m": _reason_counts(quarantine, h1)}}
    finally:
        dlq.unpersist()
        quarantine.unpersist()
    sinks.upsert("rt_rejects", [doc])


def write_progress(spark, restarts: dict) -> None:
    """Observability outside stdout; rows/rates are per query, not end-to-end throughput."""
    destination = Path(settings.get().progress_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    report = {"updated_at": iso_ms(), "sink_failures": sinks.failures(), "query_restarts": restarts,
              "effective_dedup_hours": settings.get().dedup_ttl_hours, "stage_timings": stages(),
              "gold_republish_pending": gold.pending_republish(),
              "queries": [{"name": q.name, "active": q.isActive, "status": q.status, "progress": q.lastProgress}
                          for q in spark.streams.active]}
    temporary = destination.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, default=str), encoding="utf-8")
    temporary.replace(destination)


def run(spark, stop: threading.Event, restarts: dict) -> None:
    last_versions = 0.0
    s = settings.get()
    last_bronze = time.monotonic()
    vacuum_thread = None
    last_vacuum = time.monotonic() - max(0, s.vacuum_interval_hours * 3600 - 600)  # first VACUUM ~10 min after start
    while not stop.wait(10):
        try:
            write_progress(spark, restarts)
            started = time.monotonic()
            refresh_summary(spark)
            log.info("rt_summary in %.2fs", time.monotonic() - started)
            if gold.pending_republish():  # P1-6: Gold windows whose MongoDB publish failed
                gold.republish_pending(spark)
            if time.monotonic() - last_versions >= 60:
                started = time.monotonic()
                wrote = refresh_version_adoption(spark)
                log.info("version adoption in %.2fs (%s)", time.monotonic() - started, "written" if wrote else "unchanged")
                started = time.monotonic()
                try:
                    refresh_rejects(spark)
                    log.info("rt_rejects in %.2fs", time.monotonic() - started)
                except Exception:  # never let the reject view stop rt_summary / versions
                    log.exception("rt_rejects refresh failed")
                last_versions = time.monotonic()
            # Bronze is append-only, so compacting it from this thread never conflicts with the Bronze stream
            if s.compact_bronze_minutes > 0 and time.monotonic() - last_bronze >= s.compact_bronze_minutes * 60:
                last_bronze = time.monotonic()
                compaction.compact_append_only(spark)
            if s.vacuum_interval_hours > 0 and time.monotonic() - last_vacuum >= s.vacuum_interval_hours * 3600:
                last_vacuum = time.monotonic()
                # its own thread: VACUUM of all tables takes ~1-2 minutes and must not make rt_summary stale
                if vacuum_thread is None or not vacuum_thread.is_alive():
                    vacuum_thread = threading.Thread(target=compaction.vacuum_safely, args=(spark,), daemon=True,
                                                     name="vacuum")
                    vacuum_thread.start()
        except Exception:  # keep the streams alive; next tick retries
            log.exception("periodic refresh failed")
