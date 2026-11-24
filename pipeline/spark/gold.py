"""P4 Gold windows -> Delta + MongoDB rt_* (D21: MERGE bounded by window_start)."""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import timedelta
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import compaction
import sinks
import tables as tb
from common import settings
from common.timeutil import window_key

log = logging.getLogger("gold")

ALERT_MIN_ATTEMPTS = 20
ALERT_MIN_RATE = 0.4
ALERT_BASELINE_FACTOR = 1.5


# ---------------------------------------------------------------- MongoDB publishing (reliability review P1-6)
# Gold Delta is the source of truth; rt_* in MongoDB is its published copy. When the publish of a batch fails
# after the sink retries, the affected window range is remembered in a small JSON file on the lake volume
# (MongoDB is the thing that is down) and republish_pending() copies those windows from Gold Delta again once
# MongoDB answers. Publishing is an _id replace, so repeating it never inflates a value, and a window is
# repaired even if no new event ever arrives for it.
PUBLISH = {  # collection -> (gold table, document id)
    "rt_minigame_1m": ("gold.minigame_1m", lambda r: f"{r['minigame_id']}:{window_key(r['window_start'])}"),
    "rt_alerts": ("gold.failure_alerts", lambda r: f"{r['minigame_id']}:{window_key(r['window_start'])}"),
    "rt_updates": ("gold.update_progress_5m", lambda r: f"{r['target_version']}:{window_key(r['window_start'])}"),
}
_pending_lock = threading.Lock()


def _pending_path() -> Path:
    return Path(settings.get().progress_path).with_name("gold-republish.json")


def _load_pending() -> list[dict]:
    try:
        return json.loads(_pending_path().read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return []


def _save_pending(items: list[dict]) -> None:
    path = _pending_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(items), encoding="utf-8")
    tmp.replace(path)


def publish(collection: str, rows: list[dict]) -> bool:
    """Upsert Gold rows into MongoDB; on final failure remember their window range for republish."""
    docs = [dict(r, _id=PUBLISH[collection][1](r)) for r in rows]
    if sinks.upsert(collection, docs) or not docs:
        return True
    lo = min(r["window_start"] for r in rows).isoformat()
    hi = max(r["window_start"] for r in rows).isoformat()
    with _pending_lock:
        items = _load_pending()
        items.append({"collection": collection, "lo": lo, "hi": hi})
        _save_pending(items)
    log.warning("publish %s failed; windows %s..%s queued for republish", collection, lo, hi)
    return False


def pending_republish() -> int:
    with _pending_lock:
        return len(_load_pending())


def republish_pending(spark) -> int:
    """Copy remembered window ranges from Gold Delta to MongoDB again. Returns ranges still pending."""
    from datetime import datetime
    with _pending_lock:
        items = _load_pending()
    if not items:
        return 0
    left = []
    for item in items:
        table, _ = PUBLISH[item["collection"]]
        lo, hi = datetime.fromisoformat(item["lo"]), datetime.fromisoformat(item["hi"])
        rows = [r.asDict() for r in spark.read.format("delta").load(tb.path(table))
                .where((F.col("window_start") >= F.lit(lo)) & (F.col("window_start") <= F.lit(hi))).collect()]
        docs = [dict(r, _id=PUBLISH[item["collection"]][1](r)) for r in rows]
        if docs and not sinks.upsert(item["collection"], docs):
            left.append(item)
        else:
            log.info("republished %s windows %s..%s (%d rows)", item["collection"], item["lo"], item["hi"], len(docs))
    with _pending_lock:  # keep ranges that failed again plus any added while we were copying
        current = _load_pending()
        # Remove only the occurrences that this pass consumed. A new failure may append an
        # identical range while the copy is in progress; membership filtering loses it.
        added = list(current)
        for item in items:
            try:
                added.remove(item)
            except ValueError:
                pass
        _save_pending(left + added)
    return len(left) + len(added)


def count_of(event_type: str):
    return F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0)).cast("long")


def merge(spark, table: str, df: DataFrame, keys: list[str]) -> None:
    from delta.tables import DeltaTable
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    # every Gold table is keyed by window_start: a literal lower bound lets Delta skip files of older windows
    # by their min/max stats, so the MERGE reads only recent files however long the table grows (D21)
    lo = df.agg(F.min("window_start")).first()[0]
    if lo is None:
        return
    cond += f" AND t.window_start >= TIMESTAMP '{lo.strftime('%Y-%m-%d %H:%M:%S')}'"
    (DeltaTable.forPath(spark, tb.path(table)).alias("t").merge(df.alias("s"), cond)
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def gold_minigame(agg: DataFrame, batch_id: int) -> None:
    started = time.monotonic()
    rows = (agg.select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                       "minigame_id", "category", "attempts", "successes", "failures", "hints", "players")
            .withColumn("success_rate", F.when((F.col("successes") + F.col("failures")) > 0,
                                               F.col("successes") / (F.col("successes") + F.col("failures"))))
            .withColumn("updated_at", F.current_timestamp())).persist()
    try:
        merge(agg.sparkSession, "gold.minigame_1m", rows, ["window_start", "minigame_id"])
        publish("rt_minigame_1m", [r.asDict() for r in rows.collect()])
        # after the MongoDB publish, so the live page never waits for a compaction (follow-up review #2)
        compaction.compact_after_write(agg.sparkSession, "gold.minigame_1m")
    finally:
        rows.unpersist()
    log.info("gold_1m batch %s in %.1fs", batch_id, time.monotonic() - started)


def gold_alerts(agg: DataFrame, batch_id: int) -> None:
    spark = agg.sparkSession
    cur = (agg.select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                      "minigame_id", "category", "attempts", "successes", "failures")
           .withColumn("failure_rate", F.when((F.col("successes") + F.col("failures")) > 0,
                                              F.col("failures") / (F.col("successes") + F.col("failures"))))
           .localCheckpoint())
    # baseline = failure rate over the 30 minutes before the window, from gold.minigame_1m
    lo = cur.agg(F.min("window_start")).first()[0]
    if lo is None:
        return
    hist = (spark.read.format("delta").load(tb.path("gold.minigame_1m"))
            .where(F.col("window_start") >= F.lit(lo - timedelta(minutes=30)))  # file skipping (D21)
            .select(F.col("minigame_id").alias("h_game"), F.col("window_start").alias("h_start"),
                    F.col("successes").alias("h_s"), F.col("failures").alias("h_f")))
    keys = cur.select(F.col("window_start").alias("k_start"), F.col("minigame_id").alias("k_game")).distinct()
    joined = keys.join(hist, (keys.k_game == hist.h_game)
                       & (hist.h_start >= keys.k_start - F.expr("INTERVAL 30 MINUTES"))
                       & (hist.h_start < keys.k_start), "left")
    base = (joined.groupBy("k_start", "k_game")
            .agg(F.sum("h_f").alias("bf"), F.sum(F.col("h_s") + F.col("h_f")).alias("bt"))
            .select(F.col("k_start").alias("window_start"), F.col("k_game").alias("minigame_id"),
                    F.when(F.col("bt") > 0, F.col("bf") / F.col("bt")).alias("baseline_rate")))
    rows = (cur.join(base, ["window_start", "minigame_id"], "left")
            .withColumn("alert", (F.col("attempts") >= ALERT_MIN_ATTEMPTS)
                        & F.coalesce(F.col("failure_rate") >= ALERT_MIN_RATE, F.lit(False))
                        & (F.col("baseline_rate").isNull()
                           | (F.col("failure_rate") >= F.col("baseline_rate") * ALERT_BASELINE_FACTOR)))
            .withColumn("updated_at", F.current_timestamp())
            .select(*tb.SCHEMAS["gold.failure_alerts"][0].fieldNames())).persist()
    try:
        merge(spark, "gold.failure_alerts", rows, ["window_start", "minigame_id"])
        publish("rt_alerts", [r.asDict() for r in rows.collect()])
        compaction.compact_after_write(spark, "gold.failure_alerts")
    finally:
        rows.unpersist()


def start_gold_1m(spark):
    silver = spark.readStream.format("delta").load(tb.path("silver.learning_events"))
    one_min = (silver.withWatermark("event_time", "2 minutes")
               .groupBy(F.window("event_time", "1 minute"), "minigame_id", "category")
               .agg(count_of("attempt_started").alias("attempts"), count_of("attempt_succeeded").alias("successes"),
                    count_of("attempt_failed").alias("failures"), count_of("hint_used").alias("hints"),
                    F.approx_count_distinct("player_id").alias("players")))
    return (one_min.writeStream.outputMode("update").queryName("gold_1m").foreachBatch(gold_minigame)
            .option("checkpointLocation", tb.checkpoint("gold-minigame-1m-v1"))
            .trigger(processingTime=settings.get().stage_trigger).start())


def on_time():
    """Not late (common.lateness): play and receive time within LATE_SECONDS."""
    from common.lateness import LATE_SECONDS
    return F.abs(F.col("received_at").cast("long") - F.col("occurred_at").cast("long")) <= LATE_SECONDS


def start_gold_alert(spark):
    # 7th round: a failure spike is judged on events played now. Late events (an offline queue or an outage
    # backlog) still count in gold_1m and in the play-date daily table, but cannot raise a false spike alert.
    # A stateless filter before the aggregation keeps the existing checkpoint compatible.
    silver = spark.readStream.format("delta").load(tb.path("silver.learning_events")).where(on_time())
    five_min = (silver.withWatermark("event_time", "2 minutes")
                .groupBy(F.window("event_time", "5 minutes", "1 minute"), "minigame_id", "category")
                .agg(count_of("attempt_started").alias("attempts"), count_of("attempt_succeeded").alias("successes"),
                     count_of("attempt_failed").alias("failures")))
    return (five_min.writeStream.outputMode("update").queryName("gold_alert").foreachBatch(gold_alerts)
            .option("checkpointLocation", tb.checkpoint("gold-failure-alerts-v1"))
            .trigger(processingTime=settings.get().stage_trigger).start())


# ---------------------------------------------------------------- P4-2 update progress
def gold_updates(agg: DataFrame, batch_id: int) -> None:
    spark = agg.sparkSession
    cur = agg.select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                     "target_version", "checks", "prompted", "accepted", "dismissed", "downloads_started",
                     "downloads_succeeded", "downloads_failed", "mounted", "p50_duration_ms").persist()
    try:
        bounds = cur.agg(F.min("window_start").alias("lo"), F.max("window_end").alias("hi")).first()
        if bounds is None or bounds.lo is None:
            return
        failed = (spark.read.format("delta").load(tb.path("silver.update_events"))
                  .where((F.col("event_type") == "pack_download_failed")
                         & (F.col("event_time") >= F.lit(bounds.lo)) & (F.col("event_time") < F.lit(bounds.hi)))
                  .select(F.window("event_time", "5 minutes").start.alias("window_start"),
                          F.coalesce(F.col("details.target_version"), F.lit("none")).alias("target_version"),
                          F.col("details.failure_reason").alias("reason"))
                  .groupBy("window_start", "target_version", "reason").count())
        w = Window.partitionBy("window_start", "target_version").orderBy(F.desc("count"), F.asc("reason"))
        top = (failed.withColumn("rn", F.row_number().over(w)).where("rn = 1")
               .select("window_start", "target_version", F.col("reason").alias("top_failure_reason")))
        done = F.col("downloads_succeeded") + F.col("downloads_failed")
        rows = (cur.join(top, ["window_start", "target_version"], "left")
                .withColumn("failure_rate", F.when(done > 0, F.col("downloads_failed") / done))
                .withColumn("updated_at", F.current_timestamp())
                .select(*tb.SCHEMAS["gold.update_progress_5m"][0].fieldNames())).persist()
        try:
            merge(spark, "gold.update_progress_5m", rows, ["window_start", "target_version"])
            publish("rt_updates", [r.asDict() for r in rows.collect()])
            compaction.compact_after_write(spark, "gold.update_progress_5m")
        finally:
            rows.unpersist()
    finally:
        cur.unpersist()


def start_gold_update(spark):
    silver = spark.readStream.format("delta").load(tb.path("silver.update_events"))
    agg = (silver.withWatermark("event_time", "10 minutes")
           .withColumn("target_version", F.coalesce(F.col("details.target_version"), F.lit("none")))
           .groupBy(F.window("event_time", "5 minutes"), "target_version")
           .agg(count_of("update_checked").alias("checks"), count_of("update_prompted").alias("prompted"),
                count_of("update_accepted").alias("accepted"), count_of("update_dismissed").alias("dismissed"),
                count_of("pack_download_started").alias("downloads_started"),
                count_of("pack_download_succeeded").alias("downloads_succeeded"),
                count_of("pack_download_failed").alias("downloads_failed"),
                count_of("pack_mounted").alias("mounted"),
                F.percentile_approx(F.when(F.col("event_type") == "pack_download_succeeded",
                                           F.col("details.duration_ms")), 0.5).cast("long").alias("p50_duration_ms")))
    return (agg.writeStream.outputMode("update").queryName("gold_update").foreachBatch(gold_updates)
            .option("checkpointLocation", tb.checkpoint("gold-update-progress-v1"))
            .trigger(processingTime=settings.get().stage_trigger).start())
