"""P3 Silver (docs/DECISIONS.md D19, D23).

Two kinds of streams read Bronze:
  silver_<table>  dashboard path. State-store dedup on event_id (dropDuplicatesWithinWatermark, 7 days on
                  kafka_ts = Kafka retention) -> insert-only MERGE into Silver, bounded to the batch's
                  event_date range (BN-1) -> optional remote outbox staging.
  silver_audit    quarantine path. Keeps topic:event_id -> payload_hash in the state store and writes
                  id_conflict / invalid_json / invalid_schema rows to silver.quarantine.
"""
from __future__ import annotations

import logging
import re
import time
from datetime import timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import compaction
import sinks
import tables as tb
from audit_state import DECISION_SCHEMA, STATE_SCHEMA, audit
from common import settings
from common.observability import record_stage
from common.lateness import LATE_SECONDS  # event_time falls back to received_at beyond this gap

log = logging.getLogger("silver")

_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")  # also guards the SQL literal
LOOKBACK_DAYS = 7  # >= collector MAX_EVENT_AGE (6 days) and Kafka retention (7 days)
SPECS = {
    # topic: (json schema, table)
    "nv.learning.v2": (tb.LEARNING_JSON, "silver.learning_events"),
    "nv.update.v1": (tb.UPDATE_JSON, "silver.update_events"),
}


def complete():
    """Rows Silver accepts. A function: Column objects need an active SparkSession."""
    return (F.col("event_id").isNotNull() & F.col("payload_hash").isNotNull() & F.col("player_id").isNotNull()
            & F.col("event_type").isNotNull() & F.col("occurred_at").isNotNull()
            & F.col("received_at").isNotNull() & (F.col("schema_version") == 2))


def parse(df: DataFrame, schema) -> DataFrame:
    return (df.withColumn("j", F.from_json("value", schema))
            .select("value", F.col("topic").alias("kafka_topic"), F.col("partition").alias("kafka_partition"),
                    F.col("offset").alias("kafka_offset"), "kafka_ts", "j.*")
            .withColumn("occurred_at", F.to_timestamp("occurred_at"))
            .withColumn("received_at", F.to_timestamp("received_at")))


def merge_condition(lo, hi, id_lo=None, id_hi=None) -> str:
    """Insert-only MERGE key, bounded to the event_date partitions a legal duplicate can live in.

    The MERGE is the second line of defence behind the state-store dedup (e.g. after a checkpoint rebuild).
    event_time falls back to received_at for late events, so a resend can land on a later event_date than
    the original. Events older than 6 days are rejected by the collector (MAX_EVENT_AGE) and Kafka keeps
    7 days, so the original of any duplicate lies within 7 days before the batch: scan [lo - 7d, hi + 1d]
    instead of the whole table (BN-1, see DELTA_REQUIREMENTS_REVIEW section 5)."""
    cond = "t.event_id = s.event_id"
    if lo is not None and hi is not None:
        cond += (f" AND t.event_date BETWEEN DATE '{(lo - timedelta(days=LOOKBACK_DAYS)).isoformat()}'"
                 f" AND DATE '{(hi + timedelta(days=1)).isoformat()}'")
    if id_lo and id_hi and _UUID.match(id_lo) and _UUID.match(id_hi):
        # UUIDv7 ids sort by time, so Delta can skip every file whose event_id min/max misses this range.
        # Always correct (a matching target row has an id inside the batch's own range); with random v4 ids
        # the range is just wide and nothing is skipped.
        cond += f" AND t.event_id BETWEEN '{id_lo}' AND '{id_hi}'"
    return cond


def id_day_col():
    """Spark mirror of common.lateness.v7_day: creation day of an UUIDv7 event_id, 'other' for other ids."""
    hexs = F.regexp_replace(F.col("event_id"), "-", "")
    ms = F.conv(F.substring(hexs, 1, 12), 16, 10).cast("long")
    return F.coalesce(F.when(F.substring(F.col("event_id"), 15, 1) == "7",
                  F.date_format(F.from_unixtime(ms / 1000), "yyyy-MM-dd")), F.lit("other"))


def merge_groups(source: DataFrame) -> list:
    """Split a batch by the creation day of its ids (7th round). One late event (an offline queue flushed days later,
    or a v4 id) used to stretch the batch's single event_id range back to its own day, so the MERGE read every
    file in between. Each group now gets its own narrow range. Usually there is one group (one MERGE as before)."""
    days = [r.d for r in source.select(id_day_col().alias("d")).distinct().collect()]
    if len(days) <= 1:
        return [source]
    return [source.where(id_day_col() == d) for d in sorted(days)]


def merge_plan(source: DataFrame, max_groups: int = 16) -> list:
    """One cached-source aggregation for group bounds; cap driver metadata and MERGE fan-out.

    Falling back to the whole batch changes performance only, never the match predicate's safety.
    Each tuple is (source, lo, hi, id_lo, id_hi, dates, row_count).
    """
    limit = max(1, int(max_groups))
    aggregates = [F.min('event_date').alias('lo'), F.max('event_date').alias('hi'),
                  F.min('event_id').alias('id_lo'), F.max('event_id').alias('id_hi'),
                  F.collect_set('event_date').alias('dates'), F.count('*').alias('rows')]
    bounds = source.groupBy(id_day_col().alias('_id_day')).agg(*aggregates).limit(limit+1).collect()
    if len(bounds) > limit:
        log.warning('silver MERGE groups exceed %d; using one safe whole-batch range', limit)
        row = source.agg(*aggregates).first()
        return [(source, row.lo, row.hi, row.id_lo, row.id_hi, row.dates, row.rows)]
    single = len(bounds) == 1
    return [(source if single else source.where(id_day_col() == row._id_day),
             row.lo, row.hi, row.id_lo, row.id_hi, row.dates, row.rows)
            for row in sorted(bounds, key=lambda r: r._id_day)]


def _write_silver(table: str, name: str):
    def write(batch: DataFrame, batch_id: int) -> None:
        from delta.tables import DeltaTable
        spark = batch.sparkSession
        started = time.monotonic()
        merged = staged = started
        # Events are immutable: replay inserts no extra rows; conflicting payloads never overwrite.
        source = batch.drop("kafka_ts").persist()
        had_rows = False
        try:
            if settings.get().silver_optimization_enabled:
                groups = merge_plan(source, settings.get().silver_max_merge_groups)
            elif not source.isEmpty():
                groups = []
                for part in merge_groups(source):
                    r = part.agg(F.min('event_date'), F.max('event_date'), F.min('event_id'),
                                 F.max('event_id'), F.collect_set('event_date'), F.count('*')).first()
                    groups.append((part, *r))
            else:
                groups = []
            planned = time.monotonic()
            if groups:
                had_rows = True
                touched = set()
                for part, lo, hi, id_lo, id_hi, dates, row_count in groups:
                    # the same aggregation also lists every date this group writes (not only min/max), so a
                    # middle date of a late burst is a compaction candidate too (follow-up review 2026-11-21 #4)
                    (DeltaTable.forPath(spark, tb.path(table)).alias("t")
                     .merge(part.alias("s"), merge_condition(lo, hi, id_lo, id_hi))
                     .whenNotMatchedInsertAll().execute())
                    touched.update(dates or ())
                if len(groups) > 1:
                    log.info("silver %s batch %s split into %d MERGE groups by id day", name, batch_id, len(groups))
                merged = time.monotonic()
                if settings.get().remote_export_enabled:
                    sinks.stage_outbox(row.asDict(recursive=True) for row in source.toLocalIterator())
                staged = time.monotonic()
                # every COMPACT_EVERY_BATCHES batches, in this thread (cannot conflict with this MERGE), after the
                # outbox staging so the remote export never waits for a compaction (follow-up review #2)
                compaction.compact_after_write(spark, table, "event_date", touched)
        finally:
            source.unpersist()
        log.info("silver %s batch %s in %.1fs (merge %.1fs, outbox %.1fs)", name, batch_id,
                 time.monotonic() - started, max(0.0, merged - started), max(0.0, staged - merged))
        if had_rows:  # don't replace a useful measurement with an empty no-data batch
            record_stage("silver_" + name, batch_id=int(batch_id),
                         merge_ms=round(max(0.0, merged-started)*1000),
                         plan_ms=round(max(0.0, planned-started)*1000),
                         merge_groups=len(groups), input_rows=sum(group[-1] for group in groups),
                         outbox_ms=round(max(0.0, staged-merged)*1000),
                         maintenance_ms=round(max(0.0, time.monotonic()-staged)*1000))
    return write


def start_silver(spark, topic: str):
    s = settings.get()
    schema, table = SPECS[topic]
    name = table.split(".")[1]
    rows = (parse(spark.readStream.format("delta").load(tb.path("bronze.game_events"))
                  .where(F.col("topic") == topic), schema)
            .where(complete())
            .withWatermark("kafka_ts", f"{s.dedup_ttl_hours} hours")
            .dropDuplicatesWithinWatermark(["event_id"])
            .withColumn("event_time",
                        F.when(F.abs(F.col("occurred_at").cast("long") - F.col("received_at").cast("long"))
                               > LATE_SECONDS, F.col("received_at")).otherwise(F.col("occurred_at")))
            .withColumn("event_date", F.to_date("event_time"))
            # kafka_ts (the watermark column) must stay in the streaming plan; it is dropped per batch
            .select(*tb.SCHEMAS[table][0].fieldNames(), "kafka_ts"))
    return (rows.writeStream.queryName("silver_" + name).foreachBatch(_write_silver(table, name))
            .option("checkpointLocation", tb.checkpoint(f"silver-{name}-v2"))
            .trigger(processingTime=s.stage_trigger).start())


def start_silver_learning(spark):
    return start_silver(spark, "nv.learning.v2")


def start_silver_update(spark):
    return start_silver(spark, "nv.update.v1")


def _append(df: DataFrame, table: str, app: str, batch_id: int) -> None:
    # idempotent append: a replayed batch_id is skipped by Delta (txnAppId/txnVersion)
    (df.write.format("delta").mode("append")
     .option("txnAppId", app).option("txnVersion", batch_id).save(tb.path(table)))


def to_quarantine(decisions: DataFrame, batch_id: int) -> None:
    started = time.monotonic()
    decisions = decisions.persist()
    try:
        q = (decisions.where(F.col("action") != "first")
             .select(F.col("topic").alias("kafka_topic"), F.col("partition").alias("kafka_partition"),
                     F.col("offset").alias("kafka_offset"),
                     F.when(F.col("action") == "id_conflict", F.lit("id_conflict"))
                      .when(F.get_json_object("value", "$").isNull(), F.lit("invalid_json"))
                      .otherwise(F.lit("invalid_schema")).alias("reason"),
                     "event_id", "value", F.current_timestamp().alias("detected_at")))
        # first copies that the silver stream rejects as incomplete are invalid_schema too
        for topic, (schema, _) in SPECS.items():
            first = parse(decisions.where((F.col("action") == "first") & (F.col("topic") == topic))
                          .withColumn("kafka_ts", F.current_timestamp()), schema)
            q = q.unionByName(first.where(~complete()).select(
                "kafka_topic", "kafka_partition", "kafka_offset", F.lit("invalid_schema").alias("reason"),
                "event_id", "value", F.current_timestamp().alias("detected_at")))
        q = q.persist()
        if q.limit(1).count():
            _append(q, "silver.quarantine", "silver-quarantine-v2", batch_id)
        q.unpersist()
    finally:
        decisions.unpersist()
    log.info("audit batch %s in %.1fs", batch_id, time.monotonic() - started)


def start_silver_audit(spark):
    from pyspark.sql.streaming.state import GroupStateTimeout
    bronze = (spark.readStream.format("delta").load(tb.path("bronze.game_events"))
              .where(F.col("topic").isin(*SPECS)))
    eid = F.get_json_object("value", "$.event_id")
    phash = F.get_json_object("value", "$.payload_hash")
    keyed = bronze.select(
        F.when(eid.isNotNull() & phash.isNotNull(), F.concat_ws(":", "topic", eid))
         .otherwise(F.concat_ws(":", F.lit("bad"), "topic", "partition", "offset")).alias("key"),
        "value", "topic", "partition", "offset", eid.alias("event_id"), phash.alias("hash"))
    decisions = keyed.groupBy("key").applyInPandasWithState(
        audit, outputStructType=DECISION_SCHEMA, stateStructType=STATE_SCHEMA,
        outputMode="append", timeoutConf=GroupStateTimeout.ProcessingTimeTimeout)
    return (decisions.writeStream.queryName("silver_audit").foreachBatch(to_quarantine)
            .option("checkpointLocation", tb.checkpoint("silver-audit-v2"))
            .trigger(processingTime=settings.get().audit_trigger).start())
