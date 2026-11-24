"""P2 Bronze: Kafka (learning, runtime, update topics) -> bronze.game_events, append only."""
from __future__ import annotations

import tables as tb
from common import settings

TOPICS = "nv.learning.v2,nv.runtime.v1,nv.update.v1"


def start_bronze(spark):
    s = settings.get()
    src = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", tb.KAFKA)
           .option("subscribe", TOPICS)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "true")
           .option("maxOffsetsPerTrigger", str(s.max_offsets_per_trigger))
           .load())
    bronze = src.selectExpr("CAST(value AS STRING) AS value", "topic", "partition", "offset",
                            "timestamp AS kafka_ts", "to_date(timestamp) AS ingest_date")
    return (bronze.writeStream.format("delta").outputMode("append").queryName("bronze")
            .option("checkpointLocation", tb.checkpoint("bronze-v1"))
            .trigger(processingTime=s.bronze_trigger)
            .start(tb.path("bronze.game_events")))
