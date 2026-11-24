"""Read collector rejects (Kafka nv.dlq.v1) of the last N minutes, newest last (PL-8).

The server live page only shows counts, reasons and short samples. Use this to see the raw bodies:
  docker compose -p nodeverse --env-file .env -f compose.json exec collector python -m tools.dlq_peek --minutes 60
  ... --reason schema_error --limit 20 --raw
Nothing is consumed or committed (no consumer group offsets), so it can be run any number of times.
"""
from __future__ import annotations

import argparse
import json
import os
import time


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--minutes", type=int, default=60)
    p.add_argument("--reason", help="only this reason (invalid_json, unknown_event_type, schema_error, too_old)")
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--raw", action="store_true", help="also print the stored body (up to 4 KiB, may hold player data)")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092,kafka-3:9092"))
    a = p.parse_args(argv)

    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": a.bootstrap, "group.id": "dlq-peek", "enable.auto.commit": False,
                  "auto.offset.reset": "latest"})
    topic = "nv.dlq.v1"
    meta = c.list_topics(topic, timeout=10).topics[topic]
    since = int((time.time() - a.minutes * 60) * 1000)
    starts = c.offsets_for_times([TopicPartition(topic, pid, since) for pid in meta.partitions], timeout=10)
    ends = {pid: c.get_watermark_offsets(TopicPartition(topic, pid), timeout=10)[1] for pid in meta.partitions}
    todo = [tp for tp in starts if tp.offset >= 0 and tp.offset < ends[tp.partition]]
    rows = []
    if todo:
        c.assign(todo)
        left = {tp.partition for tp in todo}
        while left:
            m = c.poll(5)
            if m is None:
                break
            if m.error():
                continue
            if m.offset() + 1 >= ends[m.partition()]:
                left.discard(m.partition())
            rec = json.loads(m.value())
            if a.reason and rec.get("reason") != a.reason:
                continue
            rows.append((m.timestamp()[1], rec))
    c.close()
    rows.sort(key=lambda r: r[0])
    for _, rec in rows[-a.limit:]:
        out = {k: rec.get(k) for k in ("received_at", "reason", "endpoint", "errors", "raw_truncated")}
        if a.raw:
            out["raw"] = rec.get("raw")
        print(json.dumps(out, ensure_ascii=False))
    print(f"# {len(rows)} reject(s) in the last {a.minutes} min" + (f" with reason {a.reason}" if a.reason else ""))


if __name__ == "__main__":
    main()
