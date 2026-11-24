"""Protocol-level health checks with the clients already in the Python image (reliability review, protocol
healthcheck request). No JVM CLI and no mongosh: confluent-kafka metadata requests and a PyMongo ping.

  python -m common.health kafka-broker kafka-1:9092   # this broker answers a metadata request itself
  python -m common.health kafka-ingest                # every required topic partition has a leader and ISR >= 2
  python -m common.health mongo                       # MongoDB answers {ping: 1} with ok: 1
  python -m common.health topics                      # create missing topics (AdminClient), then verify
  python -m common.health wait mongo kafka-ingest     # retry until all named checks pass (start-up gate)
  python -m common.health report                      # everything above as JSON (diagnosis)

Broker health and ingest readiness are different answers: with one broker down every partition can still
have a leader and min.insync.replicas=2, so ingest stays ready while that broker is reported down.
MongoDB is judged on its own; it never decides Kafka ingest readiness.
"""
from __future__ import annotations

import json
import os
import sys
import time

DEFAULT_BOOTSTRAP = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
MIN_ISR = 2
REPLICATION = 3


def _topics() -> dict:
    try:
        import topics  # pipeline/topics.py (collector image: /app/topics.py)
    except ImportError:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        import topics
    return topics


def _admin(bootstrap: str, timeout_s: float):
    from confluent_kafka.admin import AdminClient
    ms = str(int(timeout_s * 1000))
    return AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": ms,
                        "socket.connection.setup.timeout.ms": ms, "log_level": 0})


def kafka_broker(address: str, timeout_s: float = 3.0) -> dict:
    """Ask exactly one broker for metadata. The answer must come from that broker (orig_broker_name), so a
    healthy neighbour can never make a dead broker look healthy."""
    started = time.monotonic()
    try:
        md = _admin(address, timeout_s).list_topics(timeout=timeout_s)
        responder = getattr(md, "orig_broker_name", "") or ""
        host = address.split(":")[0]
        ok = responder.split(":")[0].split("/")[0] == host
        return {"broker": address, "ok": ok, "ms": round((time.monotonic() - started) * 1000, 1),
                "responder": responder, "broker_id": getattr(md, "orig_broker_id", None),
                **({} if ok else {"error": "answered_by_other_broker"})}
    except Exception as e:
        return {"broker": address, "ok": False, "ms": round((time.monotonic() - started) * 1000, 1),
                "error": type(e).__name__}


def kafka_ingest(bootstrap: str = None, timeout_s: float = 5.0) -> dict:
    """Can the collector's writes (acks=all, min ISR 2) succeed? Every required topic exists with its
    partitions, each partition has a leader and at least MIN_ISR in-sync replicas."""
    bootstrap = bootstrap or os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP)
    started = time.monotonic()
    try:
        md = _admin(bootstrap, timeout_s).list_topics(timeout=timeout_s)
    except Exception as e:
        return {"ready": False, "error": type(e).__name__, "ms": round((time.monotonic() - started) * 1000, 1)}
    problems, detail = [], {}
    for name, partitions in _topics().TOPICS.items():
        t = md.topics.get(name)
        if t is None or t.error is not None:
            problems.append(f"{name}:missing")
            continue
        leaders = sum(1 for p in t.partitions.values() if p.leader >= 0)
        min_isr = min((len(p.isrs) for p in t.partitions.values()), default=0)
        detail[name] = {"partitions": len(t.partitions), "with_leader": leaders, "min_isr": min_isr}
        if len(t.partitions) < partitions:
            problems.append(f"{name}:partitions_{len(t.partitions)}")
        if leaders < len(t.partitions):
            problems.append(f"{name}:no_leader")
        if min_isr < MIN_ISR:
            problems.append(f"{name}:isr_{min_isr}")
    return {"ready": not problems, "problems": problems, "brokers": sorted(b.id for b in md.brokers.values()),
            "topics": detail, "ms": round((time.monotonic() - started) * 1000, 1)}


def mongo(uri: str = None, timeout_s: float = 3.0) -> dict:
    """PyMongo {ping: 1}; a listening but frozen mongod fails within timeout_s."""
    from pymongo import MongoClient
    uri = uri or os.getenv("MONGO_URI", "mongodb://mongo:27017")
    started = time.monotonic()
    ms = int(timeout_s * 1000)
    client = MongoClient(uri, serverSelectionTimeoutMS=ms, connectTimeoutMS=ms, socketTimeoutMS=ms)
    try:
        ok = client.admin.command("ping").get("ok") == 1
        return {"ok": ok, "ms": round((time.monotonic() - started) * 1000, 1)}
    except Exception as e:
        return {"ok": False, "error": type(e).__name__, "ms": round((time.monotonic() - started) * 1000, 1)}
    finally:
        client.close()


def create_topics(bootstrap: str = None, timeout_s: float = 15.0) -> dict:
    """topic-init without the JVM CLI: create missing topics with the contract settings, keep existing ones,
    then verify them with kafka_ingest()."""
    from confluent_kafka.admin import NewTopic
    from confluent_kafka import KafkaException
    t = _topics()
    bootstrap = bootstrap or os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP)
    admin = _admin(bootstrap, timeout_s)
    existing = set(admin.list_topics(timeout=timeout_s).topics)
    wanted = [NewTopic(name, num_partitions=p, replication_factor=REPLICATION,
                       config={"min.insync.replicas": str(MIN_ISR), "retention.ms": str(t.RETENTION_MS)})
              for name, p in t.TOPICS.items() if name not in existing]
    created = []
    for name, fut in (admin.create_topics(wanted, request_timeout=timeout_s).items() if wanted else []):
        try:
            fut.result(timeout=timeout_s)
            created.append(name)
        except KafkaException as e:
            if e.args and getattr(e.args[0], "code", lambda: None)() == 36:  # TOPIC_ALREADY_EXISTS (race)
                continue
            raise
    return {"created": created, "existing": sorted(existing & set(t.TOPICS)), **kafka_ingest(bootstrap)}


CHECKS = {
    "mongo": lambda: mongo()["ok"],
    "kafka-ingest": lambda: kafka_ingest()["ready"],
    "topics": lambda: create_topics()["ready"],
}


def report() -> dict:
    brokers = os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP).split(",")
    return {"kafka_brokers": [kafka_broker(b) for b in brokers], "kafka_ingest": kafka_ingest(), "mongo": mongo()}


def wait(names: list[str], attempts: int, delay_s: float) -> bool:
    for attempt in range(1, attempts + 1):
        failed = []
        for name in names:
            try:
                if not CHECKS[name]():
                    failed.append(name)
            except Exception as e:
                failed.append(f"{name}({type(e).__name__})")
        if not failed:
            print(json.dumps({"ready": names, "attempt": attempt}), flush=True)
            return True
        print(json.dumps({"waiting_for": failed, "attempt": attempt, "of": attempts}), flush=True)
        time.sleep(delay_s)
    return False


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        print(__doc__)
        return 2
    cmd, rest = argv[0], argv[1:]
    if cmd == "kafka-broker":
        r = kafka_broker(rest[0])
        print(json.dumps(r))
        return 0 if r["ok"] else 1
    if cmd == "kafka-ingest":
        r = kafka_ingest()
        print(json.dumps(r))
        return 0 if r["ready"] else 1
    if cmd == "mongo":
        r = mongo()
        print(json.dumps(r))
        return 0 if r["ok"] else 1
    if cmd == "topics":
        return 0 if wait(["topics"], int(os.getenv("HEALTH_ATTEMPTS", "36")), 5) else 1
    if cmd == "wait":
        return 0 if wait(rest, int(os.getenv("HEALTH_ATTEMPTS", "60")), 3) else 1
    if cmd == "report":
        print(json.dumps(report(), indent=2))
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
