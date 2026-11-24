"""NODEVERSE ingestion API (P1, P4-2).

POST /v2/events  single event, header X-API-Key, 202 = persisted in Kafka.
  - learning (8 types)  -> nv.learning.v2  key session_id
  - update   (9 types)  -> nv.update.v1    key player_id
  - runtime  (run_*/exit_*, launcher format) -> nv.runtime.v1 key run_id
  - anything that fails validation -> 422 and one record in nv.dlq.v1
POST /v1/events  kept for the existing launcher: runtime events only (same routing).
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import threading
from datetime import datetime, timezone
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import ValidationError

import contract
from common import settings, timeutil

log = logging.getLogger("collector")

MAX_BODY_BYTES = 16 * 1024
DLQ_RAW_BYTES = 4096

app = FastAPI(title="NODEVERSE ingestion API")
_producer = None
_lock = threading.Lock()


def producer():
    global _producer
    with _lock:
        if _producer is None:
            from confluent_kafka import Producer
            _producer = Producer({
                "bootstrap.servers": settings.get().kafka_bootstrap,
                "enable.idempotence": True, "acks": "all",
                "delivery.timeout.ms": int(settings.get().kafka_delivery_timeout_s * 1000) - 500, "linger.ms": 5,
            })
            # one poller serves delivery callbacks for all concurrent requests
            threading.Thread(target=_poll_forever, args=(_producer,), daemon=True).start()
        return _producer


def _poll_forever(p):
    # A dead poller would silently turn every request into a 12 s timeout (PL-4): never let it exit.
    while True:
        try:
            p.poll(0.05)
        except Exception:
            log.exception("kafka poll failed; continuing")


def send(topic: str, key: str | None, value: dict, timeout: float | None = None) -> None:
    """Produce one record and block until the broker acknowledges it (acks=all). Raises on failure."""
    p = producer()
    # PL-3/BN-9: a request thread waits at most KAFKA_DELIVERY_TIMEOUT_S (default 5 s, was 12 s) before the
    # client gets 503 and resends the same event_id; a late delivery is then removed by the Silver dedup.
    timeout = settings.get().kafka_delivery_timeout_s if timeout is None else timeout
    done = threading.Event()
    result: list = []

    def on_delivery(err, msg):
        result.append(err)
        done.set()

    p.produce(topic, key=key, value=json.dumps(value, separators=(",", ":")), on_delivery=on_delivery)
    if not done.wait(timeout) or result[0] is not None:
        raise RuntimeError(f"Kafka delivery failed: {result[0] if result else 'timeout'}")


def send_dlq(raw: bytes, reason: str, errors: list, endpoint: str, key: str | None) -> bool:
    record = {
        "reason": reason,
        "errors": errors[:20],
        "endpoint": endpoint,
        "received_at": now_iso(),
        "raw_truncated": len(raw) > DLQ_RAW_BYTES,
        "raw": raw[:DLQ_RAW_BYTES].decode("utf-8", errors="replace"),
    }
    try:
        send(contract.TOPIC_DLQ, key, record, timeout=5.0)
        return True
    except Exception:
        return False


def now_iso() -> str:
    return timeutil.iso_ms()


def check_key(x_api_key: str) -> None:
    expected = os.getenv("INGEST_KEY", "")
    if len(expected) < 24 or not secrets.compare_digest(x_api_key.encode(), expected.encode()):
        raise HTTPException(401, "Invalid ingestion key")


def _short_errors(exc: ValidationError) -> list:
    return [{"loc": ".".join(str(p) for p in e["loc"]), "msg": e["msg"]} for e in exc.errors()]


async def reject(raw: bytes, reason: str, errors: list, endpoint: str, key: str | None):
    stored = await run_in_threadpool(send_dlq, raw, reason, errors, endpoint, key)
    return JSONResponse(status_code=422, content={"detail": errors, "reason": reason, "dlq": stored})


async def _ingest(request: Request, endpoint: str, allowed_types: tuple | None):
    raw = await request.body()
    if len(raw) > MAX_BODY_BYTES:
        raise HTTPException(413, "Body exceeds 16KiB")
    try:
        body = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        return await reject(raw, "invalid_json", [{"loc": "", "msg": str(exc)[:200]}], endpoint, None)
    if not isinstance(body, dict):
        return await reject(raw, "invalid_json", [{"loc": "", "msg": "body must be a JSON object"}], endpoint, None)

    dlq_key = str(body.get("event_id")) if body.get("event_id") is not None else None
    event_type = body.get("event_type")
    routed = contract.route(event_type)
    if routed is None or (allowed_types is not None and event_type not in allowed_types):
        return await reject(raw, "unknown_event_type", [{"loc": "event_type", "msg": f"unsupported: {event_type!r}"}],
                      endpoint, dlq_key)
    model_cls, topic, key_field = routed
    try:
        event = model_cls.model_validate(body)
    except ValidationError as exc:
        return await reject(raw, "schema_error", _short_errors(exc), endpoint, dlq_key)

    now = datetime.now(timezone.utc)
    if contract.is_too_old(event.occurred_at, now):
        return await reject(raw, "too_old", [{"loc": "occurred_at",
                                              "msg": "occurred_at is more than 6 days old; do not resend"}],
                            endpoint, dlq_key)
    record = contract.to_record(event, now)
    try:
        await run_in_threadpool(send, topic, str(record[key_field]), record)
    except Exception:
        raise HTTPException(503, "Kafka unavailable; retry the same event_id and body")
    return JSONResponse(status_code=202, content={"event_id": record["event_id"], "status": "accepted", "topic": topic})


@app.get("/health/live")
def live():
    return {"status": "ok"}


@app.get("/health/ready")
def ready():
    """Kafka ingest readiness: every contract topic has a leader per partition and ISR >= min.insync.replicas,
    so an acks=all write can succeed. One broker down can still be ready (see /health/deps for brokers).
    MongoDB is not part of it: the collector keeps accepting (202) while MongoDB is down."""
    from common import health
    r = health.kafka_ingest(settings.get().kafka_bootstrap, timeout_s=3)
    if not r["ready"]:
        raise HTTPException(503, {"status": "kafka_not_ready", **r})
    return {"status": "ready", "brokers": r["brokers"]}


@app.get("/health/deps")
def deps():
    """Diagnosis: each broker answering by itself, ingest readiness, MongoDB ping (independent answers)."""
    from common import health
    return health.report()


@app.post("/v2/events", status_code=202)
async def ingest_v2(request: Request, x_api_key: str = Header(default="")):
    check_key(x_api_key)
    return await _ingest(request, "/v2/events", None)


@app.post("/v1/events", status_code=202)
async def ingest_v1(request: Request, x_api_key: str = Header(default="")):
    """Kept until the transition ends: existing launcher runtime events only."""
    check_key(x_api_key)
    return await _ingest(request, "/v1/events", contract.RUNTIME_TYPES)


@app.get("/v2/events/{event_id}")
def remote_delivery(event_id: UUID, x_api_key: str = Header(default="")):
    check_key(x_api_key)
    from common.db import local_mongo
    try:
        row = _delivery_row(local_mongo(), event_id)
    except Exception:
        raise HTTPException(503, "delivery status store unavailable; retry later")  # PL-6: was an unhandled 500
    if not row:
        return {"event_id": str(event_id), "state": "not_staged", "meaning": "awaiting Silver/export; not a delivery confirmation"}
    row["event_id"] = row.pop("_id")
    return row


def _delivery_row(db, event_id: UUID):
    return db.remote_event_outbox.find_one({"_id": str(event_id)}, {
        "state": 1, "reason": 1, "attempts": 1, "updated_at": 1, "archive_stored": 1, "result": 1,
        "queued_at": 1, "stored_at": 1, "completed_at": 1, "quarantined_at": 1,
        "remote_archive_stored_at": 1, "queue_to_terminal_ms": 1})
