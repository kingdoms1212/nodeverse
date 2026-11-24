"""Durable, idempotent export staging after Silver validation."""
from datetime import datetime, timezone, date

from common.db import local_mongo
from common.timeutil import iso_ms


def local_db():
    """Kept for callers of the old name; the client is shared per process (common.db)."""
    return local_mongo()


def plain(value):
    if isinstance(value, datetime):
        return iso_ms(value)  # Spark hands over naive UTC datetimes
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [plain(v) for v in value]
    return value


def stage(records, db=None, chunk=500, queued_at=None):
    """Insert-once outbox rows for validated events, in bulk (BN-8: was 2 round trips per event).

    Existing rows are never reset. A different payload for a known event_id is recorded as a conflict."""
    from pymongo import UpdateOne
    db = db if db is not None else local_db()
    # 8th round: one timestamp for the whole Silver batch. Chunks used to get their own time, so a session's
    # sequence 3 (first chunk) could sort before its sequence 2 (second chunk, a few ms later); the worker then
    # claimed 3 without 2 and hit a sequence gap. With equal times the claim order falls back to sequence.
    now = queued_at if queued_at is not None else datetime.now(timezone.utc)
    buf = []
    for record in records:
        buf.append(plain(record))
        if len(buf) >= chunk:
            _stage_chunk(db, buf, UpdateOne, now)
            buf = []
    if buf:
        _stage_chunk(db, buf, UpdateOne, now)


def _stage_chunk(db, bodies, UpdateOne, now=None):
    seen = {}
    for body in bodies:
        seen.setdefault(body["event_id"], body)  # first copy wins inside one batch
    known = {d["_id"]: d.get("payload_hash") for d in
             db.remote_event_outbox.find({"_id": {"$in": list(seen)}}, {"payload_hash": 1})}
    now = now or datetime.now(timezone.utc)
    ops, conflicts = [], []
    for ident, body in seen.items():
        payload_hash = body["payload_hash"]
        if ident in known and known[ident] != payload_hash:
            conflicts.append(UpdateOne({"_id": ident + ":" + payload_hash}, {"$setOnInsert": {
                "event_id": ident, "reason": "event_id_conflict", "detected_at": now}}, upsert=True))
            continue
        ops.append(UpdateOne({"_id": ident}, {"$setOnInsert": {
            "payload_hash": payload_hash, "event": body, "state": "pending", "attempts": 0,
            "queued_at": now, "next_attempt_at": now, "sequence": body.get("sequence", 0),
            "source_session_id": body.get("session_id"), "occurred_at": body["occurred_at"]}}, upsert=True))
    if ops:
        db.remote_event_outbox.bulk_write(ops, ordered=False)
    if conflicts:
        db.remote_export_conflicts.bulk_write(conflicts, ordered=False)
