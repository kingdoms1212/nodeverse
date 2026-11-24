"""Durable export worker. Never resets existing 'stored' history or target data.

Throughput design (BN-5/BN-7 in docs/DATA_FLOW_AND_GAPS.md):
  - MySQL connections come from a DBCP-style pool (remote/sql.py) instead of one connect per event.
  - The cross-tool writer lock (GET_LOCK) is held once by a dedicated connection for the worker's lifetime.
  - Due outbox rows are claimed in batches and grouped by source session. Events of one session are
    processed in sequence order on one thread; different sessions run in parallel.
  - rt_* metrics are mirrored on their own thread, only rows changed since the last copy (remote/metrics.py).
A single lease still guarantees one worker process; parallelism is inside that process.
"""
from __future__ import annotations

import atexit
import json
import signal
import socket
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import timedelta
from pathlib import Path

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from common import settings
from common.db import local_mongo, remote_mongo
from common.timeutil import utc_now as now
from remote import sql as sqlmod
from remote.metrics import EPOCH, mirror_metrics  # noqa: F401  (re-exported for callers/tests)
from remote.projection import Conflict, Projector, Waiting

DUE_STATES = ["pending", "retrying", "waiting_context"]
PARKED = "parked"  # PL-9: retries stopped, row kept, revived automatically or by remote.requeue
PARKED_SENTINEL = object()  # process_job result when a row was parked
LEASE_SECONDS = 45

# kept for older imports
sql_connect = sqlmod.connect
WriterLock = sqlmod.WriterLock


def log(**fields):
    # Never log credentials/connection strings or raw DB error text.
    print(json.dumps({"component": "remote_sink", **fields}), flush=True)


def contexts():
    p = Path(settings.get().remote_context_path)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {"members": {}}


# ---------------------------------------------------------------- queue
def claim(local):
    """Claim one due row. Earliest due work wins; a low-sequence retry must not starve arrivals."""
    return local.remote_event_outbox.find_one_and_update(
        {"state": {"$in": DUE_STATES}, "next_attempt_at": {"$lte": now()}},
        {"$set": {"state": "processing", "updated_at": now(), "claim_id": uuid.uuid4().hex}, "$inc": {"attempts": 1}},
        sort=[("next_attempt_at", 1), ("queued_at", 1), ("sequence", 1), ("_id", 1)],
        return_document=ReturnDocument.AFTER)


def claim_batch(local, limit):
    """Claim up to `limit` due rows: find candidates, mark them with a fresh claim token in one update, then
    read back only the rows that carry the token. A row changed in between (an operator `park`, another
    worker) is not returned, so its projection never runs (reliability review P1-1)."""
    ids = [r["_id"] for r in local.remote_event_outbox.find(
        {"state": {"$in": DUE_STATES}, "next_attempt_at": {"$lte": now()}}, {"_id": 1},
        sort=[("next_attempt_at", 1), ("queued_at", 1), ("sequence", 1), ("_id", 1)], limit=limit)]
    if not ids:
        return []
    token = uuid.uuid4().hex
    local.remote_event_outbox.update_many({"_id": {"$in": ids}, "state": {"$in": DUE_STATES}},
                                          {"$set": {"state": "processing", "updated_at": now(), "claim_id": token},
                                           "$inc": {"attempts": 1}})
    return list(local.remote_event_outbox.find({"_id": {"$in": ids}, "state": "processing", "claim_id": token}))


def group_by_session(jobs):
    """Session events stay in sequence order on one thread; other jobs are independent groups."""
    groups = defaultdict(list)
    for job in jobs:
        groups[job.get("source_session_id") or job["_id"]].append(job)
    for g in groups.values():
        g.sort(key=lambda j: (j.get("sequence", 0), j.get("queued_at") or EPOCH, j["_id"]))
    return list(groups.values())


def recover_stale(local, first_run):
    """Rows left in 'processing' by a crash go back to the queue (all of them on start, old ones later)."""
    query = {"state": "processing"} if first_run else {"state": "processing", "updated_at": {"$lt": now() - timedelta(minutes=5)}}
    # dropping the claim token means a late write of the old worker no longer matches (P1-1)
    local.remote_event_outbox.update_many(query, {"$set": {"state": "pending", "next_attempt_at": now()},
                                                  "$unset": {"claim_id": ""}})


def health(local, state, **extra):
    local.remote_sink_health.update_one({"_id": "worker"}, {"$set": {"state": state, "updated_at": now(), **extra}}, upsert=True)


# ---------------------------------------------------------------- lease
def acquire(local, owner):
    try:
        doc = local.remote_worker_lease.find_one_and_update(
            {"_id": "writer", "$or": [{"owner": owner}, {"expires_at": {"$lte": now()}}]},
            {"$set": {"owner": owner, "expires_at": now() + timedelta(seconds=LEASE_SECONDS)}},
            upsert=True, return_document=ReturnDocument.AFTER)
        return doc.get("owner") == owner
    except DuplicateKeyError:
        return False


# ---------------------------------------------------------------- one event
def _needs_sql(event):
    return event.get("schema_version") == 2 and bool(event.get("session_id"))


class LostClaim(Exception):
    """The row is no longer ours (parked by an operator, reclaimed after a crash): its result is not written."""


def _owned(job) -> dict:
    """Filter for a row this worker still holds: same claim token and still 'processing'."""
    q = {"_id": job["_id"], "state": "processing"}
    if job.get("claim_id"):
        q["claim_id"] = job["claim_id"]
    return q


def _set(local, job, fields, unset=None):
    update = {"$set": fields, "$unset": {"claim_id": "", **(unset or {})}}
    if local.remote_event_outbox.update_one(_owned(job), update).matched_count != 1:
        log(task="lost_claim", event_id=str(job["_id"]), wanted=fields.get("state"))
        raise LostClaim(job["_id"])


def process_job(job, local, remote, pool, ctx, have_lock, cache=None):
    """Project one outbox row and record its terminal or retry state. Never raises.

    Returns None when the row reached a terminal state, PARKED_SENTINEL when it was parked,
    else the time it becomes due again."""
    try:
        return _process(job, local, remote, pool, ctx, have_lock, cache)
    except LostClaim:
        return None


class LazyConnection:
    """Takes a pooled MySQL connection only when the projection really queries MySQL (8th round). With the member
    cache most events never do, which saves the pool's ping on checkout and its rollback on return."""

    def __init__(self, pool):
        self._pool, self._conn = pool, None

    def _get(self):
        if self._conn is None:
            self._conn = self._pool.connection()
        return self._conn

    def cursor(self, *a, **k):
        return self._get().cursor(*a, **k)

    def commit(self):
        return self._get().commit()

    def rollback(self):
        if self._conn is not None:
            self._conn.rollback()

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def _process(job, local, remote, pool, ctx, have_lock, cache=None):
    archived = [bool(job.get("remote_archive_stored_at"))]
    try:
        event = job["event"]
        # The archive is written before the projection so a row that has to wait (member mapping, sequence gap)
        # is still archived. It is checked on every attempt (one find_one when present), so a replaced or
        # restored remote DB is refilled before the row can become 'stored' (P1-5). project() below does not
        # repeat it (it used to, costing a second find_one per event).
        Projector(local, remote, None, {}).archive(event)
        archived_at = job.get("remote_archive_stored_at") or now()  # recorded with the row's next state write
        archived[0] = True
        conn = LazyConnection(pool)
        try:
            if _needs_sql(event) and not have_lock:
                raise Waiting("other_database_writer_active")
            result = Projector(local, remote, conn, ctx, cache).project(event, archived=True)
        finally:
            conn.close()  # back to the pool, only if it was taken (member cache hit = no MySQL at all)
        state = "quarantined" if result.get("scope") == "learning_quarantine" else "stored"
        done = now()
        _set(local, job, {"state": state, "completed_at": done,
                          ("quarantined_at" if state == "quarantined" else "stored_at"): done,
                          "updated_at": done, "result": result, "archive_stored": True,
                          "remote_archive_stored_at": archived_at,
                          "queue_to_terminal_ms": max(0, int((done - job["queued_at"]).total_seconds() * 1000))},
             unset={"reason": "", "last_error": "", "waiting_since": "", "parked_at": ""})
        return None
    except LostClaim:
        return None
    except Waiting as e:
        reason = str(e)
        since = job.get("waiting_since") or now()
        limit = wait_limit(reason)
        if limit is not None and now() - since >= limit:
            _park(local, job, reason, archive_stored=archived[0], waiting_since=since)
            return PARKED_SENTINEL
        # a sequence gap is usually closed within a second by a predecessor staged in the next Silver batch (and
        # that predecessor wakes this row anyway); other waits keep the slower 5-60 s backoff
        base = 1 if reason.startswith("sequence_gap") else 5
        due = now() + timedelta(seconds=min(60, base * 2 ** min(job["attempts"] - 1, 4)))
        _set(local, job, {"state": "waiting_context", "reason": reason, "archive_stored": archived[0], "updated_at": now(),
                          "next_attempt_at": due, "waiting_since": since})
        return due
    except (Conflict, ValueError, KeyError, TypeError) as e:
        _set(local, job, {"state": "blocked", "reason": str(e)[:200], "updated_at": now()})
        return None
    except Exception as e:
        try:
            # P1-4: the budget counts attempts since the last manual requeue; `attempts` keeps the full history
            if job["attempts"] - job.get("retry_base", 0) >= settings.get().remote_max_retries:
                _park(local, job, "retry_limit", last_error=type(e).__name__)
                return PARKED_SENTINEL
            due = now() + timedelta(seconds=min(60, 2 ** min(job["attempts"] - job.get("retry_base", 0), 5)))
            _set(local, job, {"state": "retrying", "last_error": type(e).__name__, "updated_at": now(),
                              "next_attempt_at": due})
            return due
        except LostClaim:
            return None


# ---------------------------------------------------------------- PL-9: parked rows


def wait_limit(reason: str):
    """How long a waiting row keeps retrying before it is parked (None = no limit)."""
    s = settings.get()
    if reason.startswith("sequence_gap"):
        return timedelta(minutes=s.remote_wait_gap_minutes)
    if reason == "other_database_writer_active":  # a tool holds the writer lock: transient, never parked
        return None
    # member_mapping_required, member_is_not_live and any future reason: bounded, then parked and kept (P1-3)
    return timedelta(hours=s.remote_wait_member_hours)


def _park(local, job, reason, **fields):
    _set(local, job, {"state": PARKED, "reason": reason, "parked_at": now(), "updated_at": now(), **fields},
         unset={"next_attempt_at": ""})


def process_group(jobs, local, *args):
    """Process one session's rows in order. When a row must wait, the later rows of the same session go back
    to the queue with the same due time instead of each failing on a sequence gap with its own growing
    backoff, so the whole session is retried together and in order."""
    cache = {}  # 8th round: shared by this session's events in this batch (one thread, sequence order)
    for i, job in enumerate(jobs):
        due = process_job(job, local, *args, cache=cache)
        if due is PARKED_SENTINEL:
            due = now() + timedelta(seconds=60)
        if due is not None and i + 1 < len(jobs):
            for later in jobs[i + 1:]:
                local.remote_event_outbox.update_one(
                    _owned(later),
                    {"$set": {"state": "pending", "next_attempt_at": due, "updated_at": now()},
                     "$unset": {"claim_id": ""}, "$inc": {"attempts": -1}})  # these rows were not attempted
            return


# ---------------------------------------------------------------- worker process
class Worker:
    def __init__(self):
        s = settings.get()
        s.require_remote()
        self.batch = s.remote_batch
        conns = s.remote_workers * 2 + 4
        self.local = local_mongo(conns)
        self.remote = remote_mongo(conns)
        self.pool = sqlmod.pool(s.sql_pool_size)
        self.lock = sqlmod.WriterLock()
        self.executor = ThreadPoolExecutor(max_workers=s.remote_workers, thread_name_prefix="sink")
        self.owner = socket.gethostname() + ":" + str(uuid.uuid4())
        self.stop = threading.Event()
        self.leased = threading.Event()
        log(workers=s.remote_workers, batch=s.remote_batch, sql_pool=s.sql_pool_size)

    # background threads
    def _heartbeat(self):
        while not self.stop.wait(10):
            try:
                self.local.remote_worker_lease.update_one({"_id": "writer", "owner": self.owner},
                                                          {"$set": {"expires_at": now() + timedelta(seconds=LEASE_SECONDS)}})
            except Exception:
                pass

    def _metrics(self):
        marks = None
        while not self.stop.wait(5):
            if not self.leased.is_set():
                continue
            try:
                marks = mirror_metrics(self.local, self.remote, marks)
            except Exception as e:
                log(task="mirror_metrics", error=type(e).__name__)

    def _status(self):
        """PL-9: publish outbox counts (local and dashboard DB) so the server live page can show them."""
        from remote.requeue import reason_counts, state_counts
        reasons, reasons_at = {}, 0.0
        while not self.stop.wait(10):
            if not self.leased.is_set():
                continue
            try:
                states = state_counts(self.local)
                if time.monotonic() - reasons_at >= 60:  # reasons change slowly; counts stay at 10 s
                    reasons, reasons_at = reason_counts(self.local), time.monotonic()
                doc = {"_id": "remote_writer", "states": states, "reasons": reasons, "updated_at": now()}
                for db in (self.local, self.remote):
                    db.pipeline_sink_status.replace_one({"_id": doc["_id"]}, doc, upsert=True)
            except Exception as e:
                log(task="status", error=type(e).__name__)

    def _revive(self):
        """PL-9: parked 'member_mapping_required' rows go back to the queue when a member may now exist
        (new game_members row or a changed mapping file)."""
        seen = None
        while not self.stop.wait(60):
            if not self.leased.is_set():
                continue
            try:
                path = Path(settings.get().remote_context_path)
                mtime = path.stat().st_mtime if path.exists() else 0
                conn = self.pool.connection()
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT COUNT(*), MAX(registered_at) FROM game_members")
                        members = cur.fetchone()
                finally:
                    conn.close()
                marker = (mtime, members[0], str(members[1]))
                if seen is not None and marker != seen:
                    from remote.requeue import requeue
                    n = requeue(self.local, "member_mapping_required", None) + requeue(self.local, "member_is_not_live", None)
                    if n:
                        log(task="revive", requeued=n)
                seen = marker
            except Exception as e:
                log(task="revive", error=type(e).__name__)

    def _ensure_indexes(self):
        self.local.remote_event_outbox.create_index([("state", 1), ("next_attempt_at", 1)])
        self.local.remote_event_outbox.create_index([("state", 1), ("reason", 1)])
        # wake_successor looks up later rows of one session after every stored event
        self.local.remote_event_outbox.create_index([("source_session_id", 1), ("sequence", 1)])
        days = settings.get().remote_done_ttl_days
        if days > 0:  # PL-10: finished rows are removed after N days; parked/blocked/waiting rows are kept
            try:
                self.local.remote_event_outbox.create_index(
                    [("completed_at", 1)], name="done_ttl", expireAfterSeconds=days * 86400,
                    partialFilterExpression={"state": {"$in": ["stored", "quarantined"]}})
            except Exception as e:  # an index with other options already exists: keep it
                log(task="ttl_index", error=type(e).__name__)

    def release(self):
        self.stop.set()
        self.lock.release()
        try:
            self.local.remote_worker_lease.delete_one({"_id": "writer", "owner": self.owner})
        except Exception:
            pass

    def run_once(self, first_run=False) -> bool:
        """One claim/process round. Returns False when there was nothing to do."""
        recover_stale(self.local, first_run)
        jobs = claim_batch(self.local, self.batch)
        if not jobs:
            health(self.local, "ready")
            return False
        health(self.local, "processing")
        have_lock = self.lock.ensure() if any(_needs_sql(j["event"]) for j in jobs) else True
        ctx = contexts()
        futures = [self.executor.submit(process_group, g, self.local, self.remote, self.pool, ctx, have_lock)
                   for g in group_by_session(jobs)]
        wait(futures)
        for f in futures:
            if f.exception() is not None:  # process_job never raises; keep the loop alive regardless
                log(error=type(f.exception()).__name__)
        return True

    def run(self):
        self._ensure_indexes()
        atexit.register(self.release)
        signal.signal(signal.SIGTERM, _exit_on_signal)
        threading.Thread(target=self._heartbeat, daemon=True).start()
        threading.Thread(target=self._metrics, daemon=True, name="metrics").start()
        threading.Thread(target=self._status, daemon=True, name="status").start()
        threading.Thread(target=self._revive, daemon=True, name="revive").start()
        first = True
        while True:
            try:
                if not acquire(self.local, self.owner):
                    self.leased.clear()
                    time.sleep(2)
                    continue
                self.leased.set()
                busy = self.run_once(first)
                first = False
                if not busy:
                    time.sleep(0.5)
            except Exception as e:
                try:
                    health(self.local, "error", error_type=type(e).__name__)
                except Exception:
                    pass
                log(error=type(e).__name__)
                time.sleep(3)


def _exit_on_signal(signum, frame):
    raise SystemExit(0)  # runs atexit -> lease and writer lock are released


def main():
    Worker().run()


if __name__ == "__main__":
    main()
