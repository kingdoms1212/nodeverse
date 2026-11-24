"""Explicit v2 -> dashboard relation projection. Unknown identity/context stays pending."""
import hashlib
import json
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from pymongo.errors import DuplicateKeyError

NS = uuid.UUID("3d049a87-e314-4f9f-9ea2-97cb6f4c4438")
CATALOG = json.loads(Path(__file__).with_name("catalog.json").read_text(encoding="utf-8"))["games"]

class Waiting(Exception):
    pass
class Conflict(Exception):
    pass

def uid(*parts):
    return str(uuid.uuid5(NS, ":".join(str(x) for x in parts)))

def dt(value):
    if isinstance(value, datetime):
        value = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    else:
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if value.tzinfo is None:
            raise Conflict("timestamp_needs_timezone")
    value = value.astimezone(timezone.utc)
    return value.replace(microsecond=value.microsecond // 1000 * 1000)

def ensure(collection, doc, immutable):
    """Insert immutable identity fields; never overwrite existing content on replay.

    8th round: insert first and read only when the id already exists. New documents (almost every event) cost one
    round trip instead of two; a replay still compares the immutable fields and raises Conflict on a difference."""
    try:
        collection.insert_one(doc)
        return doc
    except DuplicateKeyError:
        old = collection.find_one({"_id": doc["_id"]})
    if old is None or any(old.get(k) != doc.get(k) for k in immutable):
        raise Conflict("id_content_conflict:" + collection.name)
    return old

_member_cache: dict = {}  # (player, external key) -> (member uuid, expires at monotonic)
MEMBER_CACHE_MAX = 50_000  # entries; ~100 bytes each
_member_lock = threading.Lock()


def resolve_member(sql, player, contexts):
    """Cached front of _resolve_member (8th round). Only a member that exists and is 'live' is cached, so a missing
    mapping or a non-live member is checked again on every attempt; a changed row is picked up within the TTL."""
    from common import settings
    ttl = settings.get().remote_member_cache_seconds  # REMOTE_MEMBER_CACHE_SECONDS, default 300
    if ttl <= 0:
        return _resolve_member(sql, player, contexts)
    binding = contexts.get("members", {}).get(player)
    key = (player, binding["external_member_key"] if binding else None)
    now_m = time.monotonic()
    with _member_lock:
        hit = _member_cache.get(key)
        if hit and hit[1] > now_m:
            return hit[0]
    member = _resolve_member(sql, player, contexts)
    with _member_lock:
        if len(_member_cache) >= MEMBER_CACHE_MAX:  # follow-up review #3: bounded memory
            for k in [k for k, v in _member_cache.items() if v[1] <= now_m]:
                del _member_cache[k]  # expired first
            while len(_member_cache) >= MEMBER_CACHE_MAX:
                del _member_cache[next(iter(_member_cache))]  # then oldest inserted
        _member_cache[key] = (member, now_m + ttl)
    return member


def _resolve_member(sql, player, contexts):
    """Use real member UUID or exact external key. Provision only trusted configured contexts."""
    try:
        member_hex = uuid.UUID(player).hex
    except (ValueError, TypeError, AttributeError):
        member_hex = ""
    binding = contexts.get("members", {}).get(player)
    external = binding["external_member_key"] if binding else "game:member:" + player
    if not external.startswith("game:member:") or len(external) > 150:
        raise Conflict("invalid_external_member_key")
    with sql.cursor() as cur:
        cur.execute("SELECT member_id, external_member_key, data_source FROM game_members WHERE member_id=%s OR external_member_key=%s FOR UPDATE", (member_hex, external))
        found = cur.fetchall()
        if len(found) > 1:
            raise Conflict("ambiguous_member_mapping")
        if found:
            member = found[0]
            if member[0].lower() != member_hex.lower() and member[1] != external:
                raise Conflict("external_key_case_conflict")
            if member[2] != "live":
                raise Waiting("member_is_not_live")
            if binding and member[1] != external:
                raise Conflict("binding_member_mismatch")
            sql.commit()
            return str(uuid.UUID(member[0]))
        if not binding or not binding.get("verified") or not binding.get("create_if_missing"):
            sql.rollback()
            raise Waiting("member_mapping_required")
        nickname = binding.get("nickname", "")
        if not nickname.strip() or len(nickname) > 80:
            raise Conflict("verified_nickname_required")
        ident = uuid.UUID(uid("member", external))
        cur.execute("INSERT INTO game_members(member_id,external_member_key,nickname,data_source,registered_at) VALUES(%s,%s,%s,%s,%s)", (ident.hex, external, nickname, "live", datetime.now(timezone.utc).replace(tzinfo=None)))
        sql.commit()
        return str(ident)

_problem_ids: dict = {}  # (task_id, content_version) -> problem _id; definitions never change once written


class Projector:
    def __init__(self, local, remote, sql, contexts, cache=None):
        """cache (8th round): a dict shared by the events of one session group in one batch. One thread processes a
        group in sequence order, so the cursor, run context, run and attempt documents it already read or wrote can
        be reused instead of being read again for every event. Only states confirmed by the database go in."""
        self.local, self.remote, self.sql, self.contexts = local, remote, sql, contexts
        self.cache = cache if cache is not None else {}

    def _cached(self, key, load):
        if key in self.cache:
            return self.cache[key]
        value = load()
        if value is not None:
            self.cache[key] = value
        return value

    @staticmethod
    def kind(event):
        return "learning" if "session_id" in event and event.get("schema_version") == 2 else "runtime" if event.get("schema_version") == 1 else "update"

    def archive(self, event):
        kind = self.kind(event)
        doc = dict(event, _id=event["event_id"], projection_version=1)
        for key in ["occurred_at", "received_at", "event_time"]:
            if doc.get(key):
                doc[key] = dt(doc[key])
        ensure(self.remote["pipeline_" + kind + "_events"], doc, ["payload_hash"])
        return kind

    def receipt(self, event, result):
        ensure(self.local.remote_projection_receipts, {"_id": event["event_id"], "payload_hash": event["payload_hash"], "result": result}, ["payload_hash"])
        self.local.remote_session_cursors.update_one({"_id": event["session_id"]}, {"$set": {"last_sequence": event["sequence"], "last_event_id": event["event_id"]}}, upsert=True)
        self.cache[("cursor", event["session_id"])] = {"last_sequence": event["sequence"]}
        self.wake_successor(event)
        return result

    def wake_successor(self, event):
        """Wake the next sequence of this session. Called after a receipt and again when an already projected
        event is replayed, so a failure between the cursor update and this wake is repaired by the retry of the
        predecessor (reliability review P1-2)."""
        # 8th round: one probe instead of two updates per event; almost always nothing is waiting.
        # Later rows of the session can wait in two ways: the first one that hit the gap is waiting_context, and
        # the rest of its group were put back as pending with the same future due time (worker.process_group).
        # Both are woken now; waking only sequence+1 let the session advance one row per backoff interval
        # (44 % of rows were retried in the 9,000-event drain test).
        now_ = datetime.now(timezone.utc)
        sid, seq = event["session_id"], event["sequence"]
        later = {"source_session_id": sid, "sequence": {"$gt": seq}}
        nxt = self.local.remote_event_outbox.find_one(
            {**later, "$or": [{"state": {"$in": ["waiting_context", "parked"]}},
                              {"state": "pending", "next_attempt_at": {"$gt": now_}}]}, {"_id": 1})
        if nxt is None:
            return
        # A newly available predecessor removes this wait immediately; do not make
        # a valid next event sleep through an old exponential retry interval.
        self.local.remote_event_outbox.update_many(
            {**later, "$or": [{"state": "waiting_context", "reason": {"$regex": "^sequence_gap:"}},
                              {"state": "pending", "next_attempt_at": {"$gt": now_}}]},
            {"$set": {"next_attempt_at": now_}})
        # PL-9: a successor parked after the gap deadline comes back as soon as its predecessor is stored.
        self.local.remote_event_outbox.update_many({"source_session_id": event["session_id"],
            "sequence": event["sequence"] + 1, "state": "parked",
            "reason": {"$regex": "^sequence_gap:"}},
            {"$set": {"state": "pending", "next_attempt_at": datetime.now(timezone.utc)},
             "$unset": {"parked_at": "", "waiting_since": ""}})

    def quarantine(self, event, reason):
        # Only exclude a verified, present, contiguous source sequence. Never skip a gap.
        doc = {"_id": event["event_id"], "payload_hash": event["payload_hash"], "reason": reason,
               "source_session_id": event["session_id"], "sequence": event["sequence"],
               "quarantined_at": datetime.now(timezone.utc), "event": event}
        ensure(self.remote.pipeline_quarantined_events, doc, ["payload_hash", "reason"])
        return self.receipt(event, {"scope": "learning_quarantine", "event_id": event["event_id"], "reason": reason})

    def project(self, event, archived=False):
        """archived=True: the caller already wrote (and conflict-checked) the archive copy of this event."""
        kind = self.kind(event) if archived else self.archive(event)
        if kind != "learning":
            return {"scope": kind + "_archive", "event_id": event["event_id"]}
        player = resolve_member(self.sql, event["player_id"], self.contexts)
        source_session = str(uuid.UUID(event["session_id"]))
        source_run = str(uuid.UUID(event["run_id"]))
        sequence = event["sequence"]
        seqkey = source_session + ":" + str(sequence)
        ensure(self.local.remote_sequence_registry, {"_id": seqkey, "event_id": event["event_id"], "payload_hash": event["payload_hash"]}, ["event_id", "payload_hash"])
        cursor = (self.cache.get(("cursor", event["session_id"]))
                  or self.local.remote_session_cursors.find_one({"_id": source_session}) or {"last_sequence": 0})
        if sequence > cursor["last_sequence"] + 1:
            raise Waiting("sequence_gap:expected_" + str(cursor["last_sequence"] + 1))
        if sequence <= cursor["last_sequence"]:
            receipt = self.local.remote_projection_receipts.find_one({"_id": event["event_id"]})
            if not receipt or receipt["payload_hash"] != event["payload_hash"]:
                raise Conflict("sequence_reuse_or_missing_receipt")
            if sequence == cursor["last_sequence"]:  # the newest receipt: its wake may not have happened
                self.wake_successor(event)
            return receipt["result"]
        game_id = event["minigame_id"]
        game = CATALOG.get(game_id)
        if game is None or game["category"] != event["category"]:
            raise Conflict("catalog_mismatch")
        occurred = dt(event["occurred_at"])
        details = event.get("details", {})
        et = event["event_type"]
        contextkey = source_session + ":" + source_run
        context = self._cached(("ctx", contextkey), lambda: self.local.remote_run_contexts.find_one({"_id": contextkey}))
        session_id = uid("session", source_session, player, game_id)
        metadata = {"game_id": game_id, "minigame_id": game_id, "category": game["category"],
                    "content_version": event["content_version"], "projection_version": 1}
        if et == "problem_started":
            level = details.get("level")
            # Dashboard catalog resolves exact task_id; level belongs to the run, not the game code.
            task_code = game_id
            # Use existing definition UUID for the same (task_id, content_version).
            pkey = (task_code, event["content_version"])
            if pkey not in _problem_ids:
                existing = self.remote.game_problems.find_one({"task_id": task_code, "content_version": event["content_version"]})
                _problem_ids[pkey] = existing["_id"] if existing else uid("problem", task_code, event["content_version"])
            problem_id = _problem_ids[pkey]
            definition = {"_id": problem_id, "task_id": task_code, "content_version": event["content_version"],
                          "title": game["title"],
                          "topic": game["topic"], **metadata}
            ensure(self.remote.game_problems, definition, ["task_id", "content_version", "topic"])
            session = {"_id": session_id, "player_id": player, "topic": game["topic"], "started_at": occurred,
                       "ended_at": None, "status": "in_progress", "game_version": event["build_version"],
                       "source_session_id": source_session, **metadata}
            # A source app session can contain multiple games: dashboard sessions are deterministic children.
            ensure(self.remote.game_sessions, session, ["player_id", "topic", "source_session_id"])
            run = {"_id": source_run, "session_id": session_id, "task_id": problem_id, "started_at": occurred,
                   "ended_at": None, "status": "in_progress", "active_ms": None, "paused_ms": None, "level": level, **metadata}
            self.cache[("run", source_run)] = ensure(self.remote.problem_runs, run, ["session_id", "task_id", "started_at"])
            context = {"_id": contextkey, "player_id": player, "session_id": session_id, "run_id": source_run,
                       "problem_id": problem_id, "game_id": game_id, "started_at": occurred}
            context = ensure(self.local.remote_run_contexts, context, ["player_id", "session_id", "problem_id", "started_at"])
            self.cache[("ctx", contextkey)] = context
        if context is None:
            return self.quarantine(event, "event_without_prior_problem_start")
        if context["game_id"] != game_id or context["player_id"] != player:
            raise Conflict("run_context_mismatch")
        if occurred < context["started_at"]:
            raise Conflict("event_before_problem_start")
        attempt_id = None
        # From here the event is the next contiguous sequence (gaps raised Waiting above), so nothing that is
        # still to arrive can repair a field missing from this event: quarantine it with its reason instead of
        # waiting forever (reliability review P1-3). A real missing sequence still waits.
        if et == "attempt_started":
            if not event.get("attempt_id"):
                return self.quarantine(event, "attempt_id_required")
            attempt_id = str(uuid.UUID(event["attempt_id"]))
            existing = self.cache.get(("att", attempt_id)) or self.remote.solution_attempts.find_one({"_id": attempt_id})
            if existing:
                number = existing["number"]
            elif ("lastnum", source_run) in self.cache:
                number = self.cache[("lastnum", source_run)] + 1
            else:
                last = self.remote.solution_attempts.find_one({"run_id": source_run}, sort=[("number", -1)])
                number = last["number"] + 1 if last else 1
            attempt = {"_id": attempt_id, "run_id": source_run, "number": number, "started_at": occurred,
                       "ended_at": None, "outcome": "in_progress", **metadata}
            self.cache[("att", attempt_id)] = ensure(self.remote.solution_attempts, attempt, ["run_id", "number", "started_at"])
            self.cache[("lastnum", source_run)] = max(number, self.cache.get(("lastnum", source_run), 0))
        elif et not in ("problem_started", "problem_completed"):
            if not event.get("attempt_id"):
                return self.quarantine(event, "attempt_id_required")
            attempt_id = str(uuid.UUID(event["attempt_id"]))
            attempt = self._cached(("att", attempt_id), lambda: self.remote.solution_attempts.find_one({"_id": attempt_id}))
            if not attempt or attempt["run_id"] != source_run:
                return self.quarantine(event, "event_without_prior_attempt_start")
            if attempt.get("ended_at") is not None and et not in ("attempt_succeeded", "attempt_failed"):
                return self.quarantine(event, "event_after_attempt_closed")
            if occurred < attempt["started_at"]:
                raise Conflict("event_before_attempt_start")
        run_doc = self._cached(("run", source_run), lambda: self.remote.problem_runs.find_one({"_id": source_run}))
        if run_doc and run_doc.get("ended_at") is not None and et != "problem_completed":
            return self.quarantine(event, "event_after_run_closed")
        if et in ("operation_succeeded", "operation_failed") and not details.get("operation"):
            return self.quarantine(event, "operation_required")
        if et == "operation_failed" and not details.get("failure_reason"):
            return self.quarantine(event, "failure_reason_required")
        doc = {"_id": event["event_id"], "session_id": context["session_id"], "run_id": source_run,
               "attempt_id": attempt_id, "sequence": sequence, "event_type": "hint" if et == "hint_used" else et,
               "occurred_at": occurred, "received_at": dt(event["received_at"]), "source_payload_hash": event["payload_hash"],
               "payload": {"source_session_id": source_session, "source_run_id": source_run,
                           "source_event_type": et, "details": details}, **metadata}
        for field in ["operation", "failure_reason", "hint_id"]:
            if details.get(field) is not None:
                doc[field] = details[field]
        for field in ["state_before", "state_after"]:
            state = details.get(field)
            if isinstance(state, str):
                try:state = json.loads(state)
                except ValueError:state = None
            if isinstance(state, dict):doc[field] = state
        ensure(self.remote.gameplay_events, doc, ["session_id", "run_id", "attempt_id", "sequence", "source_payload_hash"])
        if et in ("attempt_succeeded", "attempt_failed"):
            outcome = "success" if et == "attempt_succeeded" else "failure"
            self.finalize(self.remote.solution_attempts, attempt_id, "outcome", outcome, occurred)
            if ("att", attempt_id) in self.cache:
                self.cache[("att", attempt_id)] = dict(self.cache[("att", attempt_id)], outcome=outcome, ended_at=occurred)
        if et == "problem_completed":
            if self.remote.solution_attempts.count_documents({"run_id": source_run, "ended_at": None}):
                # every earlier sequence is already projected, so no later event can close these attempts
                return self.quarantine(event, "attempts_still_open")
            self.finalize(self.remote.problem_runs, source_run, "status", "completed", occurred)
            if ("run", source_run) in self.cache:
                self.cache[("run", source_run)] = dict(self.cache[("run", source_run)], status="completed", ended_at=occurred)
        result = {"scope": "dashboard_records", "member_id": player, "session_id": context["session_id"],
                  "run_id": source_run, "attempt_id": attempt_id, "event_id": event["event_id"]}
        return self.receipt(event, result)

    @staticmethod
    def finalize(collection, ident, field, status, ended):
        old = collection.find_one({"_id": ident})
        if old is None or ended < old["started_at"]:
            raise Conflict("invalid_end_time")
        if old.get("ended_at") is not None:
            if old["ended_at"] != ended or old.get(field) != status:
                raise Conflict("final_state_conflict")
            return
        if collection.update_one({"_id": ident, "ended_at": None}, {"$set": {field: status, "ended_at": ended}}).modified_count != 1:
            raise Conflict("concurrent_finalization")
