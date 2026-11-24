"""Operate on parked / waiting rows of the remote outbox (PL-9).

  python -m remote.requeue status                                   # counts by state and reason
  python -m remote.requeue requeue --reason member_mapping_required   # parked -> pending (try again now)
  python -m remote.requeue requeue --reason sequence_gap             # reason prefix match
  python -m remote.requeue park --reason member_mapping_required --state waiting_context
                                                                    # stop retrying now (e.g. test data)

Rows are never deleted here. Inside the containers:
  docker compose -p nodeverse --env-file .env --profile remote -f compose.json exec remote-writer python -m remote.requeue status
"""
from __future__ import annotations

import argparse
import json
import re

from common.db import local_mongo
from common.timeutil import utc_now


def _query(state: str, reason: str | None, player: str | None) -> dict:
    q: dict = {"state": state}
    if reason:
        q["reason"] = {"$regex": "^" + re.escape(reason)}
    if player:
        q["event.player_id"] = {"$regex": "^" + re.escape(player)}
    return q


REASON_STATES = ("parked", "waiting_context", "blocked")


def state_counts(db) -> dict:
    """Rows per state from the (state, ...) indexes: DISTINCT_SCAN + one COUNT_SCAN per state, no document reads.
    The former $group over the whole outbox was a COLLSCAN of every row every 10 s (CPU review, item 3)."""
    out = db.remote_event_outbox
    return {s: n for s in out.distinct("state") if s is not None
            for n in [out.count_documents({"state": s})] if n}


def reason_counts(db) -> dict:
    """Top reasons of the waiting/parked/blocked states; {state, reason} is covered by the (state, reason) index."""
    return {state: [{"reason": d["_id"], "n": d["n"]} for d in db.remote_event_outbox.aggregate([
        {"$match": {"state": state}}, {"$project": {"_id": 0, "reason": 1}},
        {"$group": {"_id": "$reason", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}, {"$limit": 10}],
        hint={"state": 1, "reason": 1})]
        for state in REASON_STATES}


def status(db) -> dict:
    return {"states": state_counts(db), "reasons": reason_counts(db)}


def requeue(db, reason: str | None, player: str | None, state: str = "parked") -> int:
    """Back to the queue with a fresh retry budget (REMOTE_MAX_RETRIES attempts from now on). `attempts` keeps
    the whole history; `retry_base` remembers where this round started (reliability review P1-4)."""
    from pymongo import UpdateOne
    ops = [UpdateOne({"_id": d["_id"], "state": state},
                     {"$set": {"state": "pending", "next_attempt_at": utc_now(), "updated_at": utc_now(),
                               "retry_base": d.get("attempts", 0), "requeued_at": utc_now()},
                      "$unset": {"parked_at": "", "waiting_since": ""}})
           for d in db.remote_event_outbox.find(_query(state, reason, player), {"attempts": 1})]
    return db.remote_event_outbox.bulk_write(ops, ordered=False).modified_count if ops else 0


def park(db, reason: str | None, player: str | None, state: str = "waiting_context") -> int:
    if not reason and not player:
        raise SystemExit("park needs --reason or --player")
    return db.remote_event_outbox.update_many(
        _query(state, reason, player),
        {"$set": {"state": "parked", "parked_at": utc_now(), "updated_at": utc_now()},
         "$unset": {"next_attempt_at": ""}}).modified_count


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("action", choices=["status", "requeue", "park"])
    p.add_argument("--reason", help="reason prefix, e.g. member_mapping_required, sequence_gap, retry_limit")
    p.add_argument("--player", help="player_id prefix")
    p.add_argument("--state", help="source state (requeue: parked, park: waiting_context)")
    a = p.parse_args(argv)
    db = local_mongo()
    if a.action == "status":
        out = status(db)
    elif a.action == "requeue":
        out = {"requeued": requeue(db, a.reason, a.player, a.state or "parked")}
    else:
        out = {"parked": park(db, a.reason, a.player, a.state or "waiting_context")}
    print(json.dumps(out, ensure_ascii=False, default=str))
    return out


if __name__ == "__main__":
    main()
