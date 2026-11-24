"""Stateful audit function for silver_audit (runs inside Spark Python workers).

Kept in its own module with no project imports, so the Python workers only need this file (--py-files).
"""
import os

DEDUP_TTL_MS = int(os.getenv("DEDUP_TTL_HOURS", str(7 * 24))) * 3600 * 1000
if DEDUP_TTL_MS <= 0:
    raise ValueError('DEDUP_TTL_HOURS must be positive')
DECISION_SCHEMA = ("key string, value string, topic string, partition int, offset long, "
                   "event_id string, action string")
STATE_SCHEMA = "hash string"


def audit(key, pdfs, state):
    """Per key (topic:event_id): first copy -> first, same hash -> dropped, other hash -> id_conflict."""
    import pandas as pd
    (k,) = key
    if state.hasTimedOut:  # nothing seen for DEDUP_TTL: forget the id
        state.remove()
        return
    rows = pd.concat(list(pdfs))
    if len(rows) > 1:
        rows = rows.sort_values(["partition", "offset"])
    cols = ["key", "value", "topic", "partition", "offset", "event_id", "action"]
    if k.startswith("bad:"):  # no usable event_id/hash, never stored in state
        yield rows.assign(action="invalid")[cols]
        return
    known = state.get[0] if state.exists else None
    actions = []
    for h in rows["hash"]:
        if known is None:
            known = h
            actions.append("first")
        elif h == known:
            actions.append("duplicate")
        else:
            actions.append("id_conflict")
    state.update((known,))
    state.setTimeoutDuration(DEDUP_TTL_MS)
    rows = rows.assign(action=actions)
    rows = rows[rows["action"] != "duplicate"]
    if len(rows):
        yield rows[cols]
