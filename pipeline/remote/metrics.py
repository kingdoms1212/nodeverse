"""Copy rt_* aggregates from the local MongoDB to the dashboard MongoDB (BN-7)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pymongo import UpdateOne

METRIC_COLLECTIONS = ["rt_minigame_1m", "rt_alerts", "rt_updates", "rt_versions", "rt_summary"]
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
OVERLAP = timedelta(seconds=30)  # a writer may commit an older updated_at slightly after a newer one


def mirror_metrics(local, remote, since=None):
    """Copy rows changed since the per-collection high-water marks in `since` (None = everything).

    Each write is guarded atomically so a delayed older aggregate never overwrites a newer one.
    Returns the new high-water marks."""
    marks = dict(since or {})
    for name in METRIC_COLLECTIONS:
        query = {"updated_at": {"$gte": marks[name] - OVERLAP}} if marks.get(name) else {"updated_at": {"$type": "date"}}
        ops, top = [], marks.get(name)
        for source in local[name].find(query):
            if not isinstance(source.get("updated_at"), datetime):
                continue
            doc = dict(source, sink_source="nodeverse-v2")
            ops.append(UpdateOne({"_id": doc["_id"]}, [{"$replaceWith": {"$cond": [
                {"$gte": [{"$literal": doc["updated_at"]}, {"$ifNull": ["$updated_at", EPOCH]}]},
                {"$literal": doc}, "$$ROOT"]}}], upsert=True))
            u = doc["updated_at"] if doc["updated_at"].tzinfo else doc["updated_at"].replace(tzinfo=timezone.utc)
            top = u if top is None or u > top else top
        if ops:
            remote[name].bulk_write(ops, ordered=False)
        if top is not None:
            marks[name] = top
    keep = [x["_id"] for x in local.rt_versions.find({}, {"_id": 1})]
    remote.rt_versions.delete_many({"sink_source": "nodeverse-v2", "_id": {"$nin": keep}})
    return marks
