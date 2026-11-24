"""Bounded, sanitized view of events that reached the remote outbox."""
import hashlib
from observatory.sampler import iso, date

INDEX = 'observatory_recent_v1'
KEYS = [('queued_at', -1), ('_id', 1)]


def recent_records(db):
    coll = db.remote_event_outbox
    index = coll.index_information().get(INDEX)
    if not index or list(index.get('key', [])) != KEYS:
        return {'state': 'index_required', 'records': []}
    rows = coll.find({}, {'_id': 1, 'state': 1, 'queued_at': 1, 'completed_at': 1,
                          'archive_stored': 1, 'event.event_type': 1, 'event.received_at': 1}) \
        .hint(INDEX).sort(KEYS).limit(20).max_time_ms(1000)
    records = []
    for row in rows:
        event = row.get('event') or {}
        kind = event.get('event_type')
        item = {'reference': hashlib.sha256(str(row['_id']).encode()).hexdigest()[:12],
                'event_type': kind[:80] if isinstance(kind, str) else 'unknown',
                'state': row.get('state') if row.get('state') in ('pending','processing','waiting_context','retrying','parked','blocked','quarantined','stored') else 'unknown',
                'archive_stored': row.get('archive_stored') if isinstance(row.get('archive_stored'), bool) else None}
        for key, value in [('received_at', event.get('received_at')), ('queued_at', row.get('queued_at')), ('completed_at', row.get('completed_at'))]:
            try:
                parsed = date(value)
                item[key] = iso(parsed) if parsed else None
            except (ValueError, TypeError):
                item[key] = None
        records.append(item)
    return {'state': 'ok', 'records': records}
