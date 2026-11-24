"""Disposable Mongo experiment: partial outbox failure followed by full-batch replay."""
import json
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

sys.path.insert(0, '/app/spark')
from pymongo import MongoClient
import sinks
import remote.outbox as outbox

client = MongoClient('mongodb://mongo:27017', serverSelectionTimeoutMS=15000)
db = client['isolated_outbox_recovery']
db.command('ping')
assert not db.list_collection_names(), 'Refusing to use a nonempty experiment database'
rows = [{'event_id': 'event-%03d' % i, 'payload_hash': 'hash-%03d' % i,
         'session_id': 'session-a', 'sequence': i, 'occurred_at': '2026-11-23T00:00:00Z'}
        for i in range(10)]
original_stage = outbox.stage
sinks.settings.get = lambda: SimpleNamespace(silver_optimization_enabled=True, outbox_stage_chunk=3, sink_retry_attempts=1)

calls = 0
def fail_after_first(chunk, queued_at=None):
    global calls
    calls += 1
    if calls == 2:
        raise RuntimeError('injected failure before second chunk')
    return original_stage(chunk, db=db, queued_at=queued_at)

outbox.stage = fail_after_first
try:
    sinks.stage_outbox(iter(rows))
    raise AssertionError('Injected failure did not propagate')
except RuntimeError as exc:
    assert str(exc) == 'injected failure before second chunk'
assert db.remote_event_outbox.count_documents({}) == 3
first = {x['_id']: x['queued_at'] for x in db.remote_event_outbox.find({})}

outbox.stage = lambda chunk, queued_at=None: original_stage(chunk, db=db, queued_at=queued_at)
sinks.stage_outbox(iter(rows))
sinks.stage_outbox(iter(rows))
docs = list(db.remote_event_outbox.find({}).sort('sequence', 1))
assert len(docs) == len(rows)
assert [d['sequence'] for d in docs] == list(range(10))
assert all(d['queued_at'] == first[d['_id']] for d in docs[:3])
assert len({d['queued_at'] for d in docs[3:]}) == 1
assert max(d['queued_at'] for d in docs[:3]) <= min(d['queued_at'] for d in docs[3:])
assert all(d['state'] == 'pending' for d in docs)

conflict = dict(rows[0], payload_hash='different')
original_stage([conflict], db=db)
assert db.remote_event_outbox.find_one({'_id': rows[0]['event_id']})['payload_hash'] == rows[0]['payload_hash']
assert db.remote_export_conflicts.count_documents({'event_id': rows[0]['event_id']}) == 1
print(json.dumps({'passed': True, 'first_chunk': 3, 'replayed_unique': len(docs),
                  'conflicts_recorded': 1, 'scope': 'isolated Mongo outbox only'}))
