from datetime import datetime, timezone
from observatory.recent import INDEX, KEYS, recent_records


class Cursor:
    def __init__(self, rows): self.rows = rows
    def hint(self, name): assert name == INDEX; return self
    def sort(self, keys): assert keys == KEYS; return self
    def limit(self, n): self.rows = self.rows[:n]; return self
    def max_time_ms(self, n): assert n == 1000; return iter(self.rows)


class Collection:
    def __init__(self, indexed=True): self.indexed = indexed
    def index_information(self): return {INDEX: {'key': KEYS}} if self.indexed else {}
    def find(self, query, projection):
        assert 'event.player_id' not in projection
        return Cursor([{'_id': str(i), 'state': 'stored', 'queued_at': datetime.now(timezone.utc),
                        'event': {'event_type': 'run_heartbeat', 'received_at': '2026-11-23T00:00:00Z', 'player_id': 'private'}} for i in range(30)])


class DB:
    def __init__(self, indexed=True): self.remote_event_outbox = Collection(indexed)


def test_requires_index():
    assert recent_records(DB(False)) == {'state': 'index_required', 'records': []}


def test_bounded_sanitized_records():
    result = recent_records(DB())
    assert len(result['records']) == 20
    assert result['records'][0]['event_type'] == 'run_heartbeat'
    assert len(result['records'][0]['reference']) == 12
    assert 'private' not in str(result)
