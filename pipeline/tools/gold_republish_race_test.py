"""Exercise republish bookkeeping with an identical failure added during a copy."""
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import gold


class Expr:
    def __ge__(self, other): return self
    def __le__(self, other): return self
    def __and__(self, other): return self


class Row:
    def asDict(self): return {'minigame_id': 'fixture', 'window_start': datetime(2026, 11, 23, tzinfo=timezone.utc)}


class Query:
    def format(self, *_): return self
    def load(self, *_): return self
    def where(self, *_): return self
    def collect(self): return [Row()]


gold.F.col = lambda *_: Expr()
gold.F.lit = lambda *_: Expr()
with tempfile.TemporaryDirectory() as tmp:
    gold.settings.get = lambda: SimpleNamespace(progress_path=str(Path(tmp) / 'progress.json'))
    row = Row().asDict()
    gold.sinks.upsert = lambda *_: False
    assert not gold.publish('rt_minigame_1m', [row])
    assert gold.pending_republish() == 1

    def publish_during_copy(*_):
        gold.sinks.upsert = lambda *_: False
        assert not gold.publish('rt_minigame_1m', [row])
        return True
    gold.sinks.upsert = publish_during_copy
    assert gold.republish_pending(SimpleNamespace(read=Query())) == 1
    assert gold.pending_republish() == 1, 'identical concurrent failure must remain pending'
    gold.sinks.upsert = lambda *_: False
    assert gold.republish_pending(SimpleNamespace(read=Query())) == 1
    gold.sinks.upsert = lambda *_: True
    assert gold.republish_pending(SimpleNamespace(read=Query())) == 0
    assert gold.pending_republish() == 0
print('GOLD_REPUBLISH_RACE passed')
