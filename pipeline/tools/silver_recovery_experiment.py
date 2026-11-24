"""Disposable Delta + Mongo test of failure after MERGE and partial outbox staging."""
import json
from pathlib import Path
from pymongo import MongoClient
import tables as tb
import silver
import remote.outbox as outbox
from silver_benchmark import rows, batch, TABLE

root = Path('/experiment')
if (root / 'silver').exists():
    raise RuntimeError('Refusing to overwrite experiment')
client = MongoClient('mongodb://mongo:27017', serverSelectionTimeoutMS=15000)
db = client['isolated_silver_recovery']
assert not db.list_collection_names(), 'Refusing nonempty experiment DB'
tb.LAKE = str(root)
spark = tb.spark_session('silver-recovery', 2)
try:
    rows(spark, 1000).repartition(4, 'event_date').write.format('delta').partitionBy('event_date').save(tb.path(TABLE))
    source = batch(spark)
    before = spark.read.format('delta').load(tb.path(TABLE))
    expected = before.count() + source.select('event_id').join(before.select('event_id'), 'event_id', 'left_anti').count()
    original = outbox.stage
    calls = [0]
    def inject(records, queued_at=None):
        calls[0] += 1
        if calls[0] == 2:
            raise RuntimeError('injected after first chunk')
        return original(records, db=db, queued_at=queued_at)
    outbox.stage = inject
    try:
        silver._write_silver(TABLE, 'learning_events')(source, 1)
        raise AssertionError('failure was not propagated')
    except RuntimeError as exc:
        assert str(exc) == 'injected after first chunk'
    assert db.remote_event_outbox.count_documents({}) == 100
    assert spark.read.format('delta').load(tb.path(TABLE)).count() == expected
    outbox.stage = lambda records, queued_at=None: original(records, db=db, queued_at=queued_at)
    silver._write_silver(TABLE, 'learning_events')(source, 1)
    table = spark.read.format('delta').load(tb.path(TABLE))
    assert table.count() == expected
    assert table.groupBy('event_id').count().where('count > 1').count() == 0
    assert db.remote_event_outbox.count_documents({}) == 301
    print('RECOVERY_RESULT ' + json.dumps({'passed': True, 'silver_rows': expected,
          'outbox_unique': 301, 'failure_after_outbox_rows': 100,
          'scope': 'isolated Delta and Mongo; explicit batch replay, not streaming checkpoint'}), flush=True)
finally:
    spark.stop()
