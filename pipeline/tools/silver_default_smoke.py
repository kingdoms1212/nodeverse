"""Isolated check that the default, nonexperimental Silver path still replays safely."""
from pathlib import Path
import tables as tb
import silver
from common import settings
from silver_benchmark import rows, batch, TABLE

assert not settings.get().silver_optimization_enabled
assert not settings.get().remote_export_enabled
root = Path('/experiment')
assert not (root / 'silver').exists(), 'Refusing to overwrite experiment'
tb.LAKE = str(root)
spark = tb.spark_session('silver-default-smoke', 2)
try:
    rows(spark, 1000).repartition(4, 'event_date').write.format('delta').partitionBy('event_date').save(tb.path(TABLE))
    source = batch(spark)
    before = spark.read.format('delta').load(tb.path(TABLE))
    expected = before.count() + source.select('event_id').join(before.select('event_id'), 'event_id', 'left_anti').count()
    silver._write_silver(TABLE, 'learning_events')(source, 1)
    silver._write_silver(TABLE, 'learning_events')(source, 1)
    result = spark.read.format('delta').load(tb.path(TABLE))
    assert result.count() == expected
    assert result.groupBy('event_id').count().where('count > 1').count() == 0
    print('DEFAULT_SMOKE passed rows=%s' % expected, flush=True)
finally:
    spark.stop()
