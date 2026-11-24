"""Isolated Spark operator comparison. Never mounts production data or connects to DB/Kafka.

Simulated kafka_ts advances event time, not the host clock. Audit timeout is tested separately
with its actual function and a fake GroupState; this is not a 168-hour wall-clock soak.
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path('/experiment')
BASE = datetime(2026, 11, 1, tzinfo=timezone.utc)
N = int(os.getenv('COMPARE_ROWS', '2000'))


def event(ident, hour, payload='original'):
    return {'event_id': ident, 'kafka_ts': (BASE+timedelta(hours=hour)).isoformat(),
            'payload_hash': payload, 'event_date': BASE.date().isoformat()}


def run(spark, hours, label, migrate=False):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType
    from delta.tables import DeltaTable
    import silver
    root = ROOT/label
    root.mkdir(parents=True, exist_ok=False)
    source = root/'input';source.mkdir()
    output = str(root/'accepted');checkpoint = str(root/'checkpoint')
    schema = StructType([StructField('event_id', StringType()), StructField('kafka_ts', TimestampType()),
                         StructField('payload_hash', StringType()), StructField('event_date', DateType())])
    def start(ttl):
        return (spark.readStream.schema(schema).json(str(source))
                .withWatermark('kafka_ts', f'{ttl} hours').dropDuplicatesWithinWatermark(['event_id'])
                .writeStream.format('parquet').option('path', output)
                .option('checkpointLocation', checkpoint).trigger(processingTime='1 second').start())
    query = start(hours)
    samples = []
    steps = [
        ('initial', [event(f'id-{i:06d}', 0) for i in range(N)]),
        ('within_1h', [event('id-000000', 1), event('id-000001', 1, 'conflict')]),
        ('advance_49h', [event('clock-49', 49)]),
        ('replay_49h', [event('id-000000', 49)]),
        ('advance_97h', [event('clock-97', 97)]),
        ('replay_97h', [event('id-000000', 97), event('id-000001', 97, 'conflict')]),
        ('backlog_1h', [event('late-new', 1)]),
        ('advance_169h', [event('clock-169', 169)]),
        ('replay_169h', [event('id-000000', 169)]),
    ]
    try:
        for step, (name, rows) in enumerate(steps):
            if step == 4:  # checkpoint resume in all cases, including an explicit 168->48 case
                query.stop();query = start(48 if migrate else hours)
            tmp=root/'pending.json';tmp.write_text('\n'.join(json.dumps(r) for r in rows)+'\n')
            tmp.replace(source/f'{step:03d}.json')
            started=time.monotonic();query.processAllAvailable()
            progress=list(query.recentProgress)
            data=[p for p in progress if p.get('numInputRows',0)>0]
            p=data[-1] if data else (progress[-1] if progress else {})
            state=(progress[-1].get('stateOperators') or [{}])[0] if progress else {}
            count=spark.read.parquet(output).count()
            samples.append({'step':name,'accepted_total':count,'elapsed_ms':round((time.monotonic()-started)*1000),
                'input_batch_ms':p.get('durationMs',{}).get('triggerExecution'),
                'state_rows':state.get('numRowsTotal'),'state_bytes':state.get('memoryUsedBytes'),
                'watermark':(progress[-1].get('eventTime') or {}).get('watermark') if progress else None,
                'dropped_late':sum((op.get('numRowsDroppedByWatermark') or 0) for op in p.get('stateOperators',[]))})
            print('COMPARE '+json.dumps({'case':label,**samples[-1]}),flush=True)
    finally:
        query.stop()
    accepted=spark.read.parquet(output)
    # Replay accepted rows in batch order through the production insert-only MERGE condition.
    # All events use the same legal event_date so the 7-day target pruning remains in force.
    target=str(root/'silver')
    first=spark.createDataFrame([event('seed',0)],schema=None).select('event_id','payload_hash').withColumn('event_date',F.lit(BASE.date()))
    first.write.format('delta').save(target)
    source_rows=accepted.orderBy('kafka_ts').collect()
    # Preserve deterministic first value within this test; equal-time conflicts are intentionally omitted.
    unique={}
    for row in source_rows:unique.setdefault(row.event_id,row)
    source_df=spark.createDataFrame(list(unique.values()),accepted.schema).drop('kafka_ts')
    cond=silver.merge_condition(BASE.date(),BASE.date())
    table=DeltaTable.forPath(spark,target)
    for _ in range(2):
        table.alias('t').merge(source_df.alias('s'),cond).whenNotMatchedInsertAll().execute()
    silver_rows=spark.read.format('delta').load(target).where("event_id != 'seed'")
    ids={r.event_id:r.payload_hash for r in silver_rows.select('event_id','payload_hash').collect()}
    assert ids['id-000000']=='original' and ids['id-000001']=='original'
    assert silver_rows.count()==len(unique)
    return {'label':label,'ttl_hours':hours,'migrated_to':48 if migrate else None,'samples':samples,
            'silver_unique_rows':len(ids),'late_new_present':'late-new' in ids,
            'immutable_original_kept':True,'merge_replay_no_duplicates':True,
            'logical_gold_count_proxy':sum(k.startswith('id-') for k in ids)}


def main():
    import tables
    spark=tables.spark_session('dedup-comparison',2)
    try:
        results=[run(spark,168,'ttl168'),run(spark,48,'ttl48'),run(spark,168,'resume168to48',True)]
        baseline,short,resume=results
        assert baseline['late_new_present'] and not short['late_new_present'] and not resume['late_new_present']
        assert all(case['logical_gold_count_proxy']==N for case in results)
        assert baseline['silver_unique_rows']==N+4 and short['silver_unique_rows']==resume['silver_unique_rows']==N+3
        result={'rows':N,'spark':spark.version,'synthetic_event_time':True,'cases':results}
        (ROOT/'comparison.json').write_text(json.dumps(result,indent=2))
        print('RESULT '+json.dumps(result),flush=True)
    finally:spark.stop()


if __name__=='__main__':main()
