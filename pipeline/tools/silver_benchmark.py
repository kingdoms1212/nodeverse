"""Isolated actual _write_silver comparison; synthetic Delta tables and Spark event log only."""
import importlib.util
import json
import os
import shutil
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT=Path('/experiment')
BASE=datetime(2026,10,1,tzinfo=timezone.utc)
TABLE='silver.learning_events'


def eid(day,offset):
    ms=int((BASE+timedelta(days=day)).timestamp()*1000)
    return str(uuid.UUID(int=(ms<<80)|(7<<76)|(2<<62)|offset))


def rows(spark,n):
    from pyspark.sql import functions as F
    import tables as tb
    r=spark.range(n).withColumn('day', (F.col('id')%40).cast('int'))
    ms=F.lit(int(BASE.timestamp()*1000))+F.col('day').cast('long')*F.lit(86400000)
    h=F.format_string('%012x',ms)
    r=r.withColumn('event_id',F.concat(F.substring(h,1,8),F.lit('-'),F.substring(h,9,4),F.lit('-7000-8000-'),F.format_string('%012x',F.col('id'))))
    r=r.withColumn('event_date',F.date_add(F.lit(BASE.date()),F.col('day'))).withColumn('event_time',F.col('event_date').cast('timestamp'))
    r=r.withColumn('payload_hash',F.sha2(F.col('event_id'),256))
    values={'schema_version':F.lit(2),'player_id':F.lit('synthetic-player'),'session_id':F.lit('synthetic-session'),
            'event_type':F.lit('operation'),'sequence':F.col('id'),'occurred_at':F.col('event_time'),
            'received_at':F.col('event_time'),'minigame_id':F.lit('synthetic'),'category':F.lit('sql')}
    schema=tb.SCHEMAS[TABLE][0]
    return r.select(*[(F.col(f.name) if f.name in r.columns else values.get(f.name,F.lit(None))).cast(f.dataType).alias(f.name) for f in schema])


def batch(spark):
    import tables as tb
    from pyspark.sql import functions as F
    records=[]
    # Existing IDs from recent days 36 and 39; new IDs on those same dates.
    for day in (36,39):
        for i in range(50):
            ident=eid(day,day+40*i)
            records.append({'event_id':ident,'event_date':(BASE+timedelta(days=day)).date(),'payload_hash':'changed-copy'})
        for i in range(100):
            records.append({'event_id':eid(day,10_000_000+i),'event_date':(BASE+timedelta(days=day)).date(),'payload_hash':'new'})
    records.append({'event_id':'ffffffff-ffff-4fff-8fff-ffffffffffff','event_date':(BASE+timedelta(days=39)).date(),'payload_hash':'new-v4'})
    r=spark.createDataFrame(records)
    schema=tb.SCHEMAS[TABLE][0]
    defaults={'schema_version':F.lit(2),'player_id':F.lit('synthetic-player'),'event_type':F.lit('operation'),
              'occurred_at':F.col('event_date').cast('timestamp'),'received_at':F.col('event_date').cast('timestamp'),
              'event_time':F.col('event_date').cast('timestamp'),'sequence':F.lit(0)}
    return r.select(*[(F.col(f.name) if f.name in r.columns else defaults.get(f.name,F.lit(None))).cast(f.dataType).alias(f.name) for f in schema]).withColumn('kafka_ts',F.col('event_date').cast('timestamp'))


def candidate_files(spark,path,condition):
    # Delta's internal metadata API pinned to 3.2.1. This is candidate file metadata,
    # not proof of physical file opens; physical task bytes come from the event log.
    jvm=spark._jvm
    snapshot=jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession,path).snapshot()
    expr=spark.read.format('delta').load(path).where(condition)._jdf.queryExecution().analyzed().condition()
    seq=jvm.PythonUtils.toSeq([expr])
    scan=snapshot.filesForScan(seq,False)
    it=scan.files().iterator();files=[]
    while it.hasNext():
        f=it.next();files.append({'path':f.path(),'bytes':f.size()})
    return files


def execute(spark,module,target,source,label,batch_id):
    import tables as tb
    from delta.tables import DeltaTable
    tb.LAKE=str(target)
    group='stage3:'+label
    spark.sparkContext.setJobGroup(group,label)
    started=time.monotonic()
    try:module._write_silver(TABLE,'learning_events')(source,batch_id)
    finally:spark.sparkContext.setLocalProperty('spark.jobGroup.id',None)
    from common.observability import stages
    commits=DeltaTable.forPath(spark,tb.path(TABLE)).history(10).where("operation = 'MERGE'").select('version','operationMetrics').collect()
    result={'label':label,'wall_ms':round((time.monotonic()-started)*1000),'stage':stages().get('silver_learning_events'),
            'commits':[{'version':r.version,'metrics':dict(r.operationMetrics)} for r in commits]}
    print('BENCH '+json.dumps(result),flush=True)
    return result


def summarize_events():
    stage_groups={};groups={}
    for path in (ROOT/'events').glob('*'):
        if not path.is_file():continue
        for line in path.open():
            try:e=json.loads(line)
            except ValueError:continue
            if e.get('Event')=='SparkListenerJobStart':
                group=(e.get('Properties') or {}).get('spark.jobGroup.id')
                if group:
                    for stage in e.get('Stage IDs',[]):stage_groups[stage]=group
            elif e.get('Event')=='SparkListenerTaskEnd':
                # Attribute at task completion: later jobs may mention a reused stage ID.
                group=stage_groups.get(e.get('Stage ID'))
                if not group or not group.startswith('stage3:'):continue
                m=e.get('Task Metrics') or {};inp=m.get('Input Metrics') or {}
                g=groups.setdefault(group,{'task_input_bytes':0,'task_input_records':0,'tasks':0,'stages':set()})
                g['task_input_bytes']+=inp.get('Bytes Read',0);g['task_input_records']+=inp.get('Records Read',0);g['tasks']+=1;g['stages'].add(e['Stage ID'])
    for value in groups.values():value['stages']=len(value['stages'])
    return groups


def main():
    import tables as tb
    import silver
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable
    spec=importlib.util.spec_from_file_location('baseline_silver',ROOT/'baseline_silver.py')
    baseline=importlib.util.module_from_spec(spec);spec.loader.exec_module(baseline)
    (ROOT/'events').mkdir(exist_ok=True)
    spark=tb.spark_session('silver-stage3',2)
    results=[]
    try:
        for size in (20000,200000):
            base=ROOT/f'base-{size}';path=str(base/'silver'/'learning_events')
            if base.exists():raise RuntimeError('Refuse to overwrite experiment')
            rows(spark,size).repartition(40,'event_date').write.format('delta').partitionBy('event_date').save(path)
            source=batch(spark)
            sample=spark.read.format('delta').load(path).where(F.col('sequence')==36).select('event_id').first()[0]
            assert sample==eid(36,36), 'fixture UUID timestamp encoding mismatch'
            # Metadata-only comparison of safe predicates against the same baseline snapshot.
            bounds=[]
            for part in baseline.merge_groups(source.drop('kafka_ts')):
                r=part.agg(F.min('event_date'),F.max('event_date'),F.min('event_id'),F.max('event_id')).first()
                lo,hi,imin,imax=r
                c=baseline.merge_condition(lo,hi,imin,imax).replace('t.event_id = s.event_id','true').replace('t.','')
                bounds.append(c)
            all_files=candidate_files(spark,path,'true')
            selected={f['path']:f['bytes'] for c in bounds for f in candidate_files(spark,path,c)}
            metadata={'total_files':len(all_files),'total_bytes':sum(f['bytes'] for f in all_files),
                      'candidate_files':len(selected),'candidate_bytes':sum(selected.values())}
            print('SCAN_METADATA '+json.dumps({'rows':size,**metadata}),flush=True)
            for mode in ('legacy_unbounded','current','optimized'):
                target=ROOT/f'{size}-{mode}';shutil.copytree(base,target)
                mod=baseline if mode!='optimized' else silver
                original_condition,original_groups=baseline.merge_condition,baseline.merge_groups
                if mode=='legacy_unbounded':
                    baseline.merge_condition=lambda *args:'t.event_id = s.event_id'
                    baseline.merge_groups=lambda df:[df]
                try:
                    before=DeltaTable.forPath(spark,str(target/'silver'/'learning_events')).toDF()
                    known=before.select('event_id','payload_hash').cache();known.count()
                    result=execute(spark,mod,target,source,f'{size}-{mode}',1)
                    result.update(rows=size,mode=mode,metadata=metadata)
                    after=spark.read.format('delta').load(str(target/'silver'/'learning_events'))
                    assert after.count()==size+201
                    assert after.groupBy('event_id').count().where('count > 1').count()==0
                    assert known.alias('a').join(after.alias('b'),'event_id').where(F.col('a.payload_hash')!=F.col('b.payload_hash')).count()==0
                    known.unpersist()
                    replay=execute(spark,mod,target,source,f'{size}-{mode}-replay',2)
                    assert spark.read.format('delta').load(str(target/'silver'/'learning_events')).count()==size+201
                    result['replay']=replay;result['correct']=True;results.append(result)
                finally:baseline.merge_condition,baseline.merge_groups=original_condition,original_groups
        # Guard against excessive distinct ID days; all rows must remain represented.
        plans=silver.merge_plan(source.drop('kafka_ts'),1)
        assert len(plans)==1 and sum(p[-1] for p in plans)==301
        assert silver.merge_plan(source.limit(0).drop('kafka_ts'))==[]
    finally:spark.stop()
    report={'spark':'3.5.3','delta':'3.2.1','results':results,'event_log':summarize_events()}
    (ROOT/'silver-comparison.json').write_text(json.dumps(report,indent=2))
    print('COMPLETE '+json.dumps({'cases':len(results)}),flush=True)


if __name__=='__main__':main()
