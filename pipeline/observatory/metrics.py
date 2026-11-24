"""Bounded metadata probes: no Spark sessions, Delta table scans or Kafka commits."""
from __future__ import annotations
import json
import math
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import urllib.request

TOPICS = {'nv.learning.v2','nv.update.v1','nv.runtime.v1'}
LATENCY_INDEX = 'observatory_latency_v1'


def positive(value):
    try:
        n=float(value)
        return n if math.isfinite(n) and n>=0 else None
    except (ValueError,TypeError):return None


def timestamp(value):
    if isinstance(value,str):
        try:value=datetime.fromisoformat(value.replace('Z','+00:00'))
        except ValueError:return None
    if not isinstance(value,datetime):return None
    return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)


def percentile(values,p):
    return sorted(values)[max(0,math.ceil(len(values)*p)-1)] if values else None


def latency_summary(rows,now,window_minutes=15,limit=1000):
    """Nearest-rank p95, independent unique terminal records, never percentiles of polling samples."""
    groups={};seen=set();anomalies=0
    for row in rows[:limit]:
        if row['_id'] in seen:continue
        seen.add(row['_id'])
        end=timestamp(row.get('completed_at') or row.get('stored_at'))
        queued=timestamp(row.get('queued_at'))
        received=timestamp((row.get('event') or {}).get('received_at'))
        if end is None or end>now or end<now-timedelta(minutes=window_minutes):continue
        scope=(row.get('result') or {}).get('scope')
        if scope not in ['dashboard_records','learning_archive','update_archive','runtime_archive']:scope='other'
        bucket=groups.setdefault(scope,{'queue_ms':[],'received_ms':[]})
        if queued is not None and end>=queued:bucket['queue_ms'].append((end-queued).total_seconds()*1000)
        else:anomalies+=1
        if received is not None and end>=received:bucket['received_ms'].append((end-received).total_seconds()*1000)
    result={}
    for scope,values in groups.items():
        result[scope]={key:{'n':len(v),'p50':percentile(v,.5),'p95':percentile(v,.95),'max':max(v) if v else None} for key,v in values.items()}
    return {'state':'ok','window_minutes':window_minutes,'limit':limit,'truncated':len(rows)>limit,
            'groups':result,'clock_anomalies':anomalies,'method':'nearest_rank_unique_terminal_records'}


def read_latency(db,now,window_minutes=15,limit=1000):
    indexes=db.remote_event_outbox.index_information()
    index=indexes.get(LATENCY_INDEX)
    if not index or list(index.get('key',[])) != [('state',1),('completed_at',-1),('_id',1)]:
        return {'state':'index_required','groups':{},'window_minutes':window_minutes}
    rows=list(db.remote_event_outbox.find({'state':'stored','completed_at':{'$gte':now-timedelta(minutes=window_minutes),'$lte':now}},
        {'event.received_at':1,'result.scope':1,'queued_at':1,'completed_at':1,'stored_at':1})
        .hint(LATENCY_INDEX).sort([('completed_at',-1),('_id',1)]).limit(limit+1).max_time_ms(1000))
    return latency_summary(rows,now,window_minutes,limit)


class DeltaCommits:
    def __init__(self,root):
        self.root=Path(root)/'silver'/'learning_events'/'_delta_log'
        self.version=None;self.latest=None;self.last_error=None

    def sample(self):
        try:
            if self.version is None:
                checkpoint=self.root/'_last_checkpoint'
                start=int(json.loads(checkpoint.read_text())['version']) if checkpoint.exists() else 0
                self.version=max(-1,start-17)  # at most 16 preceding commit files, then incremental reads
            # A missing expired file before the checkpoint is not an empty lake.
            if not self.root.exists():return {'state':'unavailable'}
            checked=0
            while checked<128:
                version=self.version+1;path=self.root/f'{version:020d}.json'
                if not path.exists():
                    checkpoint=self.root/'_last_checkpoint'
                    cp=int(json.loads(checkpoint.read_text())['version']) if checkpoint.exists() else -1
                    if version<cp:
                        self.version+=1;checked+=1;continue
                    break
                with path.open(encoding='utf-8') as f:
                    budget=262144
                    while budget>0:
                        line=f.readline(min(65536,budget));budget-=len(line)
                        if not line:break
                        record=json.loads(line)
                        info=record.get('commitInfo')
                        if info:
                            if info.get('operation')=='MERGE':
                                m=info.get('operationMetrics') or {}
                                predicate=json.dumps((info.get('operationParameters') or {}).get('predicate',''))
                                self.latest={'state':'ok','version':version,
                                    'at':datetime.fromtimestamp(info['timestamp']/1000,timezone.utc).isoformat(),
                                    'execution_ms':positive(m.get('executionTimeMs')),'scan_ms':positive(m.get('scanTimeMs')),
                                    'rewrite_ms':positive(m.get('rewriteTimeMs')),'source_rows':positive(m.get('numSourceRows')),
                                    'inserted_rows':positive(m.get('numTargetRowsInserted')),
                                    'files_added':positive(m.get('numTargetFilesAdded')),
                                    'scan_files':positive(m.get('numTargetFilesAfterSkipping')),
                                    'scan_bytes':positive(m.get('numTargetBytesAfterSkipping')),
                                    'date_predicate': 'event_date' in predicate,
                                    'method':'delta_commit_operation_metrics'}
                            break
                self.version=version;checked+=1
            return dict(self.latest or {'state':'no_merge_found'},metadata_files_checked=checked)
        except Exception as e:
            return {'state':'error','error_type':type(e).__name__}


def bronze_offsets(report):
    for q in report.get('queries',[]):
        if q.get('name')!='bronze':continue
        progress=q.get('progress') or {}
        result={}
        for source in progress.get('sources',[]):
            offsets=source.get('endOffset')
            if isinstance(offsets,str):
                try:offsets=json.loads(offsets)
                except ValueError:continue
            if not isinstance(offsets,dict):continue
            for topic,parts in offsets.items():
                if topic not in TOPICS or not isinstance(parts,dict):continue
                for partition,offset in parts.items():
                    if isinstance(offset,int) and offset>=0:result[(topic,int(partition))]=offset
        return result,progress.get('batchId')
    return {},None


class KafkaLag:
    def __init__(self,bootstrap,fetch=None):
        self.bootstrap=bootstrap;self.fetch=fetch;self.admin=None

    def sample(self,report):
        offsets,batch=bronze_offsets(report)
        if not offsets:return {'state':'no_checkpoint_offsets','pending':None}
        try:
            if self.fetch is not None:ends=self.fetch(offsets)
            else:
                from confluent_kafka import TopicPartition
                from confluent_kafka.admin import AdminClient,OffsetSpec
                if self.admin is None:self.admin=AdminClient({'bootstrap.servers':self.bootstrap,'socket.timeout.ms':2000})
                futures=self.admin.list_offsets({TopicPartition(t,p):OffsetSpec.latest() for t,p in offsets},request_timeout=2)
                deadline=time.monotonic()+3
                ends={(tp.topic,tp.partition):future.result(timeout=max(.01,deadline-time.monotonic())).offset for tp,future in futures.items()}
            if any(k not in ends or ends[k]<v for k,v in offsets.items()):return {'state':'offset_reset_or_missing','pending':None}
            topics={t:sum(ends[k]-v for k,v in offsets.items() if k[0]==t) for t in {k[0] for k in offsets}}
            return {'state':'ok','pending':sum(topics.values()),'per_topic':topics,'partitions':len(offsets),
                    'source_batch_id':batch,'method':'broker_log_end_minus_bronze_last_completed_end_offset'}
        except Exception as e:return {'state':'error','pending':None,'error_type':type(e).__name__}


def runtime_config(path,ui_url):
    """A narrow operator-captured manifest is valid only for the current Spark application."""
    if not path or not ui_url:return {'state':'unconfigured'}
    try:
        manifest=json.loads(Path(path).read_text(encoding='utf-8-sig'))
        with urllib.request.urlopen(ui_url.rstrip('/')+'/api/v1/applications',timeout=2) as response:apps=json.load(response)
        if not any(app.get('id')==manifest.get('application_id') for app in apps):return {'state':'app_changed'}
        if manifest.get('verification')!='process_env_and_verified_code_default':return {'state':'unverified'}
        hours=manifest.get('effective_dedup_hours')
        if not isinstance(hours,int) or hours<1 or hours>8760:return {'state':'unverified'}
        return {'state':'ok','effective_dedup_hours':hours,'captured_at':manifest.get('captured_at'),
                'source':'verified_runtime_manifest','source_sha256':manifest.get('source_sha256')}
    except Exception as e:return {'state':'unavailable','error_type':type(e).__name__}
