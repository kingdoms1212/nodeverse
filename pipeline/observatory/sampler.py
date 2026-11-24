"""Read-only operational sampling. Writes only observatory-owned time series."""
from __future__ import annotations
import json
import math
import re
import time
import urllib.request
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from common import settings
from common.db import local_mongo, remote_mongo
from observatory.metrics import DeltaCommits, KafkaLag, read_latency, runtime_config

STATES = ('pending', 'processing', 'waiting_context', 'retrying', 'parked', 'blocked', 'quarantined', 'stored')
NAMES = {'bronze': 'Bronze', 'silver_learning_events': 'Silver 학습', 'silver_update_events': 'Silver 업데이트',
         'silver_audit': '충돌 감사', 'gold_1m': 'Gold 1분', 'gold_alert': 'Gold 경고',
         'gold_update': 'Gold 업데이트', 'remote_runtime': '런타임 전달'}


def utc():
    return datetime.now(timezone.utc)


def iso(value):
    return value.astimezone(timezone.utc).isoformat()


def date(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)


def age(value, now):
    value = date(value)
    return max(0, (now-value).total_seconds()) if value else None


def number(value):
    try:
        n = float(value)
        return n if math.isfinite(n) and n >= 0 else None
    except (TypeError, ValueError):
        return None


def summarize_progress(report, now):
    """Expose an allowlist, never Spark source descriptions, hostnames, or raw payloads."""
    queries = []
    for q in report.get('queries', []):
        p = q.get('progress') or {}
        status = q.get('status') or {}
        ops = p.get('stateOperators') or []
        duration = p.get('durationMs') or {}
        queries.append({'name': q.get('name'), 'label': NAMES.get(q.get('name'), '스트리밍 쿼리'),
            'active': bool(q.get('active')), 'has_data': bool(status.get('isDataAvailable')),
            'trigger_active': bool(status.get('isTriggerActive')), 'batch_id': p.get('batchId'),
            'batch_at': p.get('timestamp'), 'batch_age_s': age(p.get('timestamp'), now),
            'input_rows': number(p.get('numInputRows')), 'input_rps': number(p.get('inputRowsPerSecond')),
            'processed_rps': number(p.get('processedRowsPerSecond')),
            'batch_ms': number(duration.get('triggerExecution')), 'add_batch_ms': number(duration.get('addBatch')),
            'state_rows': sum(number(op.get('numRowsTotal')) or 0 for op in ops),
            'state_bytes': sum(number(op.get('memoryUsedBytes')) or 0 for op in ops),
            'dropped_by_watermark': sum(number(op.get('numRowsDroppedByWatermark')) or 0 for op in ops)})
    return queries


class AlertBook:
    """In-page incidents only. No outbound messages. Repeat samples update one incident."""
    def __init__(self):
        self.active = {}
        self.events = deque(maxlen=100)
        self.candidates = {}

    def update(self, findings, now, unknown=()):
        present = {item['id']: item for item in findings}
        for ident in list(self.candidates):
            if ident not in present:
                self.candidates.pop(ident)
        for ident in list(self.active):
            if ident not in present and ident not in unknown:
                item = self.active.pop(ident)
                self.events.appendleft(dict(item, phase='resolved', at=iso(now)))
        for ident, item in present.items():
            self.candidates[ident] = self.candidates.get(ident, 0) + 1
            # Stopped services report immediately; performance warnings need persistence.
            if self.candidates[ident] < item.get('samples', 1):
                continue
            clean = {k: v for k, v in item.items() if k != 'samples'}
            if ident not in self.active:
                clean.update(first_seen=iso(now))
                self.events.appendleft(dict(clean, phase='opened', at=iso(now)))
                self.active[ident] = clean
            else:
                if self.active[ident]['level'] != clean['level']:
                    self.events.appendleft(dict(clean, first_seen=self.active[ident]['first_seen'], phase='changed', at=iso(now)))
                self.active[ident].update(clean)
        return list(self.active.values()), list(self.events)


def findings(snapshot):
    result = []
    def add(ident, level, title, detail, samples=1):
        result.append(dict(id=ident, level=level, title=title, detail=detail, samples=samples))
    if snapshot['progress']['state'] != 'ok':
        add('progress', 'error', 'Spark 관측 신호 확인 필요', '진행 파일이 없거나 갱신이 지연되고 있습니다.')
    if snapshot['mongo']['state'] != 'ok':
        add('local-mongo', 'error', '로컬 MongoDB 응답 없음', '대기열과 이력 값을 확인할 수 없습니다. 0건으로 해석하지 마세요.')
    if snapshot['collector']['state'] == 'error':
        add('collector', 'error', '수집기 준비 응답 실패', '수집기 준비 상태와 Kafka 연결을 확인하세요.')
    if snapshot['remote']['state'] == 'error':
        add('remote', 'error', '원격 MongoDB 응답 실패', '원격 연결이 복구될 때까지 적재 상태를 확인하세요.')
    for q in snapshot['queries']:
        if not q['active']:
            add('stopped:'+str(q['name']), 'error', q['label']+' 쿼리 중단', '재시작 횟수와 오류 로그를 확인하세요.')
        elif q['has_data'] and (q['batch_age_s'] or 0) > 60:
            add('stalled:'+str(q['name']), 'error', q['label']+' 진행 지연', '대기 데이터가 있지만 완료 배치가 60초 이상 오래됐습니다.', 2)
        elif (q['input_rows'] or 0) > 0 and (q['batch_ms'] or 0) > 5000 and (q['batch_age_s'] or 0) < 60:
            add('slow:'+str(q['name']), 'warning', q['label']+' 배치 지연', '입력 있는 최근 배치가 5초를 넘었습니다. 지속 여부를 점검하세요.', 3)
    queue = snapshot['queue']
    if queue.get('counts') is not None:
        if queue['counts'].get('blocked', 0):
            add('blocked', 'error', '적재 중단 이벤트가 있습니다', 'ID 충돌 등 blocked 원인을 확인하고 승인된 재처리 절차를 사용하세요.')
        if queue['counts'].get('parked', 0):
            add('parked', 'warning', '재시도 보관 이벤트가 있습니다', '원인이 해결됐는지 확인한 뒤 재처리하세요.')
        if (queue.get('oldest_due_age_s') or 0) > 60:
            add('queue-age', 'warning', '적재 대기 시간이 길어지고 있습니다', '처리 가능한 대기열의 최고 대기 시간이 60초를 넘었습니다.', 3)
        if (queue.get('oldest_context_age_s') or 0) > 600:
            add('context-age', 'warning', '학습 기록이 조건을 기다리고 있습니다', '회원 연결 또는 앞선 순서 대기로 10분 넘게 분석 화면에 반영되지 않은 기록이 있습니다.', 2)
    return result


class Sampler:
    def __init__(self, db=None, remote=None, cfg=None, clock=utc):
        self.cfg = cfg or settings.get()
        self.db = db
        self.remote_db = remote
        self.clock = clock
        self.samples = deque(maxlen=2160)
        self.alerts = AlertBook()
        self.snapshot = None
        self.last_collector = None
        self.collector_result = {'state': 'unknown'}
        self.history_available = False
        self.last_failure = None
        self.delta_commits = DeltaCommits(self.cfg.lake_root)
        self.kafka_lag = KafkaLag(self.cfg.kafka_bootstrap)
        self.metadata_at = None
        self.metadata = {}

    def _database(self):
        if self.db is None:
            self.db = local_mongo()
        return self.db

    def load_history(self):
        try:
            saved = self._database().observatory_alert_state.find_one({'_id': 'current'})
            if saved:
                self.alerts.active = {item['id']: item for item in saved.get('active', [])}
                self.alerts.events.extend(saved.get('events', [])[:100])
        except Exception:
            pass
        try:
            since = self.clock()-timedelta(hours=6)
            rows = list(self._database().observatory_samples.find({'at': {'$gte': since}}, {'_id': 0})
                        .sort('at', -1).limit(2160).max_time_ms(1000))
            for row in reversed(rows):
                row['at'] = iso(date(row['at']))
                self.samples.append(row)
        except Exception:
            pass  # a new or unavailable DB starts with an empty chart, never fabricated points

    def _collector(self):
        if self.last_collector is None or time.monotonic()-self.last_collector > 30:
            self.last_collector = time.monotonic()
            if not self.cfg.observatory_collector_url:
                self.collector_result = {'state': 'unconfigured'}
            else:
                try:
                    request = urllib.request.Request(self.cfg.observatory_collector_url.rstrip('/')+'/health/ready')
                    with urllib.request.urlopen(request, timeout=2) as response:
                        self.collector_result = {'state': 'ok' if response.status == 200 else 'error'}
                except Exception:
                    self.collector_result = {'state': 'error'}
        return dict(self.collector_result)

    def sample(self):
        now = self.clock()
        started = time.monotonic()
        out = {'at': iso(now), 'queries': [], 'progress': {'state': 'unknown', 'age_s': None},
               'mongo': {'state': 'unknown'}, 'remote': {'state': 'unconfigured'}, 'collector': self._collector(),
               'queue': {'counts': None, 'oldest_due_age_s': None, 'source': 'unavailable'},
               'runtime': {'sink_failures': None, 'query_restarts': None, 'gold_republish_pending': None},
               'stage_timings': {}, 'effective_dedup_hours': None,
               'coverage': {'end_to_end_latency': False, 'merge_scan_bytes': False, 'kafka_lag': False}}
        try:
            report = json.loads(Path(self.cfg.progress_path).read_text(encoding='utf-8-sig'))
            a = age(report.get('updated_at'), now)
            out['progress'] = {'state': 'ok' if a is not None and a <= 30 else 'stale', 'age_s': a}
            out['queries'] = summarize_progress(report, now)
            out['runtime'] = {'sink_failures': (report.get('sink_failures') or {}).get('count'),
                'query_restarts': sum((report.get('query_restarts') or {}).values()) if 'query_restarts' in report else None,
                'gold_republish_pending': report.get('gold_republish_pending')}
            out['stage_timings'] = {name: {k: number(row.get(k)) for k in
                ['batch_id', 'merge_ms', 'outbox_ms', 'maintenance_ms', 'plan_ms', 'merge_groups', 'input_rows']} | {'completed_at': row.get('completed_at')}
                for name, row in (report.get('stage_timings') or {}).items() if name in NAMES}
            out['effective_dedup_hours'] = report.get('effective_dedup_hours')
        except Exception:
            out['progress']['state'] = 'unavailable'
            report = {}
        if self.metadata_at is None or time.monotonic()-self.metadata_at >= 30:
            self.metadata_at = time.monotonic()
            self.metadata = {'checked_at': iso(now), 'merge': self.delta_commits.sample(),
                'kafka': self.kafka_lag.sample(report),
                'deployment': runtime_config(self.cfg.observatory_deployment_path, self.cfg.observatory_spark_ui_url)}
            try:
                self.metadata['latency'] = read_latency(self._database(), now)
            except Exception as e:
                self.metadata['latency'] = {'state': 'unavailable', 'groups': {}, 'error_type': type(e).__name__}
        out['diagnostics'] = self.metadata
        if out['effective_dedup_hours'] is None and self.metadata['deployment'].get('state') == 'ok':
            out['effective_dedup_hours'] = self.metadata['deployment']['effective_dedup_hours']
        out['dedup_source'] = 'driver_progress' if report.get('effective_dedup_hours') is not None else self.metadata['deployment'].get('source')
        out['coverage']['kafka_lag'] = self.metadata['kafka'].get('state') == 'ok'
        out['coverage']['merge_scan_bytes'] = self.metadata['merge'].get('scan_bytes') is not None
        try:
            db = self._database()
            db.command('ping')
            out['mongo'] = {'state': 'ok'}
            published = db.pipeline_sink_status.find_one({'_id': 'remote_writer'}, {'states': 1, 'updated_at': 1})
            if published and (age(published.get('updated_at'), now) or 0) <= 30 and date(published.get('updated_at')):
                out['queue'] = {'counts': {s: int((published.get('states') or {}).get(s, 0)) for s in STATES},
                    'oldest_due_age_s': None, 'source': 'writer_snapshot'}
            else:
                # Existing state-prefixed index; no full collection $group per dashboard refresh.
                counts = {s: db.remote_event_outbox.count_documents({'state': s}, maxTimeMS=1000) for s in STATES}
                out['queue'] = {'counts': counts, 'oldest_due_age_s': None, 'source': 'indexed_fallback'}
            oldest = db.remote_event_outbox.find_one({'state': {'$in': ['pending', 'processing', 'retrying']}},
                {'queued_at': 1}, sort=[('queued_at', 1)], max_time_ms=1000)
            out['queue']['oldest_due_age_s'] = age(oldest.get('queued_at'), now) if oldest else 0
            context = db.remote_event_outbox.find_one({'state': 'waiting_context'},
                {'queued_at': 1}, sort=[('queued_at', 1)], max_time_ms=1000)
            out['queue']['oldest_context_age_s'] = age(context.get('queued_at'), now) if context else 0
            health = db.remote_sink_health.find_one({'_id': 'worker'}, {'state': 1, 'updated_at': 1})
            out['queue']['worker_age_s'] = age(health.get('updated_at'), now) if health else None
            summary = db.rt_summary.find_one({'_id': 'global'}, {'events_last_1m': 1, 'late_last_1m': 1, 'updated_at': 1}) or {}
            out['received_last_1m'] = summary.get('events_last_1m')
            out['received_age_s'] = age(summary.get('updated_at'), now)
            out['late_last_1m'] = summary.get('late_last_1m')
        except Exception as e:
            # No exception text: a URI/password could appear in driver errors.
            out['mongo'] = {'state': 'error', 'error_type': type(e).__name__}
            out['queue'] = {'counts': None, 'oldest_due_age_s': None, 'source': 'unavailable'}
        if self.cfg.direct_mongo_uri:
            try:
                target = self.remote_db if self.remote_db is not None else remote_mongo()
                target.command('ping')
                out['remote'] = {'state': 'ok'}
            except Exception as e:
                out['remote'] = {'state': 'error', 'error_type': type(e).__name__}
        unknown = set()
        if out['mongo']['state'] != 'ok':
            unknown.update(('blocked', 'parked', 'queue-age', 'context-age'))
        if out['progress']['state'] != 'ok':
            unknown.update(k for k in self.alerts.active if k.startswith(('stopped:', 'stalled:', 'slow:')))
        active, events = self.alerts.update(findings(out), now, unknown)
        out['alerts'] = active
        out['incidents'] = events
        try:
            self._database().observatory_alert_state.replace_one({'_id': 'current'},
                {'_id': 'current', 'active': active, 'events': events, 'updated_at': now}, upsert=True)
            out['alerts_persistent'] = True
        except Exception:
            out['alerts_persistent'] = False
        out['sample_ms'] = round((time.monotonic()-started)*1000)
        self.snapshot = out
        q = next((x for x in out['queries'] if x['name']=='silver_learning_events'), {})
        bronze = next((x for x in out['queries'] if x['name']=='bronze'), {})
        point = {'at': out['at'], 'input_rps': bronze.get('input_rps'), 'processed_rps': q.get('processed_rps'),
                 'silver_batch_ms': q.get('batch_ms') if (q.get('input_rows') or 0)>0 else None,
                 'state_rows': q.get('state_rows'), 'state_bytes': q.get('state_bytes'),
                 'queue': sum(out['queue']['counts'].get(s,0) for s in ['pending','processing','retrying']) if out['queue']['counts'] is not None else None,
                 'oldest_due_age_s': out['queue']['oldest_due_age_s']}
        point.update(kafka_pending=self.metadata['kafka'].get('pending'),
                     merge_commit_ms=self.metadata['merge'].get('execution_ms'))
        self.samples.append(point)
        try:
            db = self._database()
            if not self.history_available:
                db.observatory_samples.create_index('at', expireAfterSeconds=172800)
                self.history_available = True
            db.observatory_samples.insert_one(dict(point, at=now))
        except Exception:
            out['history_persistent'] = False
        else:
            out['history_persistent'] = True
        return out

    def history(self, minutes=15):
        since = self.clock()-timedelta(minutes=minutes)
        fields = {'at', 'input_rps', 'processed_rps', 'silver_batch_ms', 'state_rows', 'state_bytes', 'queue', 'oldest_due_age_s', 'kafka_pending', 'merge_commit_ms'}
        rows = [{k: v for k, v in p.items() if k in fields} for p in self.samples if date(p['at']) >= since]
        # In-memory history keeps the API independent of database availability and load.
        step = max(1, math.ceil(len(rows)/360))
        result = rows[::step]
        if rows and result[-1] != rows[-1]:
            result.append(rows[-1])
        return result
