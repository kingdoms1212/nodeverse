"""Bounded operational examples; never return raw events or free-form errors."""
import hashlib
from datetime import datetime, timezone

ATTENTION_STATES = ('waiting_context', 'parked', 'blocked', 'quarantined')
REASONS = {
    'member_mapping_required': '회원 UUID 연결 필요',
    'sequence_gap': '앞선 순서 이벤트 대기',
    'other_database_writer_active': '다른 적재 작업자 사용 중',
    'retry_limit': '자동 재시도 한도 초과',
    'event_id_conflict': '같은 이벤트 ID의 내용 충돌',
    'problem_context_required': '문제 실행 연결 정보 필요',
    'failure_reason_required': '실패 사유 필수 필드 누락',
    'operation_required': '행동 종류 필수 필드 누락',
}


def attention_records(db, state):
    collection = db.remote_event_outbox
    if 'state_1_next_attempt_at_1' not in collection.index_information():
        return {'state': 'index_required', 'records': []}
    projection = {'_id': 1, 'state': 1, 'reason': 1, 'result.reason': 1,
                  'queued_at': 1, 'updated_at': 1, 'attempts': 1, 'archive_stored': 1}
    rows = collection.find({'state': state}, projection).hint('state_1_next_attempt_at_1').limit(21).max_time_ms(1000)
    result = []
    for row in rows:
        raw = row.get('reason') or (row.get('result') or {}).get('reason') or ''
        reason = raw.split(':', 1)[0] if isinstance(raw, str) else ''
        item = {'reference': hashlib.sha256(str(row['_id']).encode()).hexdigest()[:12],
                'reason_code': reason if reason in REASONS else 'review_required',
                'reason_label': REASONS.get(reason, '개별 원본·작업자 로그 점검 필요'),
                'archive_stored': row.get('archive_stored') if isinstance(row.get('archive_stored'), bool) else None,
                'attempts': row.get('attempts') if isinstance(row.get('attempts'), int) else None}
        for field in ('queued_at', 'updated_at'):
            value = row.get(field)
            item[field] = value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc).isoformat() if isinstance(value, datetime) else None
        result.append(item)
    return {'state': 'ok', 'records': result[:20], 'more': len(result) > 20,
            'order': 'index_sample', 'filter': state}
