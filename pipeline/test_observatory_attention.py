import json
import mongomock
from fastapi.testclient import TestClient
from observatory.app import create_app
from observatory.sampler import Sampler
from observatory.attention import attention_records
from test_observatory import NOW, config


def test_attention_requires_index_and_redacts_freeform():
    db = mongomock.MongoClient(tz_aware=True).local
    assert attention_records(db, 'blocked')['state'] == 'index_required'
    db.remote_event_outbox.create_index([('state', 1), ('next_attempt_at', 1)])
    db.remote_event_outbox.insert_many([{'_id': str(i), 'state': 'blocked', 'reason': 'password=private',
        'event': {'player_id': 'private-player'}, 'queued_at': NOW} for i in range(25)])
    result = attention_records(db, 'blocked')
    assert len(result['records']) == 20 and result['more']
    assert 'private' not in json.dumps(result) and 'player_id' not in json.dumps(result)
    assert result['records'][0]['reason_code'] == 'review_required'


def test_attention_auth_validation_and_safe_reason(tmp_path):
    db = mongomock.MongoClient(tz_aware=True).local
    db.remote_event_outbox.create_index([('state', 1), ('next_attempt_at', 1)])
    db.remote_event_outbox.insert_one({'_id': 'private-id', 'state': 'waiting_context',
        'reason': 'sequence_gap:expected_99', 'queued_at': NOW, 'archive_stored': True})
    sampler = Sampler(db=db, cfg=config(tmp_path, observatory_key='test-key'))
    with TestClient(create_app(sampler, background=False)) as client:
        assert client.get('/api/attention?state=waiting_context').status_code == 401
        headers = {'X-Observatory-Key': 'test-key'}
        assert client.get('/api/attention?state=stored', headers=headers).status_code == 422
        response = client.get('/api/attention?state=waiting_context', headers=headers)
        assert response.status_code == 200
        row = response.json()['records'][0]
        assert row['reason_code'] == 'sequence_gap' and row['archive_stored']
        assert 'private-id' not in response.text and 'expected_99' not in response.text
