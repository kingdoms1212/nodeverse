import json
from dataclasses import replace
from datetime import datetime,timedelta,timezone
import mongomock
from fastapi.testclient import TestClient
from common.settings import Settings
from observatory.sampler import Sampler,AlertBook,findings,summarize_progress
from observatory.app import create_app

NOW=datetime(2026,11,23,0,0,tzinfo=timezone.utc)

def config(tmp_path, **kw):
 return replace(Settings(),progress_path=str(tmp_path/'progress.json'),observatory_collector_url='',direct_mongo_uri='',**kw)

def report(**kw):
 p={'timestamp':NOW.isoformat(),'batchId':1,'numInputRows':0,'inputRowsPerSecond':0,'processedRowsPerSecond':0,'durationMs':{'triggerExecution':20},'stateOperators':[{'numRowsTotal':7,'memoryUsedBytes':8192}]}
 p.update(kw)
 return {'updated_at':NOW.isoformat(),'queries':[{'name':'silver_learning_events','active':True,'status':{'isDataAvailable':False},'progress':p}]}

def test_progress_allowlist_and_idle():
 r=report(timestamp=(NOW-timedelta(hours=2)).isoformat(),sources=[{'description':'secret-db-uri'}])
 q=summarize_progress(r,NOW)
 assert q[0]['state_rows']==7 and q[0]['input_rows']==0
 assert 'secret-db-uri' not in json.dumps(q)
 s={'queries':q,'progress':{'state':'ok'},'mongo':{'state':'ok'},'collector':{'state':'ok'},'remote':{'state':'ok'},'queue':{}}
 assert not findings(s)

def test_bad_rate_is_unknown_not_nan():
 q=summarize_progress(report(inputRowsPerSecond=float('nan'),processedRowsPerSecond=-1),NOW)[0]
 assert q['input_rps'] is None and q['processed_rps'] is None

def test_incident_dedup_and_recovery():
 book=AlertBook();issue={'id':'delay','level':'warning','title':'delay','detail':'reason','samples':2}
 assert book.update([issue],NOW)[0]==[]
 a,e=book.update([issue],NOW+timedelta(seconds=10));assert len(a)==len(e)==1
 a,e=book.update([issue],NOW+timedelta(seconds=20));assert len(a)==len(e)==1
 a,e=book.update([],NOW+timedelta(seconds=30));assert len(a)==0 and e[0]['phase']=='resolved' and len(e)==2

def test_missing_sources_are_unknown_and_no_payload_exposure(tmp_path):
 class Broken:
  def command(self,*a):raise RuntimeError('password=do-not-expose')
 s=Sampler(db=Broken(),cfg=config(tmp_path),clock=lambda:NOW)
 out=s.sample()
 assert out['queue']['counts'] is None and out['progress']['state']=='unavailable'
 assert 'do-not-expose' not in json.dumps(out)
 assert len(out['alerts'])>=2

def test_history_bound_and_auth(tmp_path):
 s=Sampler(cfg=config(tmp_path,observatory_key='private-test-key'),clock=lambda:NOW)
 s.snapshot={'at':NOW.isoformat(),'queue':{'counts':None}}
 s.samples.extend([{'at':(NOW-timedelta(minutes=30)).isoformat(),'queue':1},{'at':NOW.isoformat(),'queue':2}])
 with TestClient(create_app(s,background=False)) as c:
  assert c.get('/api/snapshot').status_code==401
  assert c.get('/api/history').status_code==401
  headers={'X-Observatory-Key':'private-test-key'}
  assert c.get('/api/snapshot',headers=headers).status_code==200
  assert len(c.get('/api/history?minutes=15',headers=headers).json()['points'])==1
  assert c.get('/api/history?minutes=9999',headers=headers).status_code==422
  assert 'private-test-key' not in c.get('/api/config').text
  assert c.post('/api/requeue',headers=headers).status_code==404

def test_empty_api_is_503(tmp_path):
 s=Sampler(cfg=config(tmp_path),clock=lambda:NOW)
 with TestClient(create_app(s,background=False)) as c:
  assert c.get('/api/snapshot').status_code==503
  assert c.get('/').status_code==200
  assert c.get('/static/app.js').status_code==200
  assert "default-src 'self'" in c.get('/').headers['Content-Security-Policy']

def test_history_restart_only_observatory_data(tmp_path):
 db=mongomock.MongoClient(tz_aware=True).local
 db.observatory_samples.insert_one({'at':NOW,'queue':3})
 db.gameplay_events.insert_one({'_id':'untouched','secret':'private'})
 s=Sampler(db=db,cfg=config(tmp_path),clock=lambda:NOW)
 s.load_history();assert len(s.samples)==1 and s.history()[0]['queue']==3
 assert db.gameplay_events.find_one()['_id']=='untouched'

def test_slow_batch_requires_recent_input():
 q=summarize_progress(report(numInputRows=10,timestamp=(NOW-timedelta(minutes=10)).isoformat(),durationMs={'triggerExecution':6000}),NOW)
 s={'queries':q,'progress':{'state':'ok'},'mongo':{'state':'ok'},'collector':{'state':'ok'},'remote':{'state':'ok'},'queue':{}}
 assert not findings(s)

def test_old_context_warning_does_not_label_idle_as_stopped():
 s={'queries':[],'progress':{'state':'ok'},'mongo':{'state':'ok'},'collector':{'state':'ok'},'remote':{'state':'ok'},'queue':{'counts':{'waiting_context':19},'oldest_context_age_s':3600,'oldest_due_age_s':0}}
 result=findings(s)
 assert len(result)==1 and result[0]['id']=='context-age' and result[0]['level']=='warning'

def test_history_allowlist_and_last_sample(tmp_path):
 s=Sampler(cfg=config(tmp_path),clock=lambda:NOW)
 for i in range(1000):s.samples.append({'at':(NOW-timedelta(seconds=1000-i)).isoformat(),'queue':i,'private':'must-not-leak'})
 h=s.history(30)
 assert len(h)<=361 and h[-1]['queue']==999
 assert all('private' not in row for row in h)

def test_stage_registry_snapshot_is_not_mutable_reference():
 from common.observability import record_stage,stages
 record_stage('silver_test',batch_id=2,merge_ms=15)
 copy=stages();copy['silver_test']['merge_ms']=99
 assert stages()['silver_test']['merge_ms']==15

def test_non_loopback_requires_private_key(tmp_path):
 import pytest
 s=Sampler(cfg=config(tmp_path,observatory_bind='0.0.0.0',observatory_key=''),clock=lambda:NOW)
 with pytest.raises(RuntimeError,match='OBSERVATORY_KEY'):create_app(s,background=False)
