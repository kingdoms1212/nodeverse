import hashlib,uuid
from datetime import datetime,timedelta,timezone
import mongomock
import pytest
import remote.projection as projection
from remote.projection import Projector,Waiting,Conflict,uid
from remote.outbox import stage
@pytest.fixture
def dbs(monkeypatch):
 c=mongomock.MongoClient(tz_aware=True)
 monkeypatch.setattr(projection,"resolve_member",lambda *a:"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
 return c.local,c.target

def rows():
 session,run,attempt=[str(uuid.uuid4()) for _ in range(3)];start=datetime.now(timezone.utc).replace(microsecond=0)
 return [{"schema_version":2,"event_id":str(uuid.uuid4()),"payload_hash":hashlib.sha256(str(i).encode()).hexdigest(),"player_id":"player-1","session_id":session,"run_id":run,"attempt_id":attempt,"minigame_id":"ST03","category":"DS","build_version":"0.3.0","content_version":"base","event_type":et,"sequence":i,"occurred_at":(start+timedelta(milliseconds=i)).isoformat(),"received_at":(start+timedelta(milliseconds=i)).isoformat(),"details":{"level":1,"operation":"push"}} for i,et in enumerate(["problem_started","attempt_started","operation_succeeded","attempt_succeeded","problem_completed"],1)]

def test_projection_and_replay(dbs):
 local,target=dbs;p=Projector(local,target,None,{});events=rows()
 for e in events:p.project(e)
 for e in events:p.project(e)
 assert target.game_sessions.count_documents({})==1
 assert target.problem_runs.count_documents({"status":"completed"})==1
 assert target.solution_attempts.count_documents({"outcome":"success"})==1
 assert target.gameplay_events.count_documents({})==5
 run=target.problem_runs.find_one();session=target.game_sessions.find_one()
 assert session["player_id"]=="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
 assert run["session_id"]==session["_id"] and run["task_id"]==target.game_problems.find_one()["_id"]
 assert target.game_problems.find_one()["task_id"]=="ST03"
 assert run["level"]==1
 assert run["active_ms"] is None and run["paused_ms"] is None
 assert isinstance(target.gameplay_events.find_one()["occurred_at"],datetime)

def test_out_of_order(dbs):
 l,t=dbs;p=Projector(l,t,None,{});e=rows()
 with pytest.raises(Waiting,match="sequence_gap"):p.project(e[1])
 p.project(e[0]);p.project(e[1]);assert t.solution_attempts.count_documents({})==1

def test_same_sequence_changed_id(dbs):
 l,t=dbs;p=Projector(l,t,None,{});e=rows()[0];p.project(e)
 with pytest.raises(Conflict):p.project(dict(e,event_id=str(uuid.uuid4()),payload_hash="changed"))

def test_unknown_member_keeps_archive(dbs,monkeypatch):
 def unknown(*a):raise Waiting("member_mapping_required")
 monkeypatch.setattr(projection,"resolve_member",unknown);l,t=dbs
 with pytest.raises(Waiting):Projector(l,t,None,{}).project(rows()[0])
 assert t.pipeline_learning_events.count_documents({})==1 and t.game_sessions.count_documents({})==0

def test_stage_never_resets_stored(dbs):
 l,_=dbs;e=rows()[0];stage([e],l);l.remote_event_outbox.update_one({"_id":e["event_id"]},{"$set":{"state":"stored"}})
 stage([e],l);assert l.remote_event_outbox.find_one()["state"]=="stored"
 stage([dict(e,payload_hash="other")],l)
 assert l.remote_event_outbox.find_one()["payload_hash"]==e["payload_hash"] and l.remote_export_conflicts.count_documents({})==1

def test_child_session_identity():
 assert uid("session","s","p","ST03")==uid("session","s","p","ST03")
 assert uid("session","s","p","ST03")!=uid("session","s","p","ST04")


def test_worker_lease_prevents_two_owners(dbs):
 from remote.worker import acquire
 from datetime import datetime,timedelta,timezone
 local,_=dbs
 assert acquire(local,"owner-a")
 assert not acquire(local,"owner-b")
 local.remote_worker_lease.update_one({"_id":"writer"},{"$set":{"expires_at":datetime.now(timezone.utc)-timedelta(seconds=1)}})
 assert acquire(local,"owner-b")

def test_resolve_unknown_member_never_inserts():
 class Cursor:
  def __init__(self):self.queries=[]
  def __enter__(self):return self
  def __exit__(self,*a):pass
  def execute(self,query,args):self.queries.append(query)
  def fetchall(self):return []
 class SQL:
  def __init__(self):self.c=Cursor();self.rollbacks=0
  def cursor(self):return self.c
  def rollback(self):self.rollbacks+=1
 sql=SQL()
 with pytest.raises(Waiting,match="member_mapping_required"):
  projection.resolve_member(sql,"unbound-install-id",{})
 assert not any("INSERT" in q for q in sql.c.queries) and sql.rollbacks==1

def test_bad_initial_event_quarantined_then_valid_start_proceeds(dbs):
 l,t=dbs;p=Projector(l,t,None,{});es=rows()
 bad=dict(es[0],event_type='operation_succeeded',details={},run_id=str(uuid.uuid4()))
 assert p.project(bad)['scope']=='learning_quarantine'
 assert t.game_sessions.count_documents({})==0
 # A new valid sequence begins with the actual problem start at #2.
 valid=[dict(e,sequence=e['sequence']+1) for e in es]
 valid[0]['event_id']=str(uuid.uuid4());valid[0]['payload_hash']='valid-start'
 for e in valid:p.project(e)
 assert t.problem_runs.count_documents({'status':'completed'})==1
 assert t.gameplay_events.count_documents({})==5
 assert t.pipeline_quarantined_events.count_documents({})==1
 assert p.project(bad)['scope']=='learning_quarantine'
 assert l.remote_session_cursors.find_one()['last_sequence']==6


def test_missing_operation_does_not_block_completion(dbs):
 l,t=dbs;p=Projector(l,t,None,{});es=rows();es[2]['details']={}
 results=[p.project(e) for e in es]
 assert results[2]['reason']=='operation_required'
 assert t.gameplay_events.count_documents({})==4
 assert t.problem_runs.find_one()['status']=='completed'
 assert t.pipeline_learning_events.count_documents({})==5
 assert t.pipeline_quarantined_events.find_one()['event']['details']=={}


def test_quarantine_cannot_skip_missing_sequence(dbs):
 l,t=dbs;p=Projector(l,t,None,{});e=rows()[2];e['details']={}
 with pytest.raises(Waiting,match='sequence_gap'):p.project(e)
 assert t.pipeline_quarantined_events.count_documents({})==0
 assert l.remote_session_cursors.count_documents({})==0


def test_quarantine_receipt_still_rejects_changed_payload(dbs):
 l,t=dbs;p=Projector(l,t,None,{});e=dict(rows()[0],event_type='operation_succeeded')
 p.project(e)
 with pytest.raises(Conflict):p.project(dict(e,payload_hash='changed'))
 assert t.pipeline_quarantined_events.count_documents({})==1


def test_due_queue_is_fair_not_sorted_by_source_sequence(dbs):
 from remote.worker import claim
 l,_=dbs;n=datetime.now(timezone.utc)
 l.remote_event_outbox.insert_many([
  {'_id':'retry','state':'waiting_context','sequence':1,'next_attempt_at':n-timedelta(seconds=1),'queued_at':n-timedelta(hours=1),'attempts':10},
  {'_id':'new','state':'pending','sequence':99,'next_attempt_at':n-timedelta(seconds=5),'queued_at':n-timedelta(seconds=5),'attempts':0}])
 assert claim(l)['_id']=='new'
 assert claim(l)['_id']=='retry'

def test_predecessor_wakes_later_rows_of_its_session_only(dbs):
 # 8th round: the waiting row and the rest of its group (pending, future due) wake together; other sessions keep waiting
 l,t=dbs;es=rows();future=datetime.now(timezone.utc)+timedelta(hours=1)
 l.remote_event_outbox.insert_one({'_id':es[1]['event_id'],'source_session_id':es[1]['session_id'],'sequence':2,'state':'waiting_context','reason':'sequence_gap:expected_1','next_attempt_at':future})
 l.remote_event_outbox.insert_one({'_id':es[2]['event_id'],'source_session_id':es[2]['session_id'],'sequence':3,'state':'pending','next_attempt_at':future})
 l.remote_event_outbox.insert_one({'_id':'other','source_session_id':'another-session','sequence':2,'state':'waiting_context','reason':'sequence_gap:expected_1','next_attempt_at':future})
 l.remote_event_outbox.insert_one({'_id':'member','source_session_id':es[0]['session_id'],'sequence':4,'state':'waiting_context','reason':'member_mapping_required','next_attempt_at':future})
 Projector(l,t,None,{}).project(es[0])
 soon=future-timedelta(minutes=30)
 assert l.remote_event_outbox.find_one({'sequence':2,'source_session_id':es[0]['session_id']})['next_attempt_at']<soon
 assert l.remote_event_outbox.find_one({'sequence':3})['next_attempt_at']<soon
 assert l.remote_event_outbox.find_one({'_id':'other'})['next_attempt_at']>soon
 assert l.remote_event_outbox.find_one({'_id':'member'})['next_attempt_at']>soon  # not a sequence wait


def test_claim_batch_marks_processing_and_counts_attempts(dbs):
 from remote.worker import claim_batch
 l,_=dbs;n=datetime.now(timezone.utc)
 l.remote_event_outbox.insert_many([{'_id':str(i),'state':'pending','sequence':i,'next_attempt_at':n-timedelta(seconds=1),'queued_at':n,'attempts':0} for i in range(5)]
  +[{'_id':'later','state':'pending','sequence':9,'next_attempt_at':n+timedelta(hours=1),'queued_at':n,'attempts':0},
    {'_id':'done','state':'stored','sequence':9,'next_attempt_at':n-timedelta(seconds=1),'queued_at':n,'attempts':1}])
 jobs=claim_batch(l,3)
 assert len(jobs)==3 and all(j['attempts']==1 for j in jobs)
 assert l.remote_event_outbox.count_documents({'state':'processing'})==3
 assert {j['_id'] for j in claim_batch(l,10)}=={'3','4'}
 assert claim_batch(l,10)==[]

def test_group_by_session_keeps_sequence_order():
 from remote.worker import group_by_session
 n=datetime.now(timezone.utc)
 jobs=[{'_id':'b2','source_session_id':'B','sequence':2,'queued_at':n},{'_id':'a3','source_session_id':'A','sequence':3,'queued_at':n},
       {'_id':'a1','source_session_id':'A','sequence':1,'queued_at':n},{'_id':'u','source_session_id':None,'sequence':0,'queued_at':n}]
 groups={tuple(j['_id'] for j in g) for g in group_by_session(jobs)}
 assert groups=={('a1','a3'),('b2',),('u',)}

def test_stage_bulk_inserts_once_per_event(dbs):
 l,_=dbs;es=rows()
 stage(es+[es[0]],l)
 assert l.remote_event_outbox.count_documents({})==len(es)
 assert l.remote_event_outbox.find_one({'_id':es[2]['event_id']})['sequence']==3

def test_group_requeues_rest_of_session_when_one_waits(dbs, monkeypatch):
 import remote.worker as w
 l,_=dbs;n=datetime.now(timezone.utc);later=n+timedelta(seconds=30)
 l.remote_event_outbox.insert_many([{'_id':f'e{i}','state':'processing','sequence':i,'attempts':1,'source_session_id':'S','queued_at':n} for i in (1,2,3)])
 calls=[]
 def fake(job,local,*a,**k):
  calls.append(job['_id']); return later if job['_id']=='e2' else None
 monkeypatch.setattr(w,'process_job',fake)
 w.process_group([{'_id':f'e{i}','sequence':i} for i in (1,2,3)], l)
 assert calls==['e1','e2']
 e3=l.remote_event_outbox.find_one({'_id':'e3'})
 assert e3['state']=='pending' and e3['attempts']==0


# ---------------------------------------------------------------- PL-9 parked rows
def _job(l, i, **extra):
 n=datetime.now(timezone.utc)
 doc={'_id':f'j{i}','state':'processing','sequence':i,'attempts':1,'source_session_id':'S','queued_at':n,
      'event':{'event_id':f'j{i}','schema_version':2,'session_id':'S'}}
 doc.update(extra);l.remote_event_outbox.insert_one(doc);return doc

class _Pool:
 def connection(self):
  class C:
   def close(self):pass
  return C()

def _raising(exc):
 class P:
  kind=staticmethod(lambda e:'learning')
  def __init__(self,*a):pass
  def archive(self,e):return 'learning'
  def project(self,e,archived=False):raise exc
 return P

def test_member_wait_parks_after_deadline(dbs, monkeypatch):
 import remote.worker as w
 l,t=dbs;monkeypatch.setattr(w,'Projector',_raising(Waiting('member_mapping_required')))
 fresh=_job(l,1)
 assert w.process_job(fresh,l,t,_Pool(),{},True) not in (None,w.PARKED_SENTINEL)
 assert l.remote_event_outbox.find_one({'_id':'j1'})['state']=='waiting_context'
 old=_job(l,2,waiting_since=datetime.now(timezone.utc)-timedelta(hours=25))
 assert w.process_job(old,l,t,_Pool(),{},True) is w.PARKED_SENTINEL
 row=l.remote_event_outbox.find_one({'_id':'j2'})
 assert row['state']=='parked' and row['reason']=='member_mapping_required' and 'next_attempt_at' not in row

def test_sequence_gap_parks_after_ten_minutes_and_retry_limit_parks(dbs, monkeypatch):
 import remote.worker as w
 l,t=dbs
 monkeypatch.setattr(w,'Projector',_raising(Waiting('sequence_gap:expected_3')))
 gap=_job(l,4,waiting_since=datetime.now(timezone.utc)-timedelta(minutes=11))
 assert w.process_job(gap,l,t,_Pool(),{},True) is w.PARKED_SENTINEL
 monkeypatch.setattr(w,'Projector',_raising(RuntimeError('db down')))
 tired=_job(l,5,attempts=20)
 assert w.process_job(tired,l,t,_Pool(),{},True) is w.PARKED_SENTINEL
 row=l.remote_event_outbox.find_one({'_id':'j5'})
 assert row['reason']=='retry_limit' and row['last_error']=='RuntimeError'

def test_parked_gap_successor_revives_when_predecessor_is_stored(dbs):
 l,t=dbs;es=rows()
 l.remote_event_outbox.insert_one({'_id':es[1]['event_id'],'source_session_id':es[1]['session_id'],'sequence':2,
   'state':'parked','reason':'sequence_gap:expected_1','parked_at':datetime.now(timezone.utc)})
 Projector(l,t,None,{}).project(es[0])
 row=l.remote_event_outbox.find_one({'sequence':2})
 assert row['state']=='pending' and 'parked_at' not in row

def test_requeue_and_park_tool(dbs):
 from remote.requeue import park, requeue, status
 l,_=dbs;n=datetime.now(timezone.utc)
 l.remote_event_outbox.insert_many([{'_id':'a','state':'waiting_context','reason':'member_mapping_required','event':{'player_id':'load-1'}},
  {'_id':'b','state':'waiting_context','reason':'sequence_gap:expected_2','event':{'player_id':'x'}}])
 assert park(l,'member_mapping_required',None)==1
 st=status(l)
 assert st['states']['parked']==1 and st['reasons']['parked'][0]['reason']=='member_mapping_required'
 assert requeue(l,'member',None)==1
 assert l.remote_event_outbox.find_one({'_id':'a'})['state']=='pending'


def test_status_counts_match_full_aggregate(dbs):
 # CPU review item 3: index counts must equal the former $group over the whole outbox
 from remote.requeue import status
 l,_=dbs
 states=['stored']*5+['pending']*3+['parked']*2+['waiting_context','blocked','quarantined']
 l.remote_event_outbox.insert_many([{'_id':f's{i}','state':s,'reason':'r'+str(i%2) if s in ('parked','waiting_context','blocked') else None}
   for i,s in enumerate(states)])
 old={d['_id']:d['n'] for d in l.remote_event_outbox.aggregate([{'$group':{'_id':'$state','n':{'$sum':1}}}])}
 st=status(l)
 assert st['states']==old
 assert sum(r['n'] for r in st['reasons']['parked'])==2

def test_archive_once_per_attempt_and_refilled_after_remote_replaced(dbs, monkeypatch):
 # process_job archives once per attempt and project(archived=True) does not repeat it (CPU review);
 # a row reprocessed after the remote DB was replaced/restored writes the archive again (reliability P1-5)
 import remote.worker as w
 from remote.requeue import requeue
 l,t=dbs;calls=[]
 real=w.Projector.archive
 def counting(self,e):
  calls.append(e['event_id']);return real(self,e)
 monkeypatch.setattr(w.Projector,'archive',counting)
 e=dict(rows()[0],schema_version=1);e.pop('session_id',None)  # runtime event: archive only, no SQL
 l.remote_event_outbox.insert_one({'_id':'once','event':e,'state':'processing','attempts':1,'queued_at':datetime.now(timezone.utc)})
 assert w.process_job(l.remote_event_outbox.find_one({'_id':'once'}),l,t,_Pool(),{},True) is None
 assert calls==[e['event_id']] and t.pipeline_runtime_events.count_documents({})==1
 t.pipeline_runtime_events.delete_many({})  # remote replaced by an empty / older backup
 assert requeue(l,None,None,state='stored')==1
 job=w.claim_batch(l,10)[0]
 assert job['remote_archive_stored_at']  # the old success time is still there
 assert w.process_job(job,l,t,_Pool(),{},True) is None
 assert t.pipeline_runtime_events.count_documents({'_id':e['event_id']})==1
 assert l.remote_event_outbox.find_one({'_id':'once'})['state']=='stored'

def test_claim_skips_row_parked_between_find_and_update(dbs, monkeypatch):
 # reliability P1-1: an operator park between the candidate read and the claim update must win
 import remote.worker as w
 from remote.requeue import park
 l,t=dbs;n=datetime.now(timezone.utc)
 l.remote_event_outbox.insert_many([{'_id':k,'state':'waiting_context','reason':'member_mapping_required','sequence':1,
   'source_session_id':k,'next_attempt_at':n-timedelta(seconds=1),'queued_at':n,'attempts':0,'event':{'player_id':k}} for k in ('a','b')])
 real=l.remote_event_outbox.update_many
 def racing(q,u,*a,**k):
  if u.get('$set',{}).get('state')=='processing':
   park(l,None,'a')  # lands just before our claim
  return real(q,u,*a,**k)
 monkeypatch.setattr(l.remote_event_outbox,'update_many',racing)
 jobs=w.claim_batch(l,10)
 monkeypatch.setattr(l.remote_event_outbox,'update_many',real)
 assert [j['_id'] for j in jobs]==['b']
 assert l.remote_event_outbox.find_one({'_id':'a'})['state']=='parked'

def test_old_worker_cannot_overwrite_after_reclaim(dbs, monkeypatch):
 # reliability P1-1: after recover_stale + a new claim, the late result of the first claim is dropped
 import remote.worker as w
 l,t=dbs;n=datetime.now(timezone.utc)
 e=dict(rows()[0],schema_version=1);e.pop('session_id',None)
 l.remote_event_outbox.insert_one({'_id':'r','event':e,'state':'pending','next_attempt_at':n-timedelta(seconds=1),'queued_at':n,'attempts':0})
 first=w.claim_batch(l,10)[0]
 w.recover_stale(l,True)
 second=w.claim_batch(l,10)[0]
 assert first['claim_id']!=second['claim_id']
 assert w.process_job(first,l,t,_Pool(),{},True) is None  # lost claim: nothing written
 row=l.remote_event_outbox.find_one({'_id':'r'})
 assert row['state']=='processing' and row['claim_id']==second['claim_id']
 assert w.process_job(second,l,t,_Pool(),{},True) is None
 assert l.remote_event_outbox.find_one({'_id':'r'})['state']=='stored'

def test_replayed_predecessor_wakes_successor_after_failed_wake(dbs, monkeypatch):
 # reliability P1-2: cursor advanced, then the wake failed; the predecessor's retry must wake the successor
 l,t=dbs;es=rows()
 p=Projector(l,t,None,{})
 l.remote_event_outbox.insert_one({'_id':es[1]['event_id'],'source_session_id':es[1]['session_id'],'sequence':2,
   'state':'parked','reason':'sequence_gap:expected_1','parked_at':datetime.now(timezone.utc)})
 orig=Projector.wake_successor
 def broken(self,e):raise RuntimeError('mongo blip')
 monkeypatch.setattr(Projector,'wake_successor',broken)
 with pytest.raises(RuntimeError):p.project(es[0])  # receipt + cursor written, wake failed
 monkeypatch.setattr(Projector,'wake_successor',orig)
 assert l.remote_session_cursors.find_one({'_id':es[0]['session_id']})['last_sequence']==1
 assert l.remote_event_outbox.find_one({'sequence':2})['state']=='parked'
 assert p.project(es[0])['scope']=='dashboard_records'  # replay: no duplicate, same result
 assert l.remote_event_outbox.find_one({'sequence':2})['state']=='pending'
 assert t.problem_runs.count_documents({})==1

def test_contiguous_event_without_attempt_id_is_quarantined_not_waiting(dbs):
 # reliability P1-3: a present, contiguous event that lacks attempt_id can never be repaired
 l,t=dbs;es=rows()
 p=Projector(l,t,None,{})
 p.project(es[0])
 bad=dict(es[1]);bad.pop('attempt_id')
 assert p.project(bad)['reason']=='attempt_id_required'
 assert l.remote_session_cursors.find_one({'_id':es[0]['session_id']})['last_sequence']==2
 assert t.pipeline_quarantined_events.count_documents({'reason':'attempt_id_required'})==1

def test_gap_still_waits_even_if_attempt_id_missing(dbs):
 l,t=dbs;es=rows()
 bad=dict(es[2]);bad.pop('attempt_id')
 with pytest.raises(Waiting,match='sequence_gap'):Projector(l,t,None,{}).project(bad)

def test_unknown_wait_reason_is_bounded(dbs):
 import remote.worker as w
 assert w.wait_limit('member_is_not_live') is not None
 assert w.wait_limit('anything_new') is not None
 assert w.wait_limit('other_database_writer_active') is None

def test_requeue_gives_a_fresh_retry_budget(dbs, monkeypatch):
 # reliability P1-4: a parked retry_limit row requeued by hand survives one transient error
 import remote.worker as w
 from remote.requeue import requeue
 l,t=dbs
 monkeypatch.setattr(w,'Projector',_raising(RuntimeError('db down')))
 tired=_job(l,7,attempts=20)
 assert w.process_job(tired,l,t,_Pool(),{},True) is w.PARKED_SENTINEL
 assert requeue(l,'retry_limit',None)==1
 row=l.remote_event_outbox.find_one({'_id':'j7'})
 assert row['retry_base']==20 and row['attempts']==20
 job=w.claim_batch(l,10)[0]
 assert job['attempts']==21
 assert w.process_job(job,l,t,_Pool(),{},True) not in (None,w.PARKED_SENTINEL)
 assert l.remote_event_outbox.find_one({'_id':'j7'})['state']=='retrying'

def test_group_cache_gives_same_records_as_uncached(dbs):
 # 8th round: a session processed with the per-group cache writes exactly what per-event processing wrote
 l,t=dbs;es=rows()
 cache={}
 for e in es:Projector(l,t,None,{},cache).project(e)
 l2,t2=l.client['l2'],l.client['t2'];es2=rows()
 for e in es2:Projector(l2,t2,None,{}).project(e)
 for coll in ('gameplay_events','solution_attempts','problem_runs','game_sessions'):
  assert t[coll].count_documents({})==t2[coll].count_documents({})
 a=t.solution_attempts.find_one();assert a['outcome']=='success' and a['ended_at'] is not None
 assert t.problem_runs.find_one()['status']=='completed'
 assert l.remote_session_cursors.find_one()['last_sequence']==5

def test_member_lookup_is_cached(monkeypatch):
 import remote.projection as p
 p._member_cache.clear();calls=[]
 monkeypatch.setattr(p,'_resolve_member',lambda sql,player,ctx:(calls.append(player),'m-'+player)[1])
 assert p.resolve_member(None,'a',{})=='m-a' and p.resolve_member(None,'a',{})=='m-a'
 assert calls==['a']
 def missing(*a):raise Waiting('member_mapping_required')
 monkeypatch.setattr(p,'_resolve_member',missing)
 for _ in range(2):
  try:p.resolve_member(None,'b',{})
  except Waiting:pass
 assert ('b',None) not in p._member_cache  # a missing mapping is checked again every time
 p._member_cache.clear()


def test_member_cache_can_be_disabled(monkeypatch):
 import remote.projection as p
 from common import settings
 p._member_cache.clear();calls=[]
 monkeypatch.setattr(p,'_resolve_member',lambda sql,player,ctx:(calls.append(player),'m')[1])
 monkeypatch.setenv('REMOTE_MEMBER_CACHE_SECONDS','0');settings.get.cache_clear() if hasattr(settings.get,'cache_clear') else None
 try:
  p.resolve_member(None,'z',{});p.resolve_member(None,'z',{})
  assert calls==['z','z'] and not p._member_cache
 finally:
  monkeypatch.delenv('REMOTE_MEMBER_CACHE_SECONDS');settings.get.cache_clear() if hasattr(settings.get,'cache_clear') else None

def test_member_cache_is_bounded(monkeypatch):
 # follow-up review #3: many distinct members never grow the cache past MEMBER_CACHE_MAX
 import remote.projection as p
 p._member_cache.clear()
 monkeypatch.setattr(p,'MEMBER_CACHE_MAX',5)
 monkeypatch.setattr(p,'_resolve_member',lambda sql,player,ctx:'m-'+player)
 for i in range(20):p.resolve_member(None,f'u{i}',{})
 assert len(p._member_cache)<=5 and ('u19',None) in p._member_cache
 p._member_cache.clear()
