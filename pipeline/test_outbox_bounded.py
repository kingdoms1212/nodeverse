import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from pathlib import Path

sys.path.insert(0,str(Path(__file__).parent/'spark'))
import sinks
import remote.outbox as outbox


def test_outbox_consumes_one_bounded_chunk_and_retries_same_content(monkeypatch):
    monkeypatch.setattr(sinks.settings,'get',lambda:SimpleNamespace(silver_optimization_enabled=True,outbox_stage_chunk=3,sink_retry_attempts=2))
    monkeypatch.setattr(sinks.time,'sleep',lambda _:None)
    consumed=[];calls=[];failed=False
    def generate():
        for i in range(8):
            consumed.append(i)
            yield {'event_id':str(i)}
    def stage(rows,queued_at=None):
        nonlocal failed
        calls.append((list(rows),queued_at,len(consumed)))
        if not failed:
            failed=True
            raise RuntimeError('simulated transient failure')
    monkeypatch.setattr(outbox,'stage',stage)
    sinks.stage_outbox(generate())
    assert [len(rows) for rows,_,_ in calls]==[3,3,3,2]
    assert calls[0][0]==calls[1][0] and calls[0][2]==3
    assert len({stamp for _,stamp,_ in calls})==1
    assert [row['event_id'] for rows,_,_ in calls[1:] for row in rows]==list(map(str,range(8)))


def test_outbox_stage_keeps_shared_timestamp_across_chunks(monkeypatch):
    from pymongo import UpdateOne
    stamp=datetime(2026,11,23,tzinfo=timezone.utc);seen=[]
    monkeypatch.setattr(outbox,'_stage_chunk',lambda db,rows,operation,now:seen.append((list(rows),now)))
    outbox.stage(({'event_id':str(i)} for i in range(5)),db=object(),chunk=2,queued_at=stamp)
    assert [len(rows) for rows,_ in seen]==[2,2,1]
    assert all(time==stamp for _,time in seen)


def test_optimization_disabled_preserves_single_call(monkeypatch):
    monkeypatch.setattr(sinks.settings,'get',lambda:SimpleNamespace(silver_optimization_enabled=False,sink_retry_attempts=1))
    calls=[]
    monkeypatch.setattr(outbox,'stage',lambda rows:calls.append(list(rows)))
    sinks.stage_outbox({'event_id':str(i)} for i in range(8))
    assert len(calls)==1 and len(calls[0])==8
