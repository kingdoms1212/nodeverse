"""No DB calls: compare Python staging-buffer allocations with a synthetic counting sink."""
import gc
import json
from pathlib import Path
import sys
import tracemalloc
from types import SimpleNamespace

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'spark'))
import sinks
import remote.outbox as outbox


def measure(fn,n):
    observed={'rows':0,'max_call_rows':0}
    def stage(rows,queued_at=None):
        observed['rows']+=len(rows)
        observed['max_call_rows']=max(observed['max_call_rows'],len(rows))
    outbox.stage=stage
    def records():
        for i in range(n):yield {'event_id':str(i),'payload':'x'*512+str(i)}
    gc.collect();tracemalloc.start()
    fn(records())
    _,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
    assert observed['rows']==n
    return dict(observed,python_peak_bytes=peak)


def main():
    n=100000
    original_stage,original_get=outbox.stage,sinks.settings.get
    sinks.settings.get=lambda:SimpleNamespace(silver_optimization_enabled=True,outbox_stage_chunk=500,sink_retry_attempts=1)
    try:
        before=measure(lambda records:outbox.stage(list(records)),n)
        after=measure(sinks.stage_outbox,n)
        assert after['max_call_rows']==500 and after['python_peak_bytes']<before['python_peak_bytes']
        print(json.dumps({'rows':n,'scope':'Python allocations; counting sink; no DB/RSS measurement',
                          'before':before,'after':after},indent=2))
    finally:outbox.stage=original_stage;sinks.settings.get=original_get


if __name__=='__main__':main()
