"""Real Python worker env check plus deterministic audit-function timeout simulation."""
import json
import os
from pathlib import Path


def worker(_):
    import audit_state
    return audit_state.DEDUP_TTL_MS


class State:
    exists=False
    hasTimedOut=False
    value=None
    duration=None
    @property
    def get(self):return self.value
    def update(self,value):self.value=value;self.exists=True
    def remove(self):self.exists=False;self.value=None
    def setTimeoutDuration(self,value):self.duration=value


def audit_probe():
    import pandas as pd
    import audit_state
    state=State()
    def feed(hash_value):
        row={'key':'topic:id','value':'{}','topic':'topic','partition':0,'offset':0,'event_id':'id','hash':hash_value}
        return [a for frame in audit_state.audit(('topic:id',),iter([pd.DataFrame([row])]),state) for a in frame['action']]
    first=feed('original');same=feed('original');conflict=feed('changed')
    ttl=state.duration
    # Simulate a timeout callback at 49h without new activity after the initial calls.
    if ttl < 49*3600*1000:
        state.hasTimedOut=True;list(audit_state.audit(('topic:id',),iter([]),state));state.hasTimedOut=False
    after49=feed('changed')
    return {'configured_timeout_ms':ttl,'first':first,'same':same,'conflict_before_timeout':conflict,
            'conflict_after_49h_inactivity':after49,'simulation_not_wall_clock_soak':True}


if __name__=='__main__':
    import tables
    import audit_state
    from common import settings
    spark=tables.spark_session('dedup-env-probe',2)
    try:
        expected=int(os.environ['DEDUP_TTL_HOURS'])
        workers=spark.sparkContext.parallelize([0,1],2).map(worker).collect()
        assert settings.get().dedup_ttl_hours==expected
        assert audit_state.DEDUP_TTL_MS==expected*3600*1000
        assert workers==[expected*3600*1000]*2
        result={'hours':expected,'driver_hours':settings.get().dedup_ttl_hours,'worker_ms':workers,'audit':audit_probe()}
        assert result['audit']['conflict_after_49h_inactivity']==(['first'] if expected==48 else ['id_conflict'])
        Path(f'/experiment/audit-{expected}.json').write_text(json.dumps(result,indent=2))
        print('AUDIT_RESULT '+json.dumps(result),flush=True)
    finally:spark.stop()
