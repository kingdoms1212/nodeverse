"""Three explicit runtime probes for operational timing validation, never game-member inserts."""
import argparse,json,time,uuid,urllib.request
from pathlib import Path
from datetime import datetime,timezone


def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--allow-live-writes',action='store_true');p.add_argument('--env-path',default='.env');p.add_argument('--base-url',default='http://192.168.0.81:8000');a=p.parse_args()
 if not a.allow_live_writes:p.error('--allow-live-writes required; sends three named runtime events')
 key=next(line.split('=',1)[1].strip().strip('\"\'') for line in Path(a.env_path).read_text(encoding='utf-8-sig').splitlines() if line.startswith('INGEST_KEY='))
 events=[{'schema_version':1,'event_id':str(uuid.uuid4()),'run_id':str(uuid.uuid4()),'player_id':'observatory-metrics-probe','build_version':'0.3.0','event_type':'run_heartbeat','evidence':'launcher_observed','occurred_at':datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')} for _ in range(3)]
 folder=Path('results/observatory');folder.mkdir(parents=True,exist_ok=True);path=folder/('probe-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'.json');path.write_text(json.dumps({'events':events},indent=2),encoding='utf-8')
 def call(event=None,ident=None):
  req=urllib.request.Request(a.base_url.rstrip('/')+'/v2/events'+('/'+ident if ident else ''),data=json.dumps(event).encode() if event else None,headers={'Content-Type':'application/json','X-API-Key':key})
  with urllib.request.urlopen(req,timeout=15) as r:return r.status,json.load(r)
 for event in events:assert call(event)[0]==202
 deadline=time.monotonic()+120
 while time.monotonic()<deadline:
  deliveries=[call(ident=e['event_id'])[1] for e in events];states=[d['state'] for d in deliveries];print(json.dumps({'states':states}),flush=True)
  if all(s=='stored' for s in states):
   path.write_text(json.dumps({'passed':True,'events':events,'delivery':deliveries},indent=2),encoding='utf-8');print(json.dumps({'passed':True,'fixture':str(path)}),flush=True);return
  time.sleep(3)
 raise RuntimeError('Probe delivery timeout; fixture retained')
if __name__=='__main__':main()
