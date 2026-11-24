"""Capture only reviewed runtime configuration, without changing the container."""
import argparse,json,subprocess
from pathlib import Path

PROBE=r'''
import json,hashlib,urllib.request
from pathlib import Path
from datetime import datetime,timezone
raw=dict(x.split(b'=',1) for x in Path('/proc/1/environ').read_bytes().split(b'\0') if b'=' in x)
paths=[Path('/app/delta_stream.py'),Path('/opt/nodeverse/common/settings.py')]
for p in paths:
 if not p.exists():continue
 source=p.read_text()
 if 'int(os.getenv("DEDUP_TTL_HOURS", str(7 * 24)))' in source or '_int("DEDUP_TTL_HOURS", 7 * 24)' in source:break
else:raise RuntimeError('Unreviewed TTL configuration source; inspect it before capturing')
hours=int(raw.get(b'DEDUP_TTL_HOURS',b'168'))
with urllib.request.urlopen('http://127.0.0.1:4040/api/v1/applications',timeout=3) as response:apps=json.load(response)
assert len(apps)==1
print(json.dumps({'application_id':apps[0]['id'],'effective_dedup_hours':hours,
 'verification':'process_env_and_verified_code_default','source_sha256':hashlib.sha256(source.encode()).hexdigest(),
 'captured_at':datetime.now(timezone.utc).isoformat()}))
'''

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--container',required=True);p.add_argument('--output',required=True);a=p.parse_args()
 r=subprocess.run(['docker','exec',a.container,'python3','-c',PROBE],capture_output=True,text=True,check=True)
 data=json.loads(r.stdout);out=Path(a.output);out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(data,indent=2),encoding='utf-8');print(json.dumps(data))
if __name__=='__main__':main()
