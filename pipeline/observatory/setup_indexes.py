"""One explicit metadata-only index setup. Default prints the plan without mutation."""
import argparse,json
from common.db import local_mongo
from observatory.metrics import LATENCY_INDEX
from observatory.recent import INDEX as RECENT_INDEX, KEYS as RECENT_KEYS
KEYS=[('state',1),('completed_at',-1),('_id',1)]

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute',action='store_true');a=p.parse_args()
 if a.execute:
  coll=local_mongo().remote_event_outbox
  coll.create_index(KEYS,name=LATENCY_INDEX)
  coll.create_index(RECENT_KEYS,name=RECENT_INDEX)
 print(json.dumps({'indexes':[{'name':LATENCY_INDEX,'keys':KEYS},{'name':RECENT_INDEX,'keys':RECENT_KEYS}],'executed':a.execute}))
if __name__=='__main__':main()
