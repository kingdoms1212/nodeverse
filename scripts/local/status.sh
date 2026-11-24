#!/usr/bin/env bash
# NODEVERSE 로컬 상태 확인
docker ps -a --filter name=nodeverse --format '{{.Names}}\t{{.Status}}' | sort
printf 'collector: '; curl -s -m3 http://127.0.0.1:8000/health/ready || echo down; echo
printf 'django:    '; curl -s -m3 -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8010/ || echo down
docker exec nodeverse-mongo-1 mongosh --quiet nodeverse_logs --eval 'const s=db.rt_summary.findOne(); print("rt_summary: " + JSON.stringify(s)); print("outbox: " + JSON.stringify(db.remote_event_outbox.aggregate([{$group:{_id:"$state",n:{$sum:1}}}]).toArray()))' 2>/dev/null
