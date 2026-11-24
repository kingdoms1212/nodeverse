#!/usr/bin/env bash
# NODEVERSE 로컬 전체 중지. 데이터(볼륨)는 지우지 않는다.
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT/pipeline"
systemctl stop nodeverse-server 2>/dev/null && echo "✔ Django 중지"
docker compose -p nodeverse --env-file .env --profile remote -f compose.json stop 2>&1 | tail -1
docker stop nodeverse-mongo-ui nodeverse-mysql-ui >/dev/null 2>&1 && echo "✔ DB 조회 화면 중지"
docker stop nodeverse-mysql >/dev/null 2>&1 && echo "✔ MySQL 중지"
echo "✔ 파이프라인 중지 (데이터는 그대로 남아 있습니다)"
