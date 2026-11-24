#!/usr/bin/env bash
# NODEVERSE 로컬 DB 조회 화면 (WSL Ubuntu 안에서 root로 실행). 여러 번 실행해도 안전하다.
#   MongoDB : http://127.0.0.1:8081  (mongo-express, nodeverse_logs)
#   MySQL   : http://127.0.0.1:8082  (Adminer, 서버 nodeverse-mysql)
# 두 화면 모두 이 PC에서만 열린다(127.0.0.1). 운영 DB에는 연결하지 않는다.
set -euo pipefail
NET=nodeverse_default
docker network inspect "$NET" >/dev/null 2>&1 || { echo "파이프라인이 아직 실행되지 않았습니다. 전체실행.cmd를 먼저 실행하세요."; exit 1; }

if ! docker inspect nodeverse-mongo-ui >/dev/null 2>&1; then
  docker run -d --name nodeverse-mongo-ui --restart unless-stopped --network "$NET" -p 127.0.0.1:8081:8081 \
    -e ME_CONFIG_MONGODB_URL="mongodb://mongo:27017/" -e ME_CONFIG_MONGODB_ENABLE_ADMIN=true \
    -e ME_CONFIG_BASICAUTH=false -e ME_CONFIG_OPTIONS_EDITORTHEME=default \
    mongo-express:1.0.2-20 >/dev/null
fi
if ! docker inspect nodeverse-mysql-ui >/dev/null 2>&1; then
  docker run -d --name nodeverse-mysql-ui --restart unless-stopped --network "$NET" -p 127.0.0.1:8082:8080 \
    -e ADMINER_DEFAULT_SERVER=nodeverse-mysql -e ADMINER_DESIGN=pepa-linden \
    adminer:4.8.1 >/dev/null
fi
docker network connect "$NET" nodeverse-mysql 2>/dev/null || true
docker start nodeverse-mongo-ui nodeverse-mysql-ui >/dev/null
for i in $(seq 1 30); do
  curl -s -o /dev/null -m 2 http://127.0.0.1:8081/ && curl -s -o /dev/null -m 2 http://127.0.0.1:8082/ && break
  sleep 2
done
echo "  ✔ MongoDB 조회: http://127.0.0.1:8081  (nodeverse_logs 선택)"
echo "  ✔ MySQL 조회:   http://127.0.0.1:8082  (서버 nodeverse-mysql · 사용자 nodeverse_app · 비밀번호는 server/.env의 MYSQL_PASSWORD · DB nodeverse)"
