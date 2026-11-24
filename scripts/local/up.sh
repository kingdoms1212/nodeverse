#!/usr/bin/env bash
# NODEVERSE 로컬 전체 실행 (WSL Ubuntu 안에서 root로 실행).
# 처음 실행이면 Docker·환경 파일·MySQL·Django 가상환경을 자동으로 준비한다. 여러 번 실행해도 안전하다.
# 사용: bash scripts/local/up.sh [Windows %APPDATA% 경로]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PIPE="$ROOT/pipeline"; SRV="$ROOT/server"
VENV=/opt/nodeverse/server-venv
DJANGO_PORT=8010
say(){ printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
ok(){ printf '  \033[32m✔ %s\033[0m\n' "$*"; }
rnd(){ python3 -c "import secrets;print(secrets.token_urlsafe($1))"; }
get(){ grep -m1 "^$1=" "$2" 2>/dev/null | cut -d= -f2- | tr -d '\r'; }

say "1/7 Docker 엔진"
if ! command -v docker >/dev/null; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq && apt-get install -y -qq docker.io docker-compose-v2 python3-venv python3-dev pkg-config default-libmysqlclient-dev build-essential curl >/dev/null
fi
systemctl enable --now docker >/dev/null 2>&1 || service docker start >/dev/null
ok "$(docker version --format 'Docker {{.Server.Version}} ({{.Server.Arch}})')"

say "2/7 환경 파일 (.env, git에 올라가지 않음)"
if [ ! -f "$SRV/.env" ]; then
  cat > "$SRV/.env" <<ENV
DJANGO_SECRET_KEY=$(rnd 48)
DJANGO_DEBUG=True
DJANGO_ALLOWED_HOSTS=localhost,127.0.0.1
MYSQL_DATABASE=nodeverse
MYSQL_USER=nodeverse_app
MYSQL_PASSWORD=$(rnd 18)
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_TEST_DATABASE=test_nodeverse
ADMIN_REGISTRATION_CODE=$(rnd 9)
GAME_SERVER_API_KEY=$(rnd 32)
MONGODB_URI=mongodb://127.0.0.1:27017/
MONGODB_DATABASE=nodeverse_logs
PUBLIC_BASE_URL=
DJANGO_MEDIA_ROOT=
ENV
  ok "server/.env 생성"
fi
[ -f "$SRV/.env.mysql-root" ] || { echo "MYSQL_ROOT_PASSWORD=$(rnd 18)" > "$SRV/.env.mysql-root"; ok "server/.env.mysql-root 생성"; }
if [ ! -f "$PIPE/.env" ]; then
  sed "s#^INGEST_KEY=.*#INGEST_KEY=$(rnd 32)#" "$PIPE/.env.example" | tr -d '\r' > "$PIPE/.env"
  ok "pipeline/.env 생성"
fi
grep -q '^REMOTE_EXPORT_ENABLED=' "$PIPE/.env" || echo 'REMOTE_EXPORT_ENABLED=1' >> "$PIPE/.env"
APP_PW=$(get MYSQL_PASSWORD "$SRV/.env"); ROOT_PW=$(get MYSQL_ROOT_PASSWORD "$SRV/.env.mysql-root")
if [ ! -f "$PIPE/.env.remote" ]; then
  printf 'DIRECT_MYSQL_HOST=nodeverse-mysql\nDIRECT_MYSQL_PORT=3306\nDIRECT_MYSQL_DATABASE=nodeverse\nDIRECT_MYSQL_USER=nodeverse_app\nDIRECT_MYSQL_PASSWORD=%s\nDIRECT_MONGO_URI=mongodb://mongo:27017/\nDIRECT_MONGO_DATABASE=nodeverse_logs\n' "$APP_PW" > "$PIPE/.env.remote"
  ok "pipeline/.env.remote 생성 (로컬 MySQL·MongoDB를 원격 DB로 사용)"
fi
[ -f "$PIPE/.env.remote-context.json" ] || { echo '{"members": {}}' > "$PIPE/.env.remote-context.json"; ok "pipeline/.env.remote-context.json 생성"; }

say "3/7 MySQL 8.4"
docker volume create nodeverse-mysql >/dev/null
if ! docker inspect nodeverse-mysql >/dev/null 2>&1; then
  docker run -d --name nodeverse-mysql --restart unless-stopped -p 127.0.0.1:3306:3306 \
    -e MYSQL_ROOT_PASSWORD="$ROOT_PW" -e MYSQL_DATABASE=nodeverse -e MYSQL_USER=nodeverse_app -e MYSQL_PASSWORD="$APP_PW" \
    -v nodeverse-mysql:/var/lib/mysql mysql:8.4 --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci >/dev/null
fi
docker start nodeverse-mysql >/dev/null
for i in $(seq 1 60); do
  docker exec -e MYSQL_PWD="$ROOT_PW" nodeverse-mysql mysql -uroot -h127.0.0.1 -e "CREATE DATABASE IF NOT EXISTS test_nodeverse CHARACTER SET utf8mb4; GRANT ALL ON nodeverse.* TO 'nodeverse_app'@'%'; GRANT ALL ON test_nodeverse.* TO 'nodeverse_app'@'%';" >/dev/null 2>&1 && break
  sleep 3; [ "$i" = 60 ] && { echo "MySQL이 준비되지 않았습니다: docker logs nodeverse-mysql"; exit 1; }
done
ok "127.0.0.1:3306 준비됨"

say "4/7 파이프라인 (Kafka·Spark·수집기·MongoDB·원격 적재)"
cd "$PIPE"
docker compose -p nodeverse --env-file .env --profile remote -f compose.json up -d --build --quiet-pull 2>&1 | grep -vE '^\s*$' | tail -3
docker network connect nodeverse_default nodeverse-mysql 2>/dev/null || true
for i in $(seq 1 60); do curl -sf -m3 http://127.0.0.1:8000/health/ready >/dev/null && break; sleep 3; done
curl -sf -m3 http://127.0.0.1:8000/health/ready >/dev/null && ok "수집기 http://127.0.0.1:8000/v2/events 준비됨" || echo "  ⚠ 수집기가 아직 준비되지 않았습니다 (docker logs nodeverse-collector-1)"

say "5/7 Django 서버"
if [ ! -x "$VENV/bin/python" ]; then python3 -m venv "$VENV"; fi
"$VENV/bin/pip" install -q -r "$SRV/requirements.txt"
cd "$SRV"
# 빈 DB에서 accounts.0005가 django_session을 먼저 건드리는 순서 문제 우회
"$VENV/bin/python" manage.py migrate sessions --noinput >/dev/null
"$VENV/bin/python" manage.py migrate --noinput | grep -E 'Applying|No migrations' | tail -3 || true
cat > /etc/systemd/system/nodeverse-server.service <<UNIT
[Unit]
Description=NODEVERSE Django dev server (127.0.0.1:$DJANGO_PORT)
After=docker.service
[Service]
WorkingDirectory=$SRV
ExecStart=$VENV/bin/python manage.py runserver 127.0.0.1:$DJANGO_PORT --noreload
Restart=on-failure
RestartSec=5
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload; systemctl enable nodeverse-server >/dev/null 2>&1; systemctl restart nodeverse-server
for i in $(seq 1 30); do curl -sf -m3 -o /dev/null "http://127.0.0.1:$DJANGO_PORT/" && break; sleep 1; done
ok "Django http://127.0.0.1:$DJANGO_PORT/staff/ 준비됨"

say "6/7 게임 네트워크 설정"
if [ -n "${1:-}" ]; then
  CFG_DIR="$(wslpath -u "$1")/Godot/app_userdata/NODEVERSE 3D Assets"
  mkdir -p "$CFG_DIR"
  python3 - "$CFG_DIR/network_config.json" "$(get INGEST_KEY "$PIPE/.env")" "$DJANGO_PORT" <<'PY'
import json, os, sys
path, key, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
cfg = {}
if os.path.exists(path):
    try: cfg = json.load(open(path, encoding="utf-8"))
    except Exception: cfg = {}
cfg.update({"host": "127.0.0.1", "port": 8000, "endpoint": "/v2/events", "api_host": "127.0.0.1",
            "api_port": port, "auth_token": key})
cfg.setdefault("auto_send", True); cfg.setdefault("timeout", 3.0)
json.dump(cfg, open(path, "w", encoding="utf-8"), indent=2)
PY
  ok "게임이 로컬 수집기(8000)·Django($DJANGO_PORT)로 보내도록 설정"
fi

say "7/7 DB 조회 화면"
bash "$ROOT/scripts/local/dbui.sh" || true

printf '\n\033[1;32m완료\033[0m  대시보드 http://127.0.0.1:%s/staff/  (로그인 화면의 "관리자 테스트 로그인")\n' "$DJANGO_PORT"
docker ps --format '  {{.Names}}\t{{.Status}}' | sort
