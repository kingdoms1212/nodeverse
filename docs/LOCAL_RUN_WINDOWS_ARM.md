# 한 PC에서 전체 실행하기 (Windows 11 ARM64 + WSL2)

게임·파이프라인·서버를 개발 PC 한 대에서 모두 띄우는 방법입니다. 2026-11-20에 Windows 11 ARM64(NVIDIA N1X, RAM 30GB)에서 구성하고 확인했습니다.
Docker Desktop 없이 **WSL2 Ubuntu 안의 Docker 엔진**을 씁니다(관리자 권한·재부팅·라이선스 확인이 필요 없음).

## 구성 요약

| 무엇 | 어디서 | 주소 (Windows에서 접속) |
|---|---|---|
| 파이프라인 (Kafka×3 · Spark · 수집기 · MongoDB · remote-writer) | WSL Docker, 프로젝트 `nodeverse` | 수집기 `http://127.0.0.1:8000/v2/events`, MongoDB `mongodb://127.0.0.1:27017` |
| MySQL 8.4 | WSL Docker, 컨테이너 `nodeverse-mysql` | `127.0.0.1:3306` |
| Django 서버 | WSL systemd 서비스 `nodeverse-server` | `http://127.0.0.1:8010/` (수집기와 8000 충돌을 피해 8010) |
| 게임 | Windows (Godot x64, ARM에서 에뮬레이션으로 동작, Vulkan 정상) | — |

모든 포트는 `127.0.0.1`에만 열려 있어 다른 PC에서는 접속할 수 없습니다.

## 0. 원클릭

저장소 루트의 `전체실행.cmd`를 더블클릭하면 아래 1~2단계를 자동으로 합니다(Docker 설치, `.env` 생성, MySQL, 파이프라인, Django 마이그레이션과 서비스 등록, 게임 네트워크 설정). 끝나면 대시보드를 브라우저로 열고 게임 실행 여부를 묻습니다. 중지는 `전체중지.cmd`, 상태 확인은 `상태확인.cmd`. 실제 내용은 `scripts/local/up.sh`, `down.sh`, `status.sh`입니다.

## 1. 처음 한 번만 하는 설정 (이 PC는 완료됨)

```powershell
# WSL Ubuntu에 Docker 엔진과 빌드 도구 설치
wsl -d Ubuntu-24.04 -u root -- bash -lc "apt-get update && apt-get install -y docker.io docker-compose-v2 python3-venv python3-dev pkg-config default-libmysqlclient-dev build-essential && systemctl enable --now docker"
```

`%USERPROFILE%\.wslconfig`에 다음을 추가했습니다(원본 백업: `.wslconfig.bak-20261120`). 없으면 WSL이 잠시 후 꺼지면서 컨테이너도 같이 멈춥니다.

```ini
[wsl2]
vmIdleTimeout=-1
memory=12GB                    # stack uses 7-8GB; leave the rest to Windows

[general]
instanceIdleTimeout=-1

[experimental]
autoMemoryReclaim=dropCache    # return idle Linux cache to Windows
```

설정 파일 (모두 git 제외, 값은 각자 생성):

| 파일 | 내용 |
|---|---|
| `pipeline/.env` | `.env.example` 복사 + `INGEST_KEY` 임의 값 + `REMOTE_EXPORT_ENABLED=1` |
| `pipeline/.env.remote` | `DIRECT_MYSQL_HOST=nodeverse-mysql`, MySQL 계정, `DIRECT_MONGO_URI=mongodb://mongo:27017/` (로컬에서는 같은 MongoDB를 원격 DB로 사용) |
| `pipeline/.env.remote-context.json` | 시험 회원 매핑 (`local-smoke-player`) |
| `server/.env` | Django 키, MySQL `nodeverse_app` 계정, `MONGODB_URI=mongodb://127.0.0.1:27017/` |
| `server/.env.mysql-root` | MySQL root 비밀번호 |
| `%APPDATA%\Godot\app_userdata\NODEVERSE 3D Assets\network_config.json` | 게임 → 수집기 `127.0.0.1:8000`, Django `127.0.0.1:8010`, `auth_token` = `INGEST_KEY` |

## 2. 매일 시작·종료

PC를 켠 뒤 WSL을 한 번 깨우면 Docker와 Django 서비스가 자동으로 올라오고, 컨테이너는 `restart: unless-stopped`라 같이 시작됩니다.

```powershell
wsl -d Ubuntu-24.04 -u root -- docker ps --format "table {{.Names}}\t{{.Status}}"
```

파이프라인을 수동으로 올리거나 코드 변경 후 다시 빌드할 때:

```powershell
wsl -d Ubuntu-24.04 -u root -- bash -lc "cd /mnt/c/workspace/mlo-02-p3-team1/pipeline && docker network connect nodeverse_default nodeverse-mysql 2>/dev/null; docker compose -p nodeverse --env-file .env --profile remote -f compose.json up -d --build"
```

멈추기 (데이터는 볼륨에 남음):

```powershell
wsl -d Ubuntu-24.04 -u root -- bash -lc "cd /mnt/c/workspace/mlo-02-p3-team1/pipeline && docker compose -p nodeverse --env-file .env --profile remote -f compose.json stop; docker stop nodeverse-mysql; systemctl stop nodeverse-server"
```

Django 로그: `wsl -d Ubuntu-24.04 -u root -- journalctl -u nodeverse-server -f`
Spark 로그: `wsl -d Ubuntu-24.04 -u root -- docker logs -f nodeverse-spark-1`

## 3. 각 파트 확인 방법

| 파트 | 확인 | 2026-11-20 결과 |
|---|---|---|
| 수집기 | `http://127.0.0.1:8000/health/ready` → `{"status":"ready"}` | ✅ |
| 수집 → Kafka | 시험 이벤트 7건 202, 잘못된 카테고리 1건 422 + DLQ | ✅ |
| Spark | 로그에 `started queries: [bronze, silver_*, silver_audit, gold_1m, gold_alert, gold_update]` | ✅ |
| MongoDB `rt_*` | `rt_minigame_1m`에 ST03 1분 창 집계, `rt_summary` 갱신 | ✅ (약 15초 후 반영) |
| remote-writer | outbox 7건 `stored`, `problem_runs` 1, `gameplay_events` 7 | ✅ |
| Django | `/staff/login/` → "관리자 테스트 로그인"(DEBUG·로컬 전용) → `/staff/` 개요에 활동 플레이어 1명, 해결률 100% | ✅ |
| Django 실시간 | `/api/admin/live/summary`, `/api/admin/live/minigames`에 파이프라인 집계 표시 | ✅ |
| 게임 | Godot 4.7.2 x64가 에뮬레이션으로 실행, Vulkan(NVIDIA) 사용. 시작 시 `update_checked` 이벤트가 로컬 수집기로 202 전송됨 | ✅ (미니게임 플레이 이벤트는 직접 플레이로 확인 필요) |

## 4. 알려진 문제 (구성하면서 발견)

1. **서버 첫 마이그레이션이 실패합니다.** `accounts.0005_describe_system_tables`가 `django_session` 테이블을 건드리는데 `sessions` 마이그레이션에 의존하지 않아 빈 DB에서 순서가 어긋납니다. 우회: `manage.py migrate sessions` 먼저, 그다음 `manage.py migrate`. (서버 파트에서 마이그레이션 의존성 추가 필요)
2. **수집기와 Django가 둘 다 8000번**을 씁니다. 한 PC에서는 Django를 8010으로 띄우고 게임 `api_port`도 8010으로 맞춥니다.
3. **실제 게임 계정의 이벤트는 원격 적재에서 대기할 수 있습니다.** remote-writer는 `game_members`에 있는 회원이나 매핑 파일에 등록된 회원만 적재하고, 나머지는 `waiting_context: member_mapping_required`로 둡니다. 실시간 화면(`rt_*`)에는 바로 반영됩니다.
4. **바탕화면의 Godot 실행 파일이 폴더 안에 있어 배치 파일이 찾지 못합니다.** 배치 파일은 `바탕화면\Godot_v4.7.2-stable_win64.exe`가 *파일*일 때만 찾습니다. 저장소 루트에 복사해 두었습니다(`.gitignore`로 커밋 안 됨).
5. Windows에서 직접 `pip install -r pipeline/requirements.txt`는 `confluent-kafka` 때문에 실패합니다. 파이프라인 테스트는 WSL이나 컨테이너 안에서 실행합니다.

## 5. MongoDB·MySQL 조회

`전체실행.cmd`가 마지막 단계에서 DB 조회 화면도 함께 띄웁니다(따로 띄우려면 `DB조회화면.cmd`). 두 화면 모두 이 PC(127.0.0.1)에서만 열립니다.

| 대상 | 브라우저 | 명령줄 |
|---|---|---|
| MongoDB `nodeverse_logs` (rt_* 집계, outbox, 세션·시도·이벤트) | http://127.0.0.1:8081 (mongo-express) | `DB조회_몽고.cmd` → `show collections`, `db.rt_summary.findOne()` |
| MySQL `nodeverse` (회원·관리자·배포) | http://127.0.0.1:8082 (Adminer) — 서버 `nodeverse-mysql`, 사용자 `nodeverse_app`, 비밀번호는 `server/.env`의 `MYSQL_PASSWORD`, DB `nodeverse` | `DB조회_MySQL.cmd` → `SHOW TABLES;`, `SELECT * FROM game_members LIMIT 10;` |

자주 쓰는 조회:

```js
// MongoDB
db.rt_minigame_1m.find().sort({window_start:-1}).limit(5)                       // 최근 1분 집계
db.remote_event_outbox.aggregate([{$group:{_id:"$state", n:{$sum:1}}}])          // 원격 적재 상태별 건수
db.gameplay_events.find({session_id: "<세션 ID>"}).sort({sequence:1})            // 한 세션의 이벤트
```

```sql
-- MySQL
SELECT member_id, external_member_key, nickname, data_source FROM game_members ORDER BY registered_at DESC LIMIT 20;
SELECT COUNT(*) FROM game_accounts;
```
