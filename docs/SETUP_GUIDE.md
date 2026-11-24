# 개발환경 다시 구성하기 (통합 저장소 기준)

각자 쓰던 개별 저장소 대신 이제 **통합 저장소 하나**를 clone해서 작업합니다.
아래 순서대로 하면 30분 안에 원래 하던 작업을 이어갈 수 있습니다. 명령은 Windows PowerShell 기준입니다.

> 공용 저장소 주소: `https://github.com/encore-ai-campus/mlo-02-p3-team1.git`

## 0. 공통 준비 (모든 담당자)

| 도구 | 버전 | 비고 |
|---|---|---|
| Git | 2.40 이상 | |
| **Git LFS** | 3.x | 게임 에셋(모델·이미지·영상 974개)을 받는 데 필요. Git for Windows에 포함되어 있음 |
| VS Code | 최신 | 권장 확장은 워크스페이스를 열면 안내됨 |
| Python | 3.12 | 파이프라인 단위 테스트, 서버 |

```powershell
git lfs install                                  # PC당 한 번
git clone https://github.com/encore-ai-campus/mlo-02-p3-team1.git C:\workspace\mlo-02-p3-team1
cd C:\workspace\mlo-02-p3-team1
git lfs pull                                     # 혹시 에셋이 텍스트 포인터로 보이면
code nodeverse.code-workspace                    # 파트별 폴더가 나뉘어 보이는 워크스페이스
```

에셋이 제대로 받아졌는지 확인: `git lfs ls-files | Measure-Object` → 974개 안팎,
`game/models` 안의 `.glb` 파일 크기가 수백 KB 이상이면 정상(130바이트 정도면 LFS를 받지 못한 것).

### 예전 저장소에서 하던 작업이 있다면

1. 예전 저장소에서 아직 push 안 한 커밋이나 수정 파일이 있는지 확인합니다 (`git status`, `git log origin/main..`).
2. 있으면 **먼저 팀 채널에 알리고**, 통합 저장소에서 새 작업 브랜치를 만든 뒤 해당 파일을 복사해 커밋합니다. (예전 저장소의 경로 앞에 `game/`, `pipeline/`, `server/`가 붙는다는 것만 다릅니다.)
3. 이후 예전 저장소에는 push하지 않습니다. 원본 저장소는 읽기 전용(Archive)으로 바꿀 예정입니다.

---

## 1. 게임 담당 (`game/`)

| 항목 | 값 |
|---|---|
| 엔진 | **Godot 4.7.2 stable** (`Godot_v4.7.2-stable_win64.exe`) — 버전이 다르면 `.import`·씬 파일이 대량으로 바뀌므로 반드시 맞출 것 |
| 메인 씬 | `res://scenes/NodeverseWorld.tscn` |
| 내보내기 프리셋 | `Windows Desktop` → `build/windows/NODEVERSE.exe`, `Web` → `build/web/`, `Content Pack` → `build/packs/*.pck` |

```powershell
cd game
.\open_editor.bat        # 에디터 열기
.\play_game.bat          # 바로 실행
```

- 배치 파일은 Godot 실행 파일을 **저장소 루트(`game`의 상위 폴더)** 또는 **바탕화면**에서 찾습니다. 저장소 루트에 두어도 `.gitignore`로 커밋되지 않습니다.
- 처음 열면 에셋 import에 몇 분 걸립니다. 생기는 `.godot/` 폴더는 커밋하지 않습니다.
- EXE 내보내기:
  ```powershell
  & "..\Godot_v4.7.2-stable_win64_console.exe" --headless --path . --export-release "Windows Desktop" build/windows/NODEVERSE.exe
  ```
  `build/`는 커밋하지 않습니다. 배포는 서버의 `staff/releases`에 올립니다.

### 수집기·서버 주소와 키 설정

코드 기본값은 수집기 `192.168.0.81:8000/v2/events`, Django `192.168.0.29:8000`입니다. 장소가 바뀌면 **코드를 고치지 말고** 다음 중 하나로 바꿉니다.

- 게임 실행 중 `F6` 디버그 패널에서 주소·키 입력 후 저장
- 또는 설정 파일을 직접 작성: `%APPDATA%\Godot\app_userdata\NODEVERSE 3D Assets\network_config.json`
  ```json
  { "host": "<수집기 IP>", "port": 8000, "endpoint": "/v2/events",
    "api_host": "<Django IP>", "api_port": 8000,
    "auth_token": "<파이프라인 담당에게 받은 INGEST_KEY>", "auto_send": true, "timeout": 3.0 }
  ```

`INGEST_KEY`는 메신저로 받고 **코드·씬·커밋에 넣지 않습니다.**

### 수집기 없이 시험

```powershell
.\run_mock_server.bat      # 가짜 수집 서버
```

---

## 2. 파이프라인 담당 (`pipeline/`)

| 항목 | 값 |
|---|---|
| 필요 | Docker Desktop (Linux 엔진, 메모리 8GB 이상 권장), Python 3.12 |
| 서비스 | collector(FastAPI) · Kafka · Spark(Delta) · MongoDB |
| 포트 | 수집 API 8000, MongoDB 27017 (시험 프로젝트는 `review.env`로 8002 / 27019) |

```powershell
cd pipeline
Copy-Item .env.example .env
notepad .env          # INGEST_KEY를 24자 이상 임의 값으로 (아래 명령으로 생성)
python -c "import secrets; print(secrets.token_urlsafe(32))"

python build_compose.py                       # compose.json 재생성 (토픽·서비스 정의를 바꿨을 때)
docker compose -p nodeverse --env-file .env -f compose.json up -d --build
docker compose -p nodeverse --env-file .env -f compose.json ps
docker compose -p nodeverse --env-file .env -f compose.json logs -f spark   # "started queries" 확인
```

- 다른 PC(게임·서버)에서 접속하게 하려면 `.env`의 `COLLECTOR_BIND`, `MONGO_BIND`를 `0.0.0.0`으로 바꾸고 Windows 방화벽에서 8000, 27017을 엽니다.
- 정상 확인: `http://127.0.0.1:8000/health/ready`
- 계약 점검·스모크·부하 시험 순서는 [`pipeline/README.md`](../pipeline/README.md)의 "점검 순서"를 따릅니다.
- 볼륨(`lake`, `kafka-*`, `mongo`)은 지우지 마세요. 처음부터 다시 할 때만 `down -v`.

### 단위 테스트

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m pytest -q
```

> Windows **ARM** PC에서는 `confluent-kafka`가 설치되지 않습니다. 그 경우 테스트는 GitHub Actions 결과를 보거나 Docker 안에서 돌립니다.

### 공유 계약을 바꿀 때

`pipeline/contract.py`, `pipeline/topics.py`를 바꾸면 `contracts/`에도 같은 내용을 반영하고, PR에 게임·서버 담당자를 리뷰어로 넣습니다.

---

## 3. 서버 담당 (`server/`)

| 항목 | 값 |
|---|---|
| 필요 | Python 3.12, MySQL 8, MongoDB (파이프라인의 MongoDB를 같이 써야 실시간 화면에 실제 데이터가 나옴) |
| 실행 주소 | `http://127.0.0.1:8000/` (관리자 화면 `staff/`) |

```powershell
cd server
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
notepad .env
```

`.env`에 채울 값:

| 키 | 설명 |
|---|---|
| `DJANGO_SECRET_KEY` | `python -c "import secrets; print(secrets.token_urlsafe(48))"`로 생성 |
| `DJANGO_DEBUG` | 개발 PC는 `True` |
| `DJANGO_ALLOWED_HOSTS` | `localhost,127.0.0.1,<이 PC IP>` |
| `MYSQL_*` | 로컬 또는 팀 MySQL 접속 정보 |
| `MONGODB_URI` / `MONGODB_DATABASE` | 파이프라인 MongoDB: `mongodb://<파이프라인 PC IP>:27017` / `nodeverse_logs` |
| `ADMIN_REGISTRATION_CODE` | 관리자 가입 초대 코드 (각자 임의 값) |
| `GAME_SERVER_API_KEY` | 서버 간 호출용. 게임 클라이언트에 넣지 않음 |

```powershell
.\.venv\Scripts\python.exe manage.py check_mongodb
.\.venv\Scripts\python.exe manage.py migrate
.\.venv\Scripts\python.exe manage.py runserver 0.0.0.0:8000
```

- 같은 PC에서 파이프라인도 띄운다면 둘 다 8000을 쓰므로 `runserver 0.0.0.0:8010`처럼 포트를 바꿉니다.
- 파이프라인 데이터가 없을 때: `manage.py seed_rt` (목업), 지울 때 `seed_rt --clear`.
- 테스트: `manage.py check`, `manage.py test accounts telemetry`
- MySQL 계정 만들기·MongoDB 설치는 [`server/README.md`](../server/README.md), [`server/docs/mongodb-setup.md`](../server/docs/mongodb-setup.md)를 참고합니다.

---

## 4. 브랜치와 PR

브랜치 규칙:

```powershell
git switch main
git pull
git switch -c game/bridge-pathfinding        # 파트 접두어 + 작업 이름
# ... 작업, 커밋 ...
git push -u origin game/bridge-pathfinding
# GitHub에서 PR 생성 → 리뷰 1명 승인 → Squash merge
```

- `main`에는 직접 push할 수 없습니다(보호 브랜치).
- 접두어: `game/`, `pipeline/`, `server/`, `contracts/`, `docs/`
- 작업 중 `main`이 바뀌면 `git pull --rebase origin main`.
- 한 PR은 되도록 자기 폴더만 바꿉니다.

## 5. 절대 커밋하지 않는 것

- `.env` (각 폴더의 `.env.example`만 커밋)
- API 키·비밀번호·토큰 — 특히 웹 `config.js`, Godot 스크립트·씬 안에
- `game/build/`, `.godot/`, `.venv/`, Godot 실행 파일

실수로 커밋했다면 **바로 팀 채널에 알리고 키를 폐기**하세요. 커밋을 지우는 것만으로는 안전하지 않습니다.

## 6. 문제 해결

| 증상 | 해결 |
|---|---|
| 게임 에셋이 깨지거나 Godot가 `.glb`를 못 읽음 | `git lfs install` 후 `git lfs pull` |
| 파일이 전부 수정됨으로 표시됨 (줄바꿈) | `git config --global core.autocrlf false` 후 `git checkout -- .` |
| 게임이 이벤트 전송 시 401 | 게임의 `auth_token`과 파이프라인 `.env`의 `INGEST_KEY`가 같은지 확인 |
| 게임이 이벤트 전송 시 422 | 수집기 응답의 `reason` 확인, `pipeline/contract.py`와 필드 비교 |
| 서버 실시간 화면이 비어 있음 | 서버 `.env`의 `MONGODB_URI`가 파이프라인 MongoDB를 가리키는지 확인 |
| `mysqlclient` 설치 실패 | Visual C++ Build Tools 설치 또는 MySQL Connector/C 설치 후 재시도 |
