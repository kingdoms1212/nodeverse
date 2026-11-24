# 통합 저장소 종합 검토 (2026-11-20)

대상: `game/` (nodeverse-game `main` fd2f390), `pipeline/` (NVG pipeline-v2), `server/` (dashboard `main`).
코드를 읽고 맞춰 본 결과이며, 실제 세 파트를 함께 띄워서 돌려 본 것은 아닙니다.

## 요약

| 구간 | 상태 |
|---|---|
| 게임 → 수집기 (`POST /v2/events`) | ✅ 형식 일치. 필드·이벤트 종류·미니게임 24종·details 키·시간 형식 모두 `pipeline/contract.py`와 맞음 |
| 수집기 → Kafka → Spark → MongoDB `rt_*` | ✅ 파이프라인이 쓰는 `rt_minigame_1m`, `rt_alerts`, `rt_updates`, `rt_versions`, `rt_summary`를 서버 실시간 화면이 그대로 읽음 |
| 게임 → 서버 회원/세이브 (`/api/game/auth/*`) | ✅ 경로 일치, Django 서버 주소로 보냄 |
| 게임 → 서버 플레이어 연동 (`/api/game/player-login`) | ❌ 잘못된 주소·잘못된 키 (아래 1번) |
| 서버 분석 화면(플레이어·이벤트·통계) | ⚠️ 데이터를 넣는 쪽이 없음 (아래 2번) |
| 계약 문서 | ⚠️ 서버 JSON Schema가 옛 초안 (아래 3번) |

## 꼭 고칠 것

### 1. 게임의 `player-login` 호출이 서버에 닿지 않음
`game/scripts/network_manager.gd:221`
- 주소를 `get_base_url()`(수집기, 기본 192.168.0.81)로 만들고 있습니다. 회원·세이브 호출처럼 `get_api_base_url()`(Django, 기본 192.168.0.29)이어야 합니다.
- 인증 헤더에 수집기용 `INGEST_KEY`를 `Bearer`로 넣습니다. 서버는 `GAME_SERVER_API_KEY`를 기대하고, 서버 `.env.example`은 이 키를 "서버 간 통신 전용, 클라이언트에 넣지 말 것"이라고 적고 있습니다.
- 결과: 지금은 항상 실패하고 로컬 UUID로 진행합니다(게임은 멈추지 않음). 서버 쪽 플레이어 ID가 게임 이벤트의 `player_id`와 연결되지 않습니다.
- 정할 것: 게임이 직접 부를 수 있는 인증 방식(예: `/api/game/auth/login` 응답에 player_id를 담아 주기)으로 바꿀지, 런처/서버 간 호출로 옮길지.

### 2. 서버의 분석 화면은 비어 있게 됨
서버 `staff/players`, `staff/events`, 요약·시각화 API는 MongoDB의 `game_sessions`, `game_problems`, `problem_runs`, `gameplay_events`, `solution_attempts`를 읽습니다.
이 컬렉션은 서버의 `POST /api/game/records`로만 채워지는데, 게임도 파이프라인도 이 주소를 부르지 않습니다.
실제 게임 데이터는 파이프라인을 거쳐 `rt_*`(실시간 집계)에만 들어가므로, 서버의 `staff/live` 화면만 실제 데이터로 움직입니다.
- 정할 것: 분석 화면을 Delta/Gold 결과(또는 Silver 이벤트를 Mongo로 내보낸 컬렉션)에서 읽도록 바꿀지, 파이프라인이 `records` 형식으로도 써 줄지.

### 3. 이벤트 계약 문서가 두 벌
`contracts/README.md`에 정리한 대로 서버의 `behavior-event.schema.json`(`schema_version: "2.0"`, `payload` 구조)은 게임·파이프라인이 실제로 쓰는 v2(`schema_version: 2`, `details` 평면 객체, `minigame_id`·`category` 포함)와 다릅니다.
게임과 파이프라인은 서로 맞으므로 **`pipeline/contract.py`를 기준**으로 삼고 서버 스키마를 갱신하거나 지우는 것을 권합니다.

## 보안

- **Groq 키**: 원본 게임 저장소 히스토리에 실제 키가 남아 있습니다(2026-11-09 ~ 11-16). 통합 저장소에서는 지웠지만 원본 GitHub에는 그대로 있으므로 **키 폐기가 필요**합니다.
- **코봇 채팅**(`game/website/cobot-chat.js:270`, `server/website/`도 동일): 브라우저에서 Groq API를 직접 부르는 구조라 키를 넣는 순간 방문자 모두에게 노출됩니다. Django에 프록시 엔드포인트를 두는 방식으로 바꿔야 합니다.
- **웹 대시보드 암호** `nd1234`(`game/website/dashboard.html:382`): 브라우저 코드 안에서 비교하는 체험용 암호라 보호 기능이 없습니다. 실제 관리 화면은 서버의 `staff/`를 쓰고, 이 페이지는 데모로만 두세요.
- **수집 키 기본값**: 게임 기본값 `replace-with-a-long-random-local-key`는 36자라 수집기 최소 길이(24자)를 통과합니다. 수집기 `.env`에 이 값을 그대로 쓰면 아무나 이벤트를 넣을 수 있으니 시연 전에 실제 값으로 바꾸세요. 클라이언트에 들어가는 키라 비밀이 될 수는 없다는 점도 감안해야 합니다(속도 제한·DLQ로 방어).
- 서버의 `recovery_key`는 평문으로 저장됩니다(`server/telemetry/game_auth_api.py:134`). 비밀번호처럼 해시로 저장하는 편이 안전합니다.
- 잘 된 점: 서버 `SECRET_KEY` 필수·`DEBUG` 기본 False, 테스트 관리자 로그인은 DEBUG+로컬 접속에서만 열림, 게임 비밀번호는 서버에서 Django 해시로 저장, 로그인 시간 차 공격 방지, 수집기 키 비교는 상수 시간 비교.

## 설정·운영

- IP가 코드에 박혀 있습니다: 수집기 `192.168.0.81`, Django `192.168.0.29`(`network_manager.gd:30,33`, `scenario_bot_runner.gd:146`, 디버그 패널). 시연 장소 네트워크가 바뀌면 빌드를 다시 해야 하므로, `user://network_config.json`이나 서버 manifest에서 받는 방식을 기본으로 두는 게 좋습니다.
- 수집기와 Django가 둘 다 8000번 포트를 씁니다. 다른 PC라면 문제없지만 한 PC에서 띄우면 충돌합니다(파이프라인 `COLLECTOR_PORT`로 조정 가능).
- 미니게임 ID를 못 찾으면 `ST01/DS`로 처리합니다(`network_manager.gd:310`). 잘못된 ID가 배열 실습 데이터로 섞여 들어가므로, 차라리 전송하지 않고 로그를 남기는 편이 낫습니다.
- `game/README.md`의 아키텍처 그림은 아직 `/v1/events`, 포트 8010 대시보드 기준입니다. 실제 코드는 `/v2/events`와 Django `staff/`입니다.

## 저장소 정리

- `game/website/`와 `server/website/`가 거의 같은 파일입니다(차이 3개). `server/docs/sync_website.py`로 복사하는 구조로 보이는데, 한쪽만 원본으로 정하고 나머지는 빌드 시 복사하도록 하면 두 곳을 따로 고치는 일이 사라집니다.
- `game/scenes/AssetShowcase.tscn`이 30MB 텍스트 씬입니다. 생성기로 다시 만들 수 있다면 `.scn`(바이너리, LFS)으로 저장하거나 커밋에서 빼는 것을 고려하세요.
- 저장소 크기: git 40MB + LFS 약 296MB(게임 에셋 974개).
- CI: `pipeline`은 단위 테스트를 돌리고, `server`·`game`은 아직 골격입니다. 이 PC(Windows ARM)에서는 `confluent-kafka`가 설치되지 않아 파이프라인 테스트를 돌리지 못했습니다.

## 추가 확인 (같은 날 15:50 최신화 후)

- **2번 해결됨**: 파이프라인 `feature/codex-v2-remote-sink`(32b0248)가 검증된 학습 이벤트를 MongoDB `pipeline_learning_events`로 내보내고(`pipeline/remote/worker.py`), 서버(3dfece8)가 이를 이벤트·개요 화면에서 읽습니다(`server/telemetry/pipeline_events.py`).
- **1번은 그대로**: `game/scripts/network_manager.gd:244`의 `player-login`은 여전히 수집기 주소(`get_base_url()`)로 보냅니다. 게임(88394b1)은 로그인한 회원 ID를 `player_id`로 보내도록 바뀌었으므로, 서버의 회원 매칭은 이 호출 없이도 동작할 수 있습니다. 호출을 고치거나 지울지 게임·서버가 정하면 됩니다.
