# 5차 보완: codex 신뢰성 검토의 남은 항목 (2026-11-21)

`pipeline/docs/RELIABILITY_REVIEW_REQUEST_2026-11-20.md`의 P2 시나리오와 제출 요구(Delta 증거, ZORDER 비교) 중 4차에서 하지 않은 것을 처리했습니다. 측정은 PC "Gil"의 로컬 스택(compose 프로젝트 `nodeverse`)에서 했고, ZORDER 비교는 live 테이블을 건드리지 않도록 별도 경로(`/data/zorder-test`, 시험 후 삭제)의 복사본으로 했습니다.

## 1. 게임 디스크 큐 (`game/scripts/network_manager.gd`)

| 문제 | 수정 |
|---|---|
| 큐 파일을 바로 덮어써서, 쓰는 도중 종료되면 깨진 JSON이 남음 | `_atomic_write()`: `<파일>.tmp`에 쓰고 쓰기 오류·다시 읽은 길이를 확인한 뒤 최종 파일로 교체 |
| 쓰기 실패(디스크 부족, 권한)를 알 수 없음 | 실패하면 `disk_write_failures`, `last_disk_error`를 남기고 오류 로그. 이벤트는 메모리 큐에 남아 전송을 계속 시도 |
| 깨진 파일은 조용히 건너뜀 | 시작할 때 완전한 `.tmp`(최종 파일 없음)는 최종 파일로 올리고, 읽을 수 없는 파일은 `user://telemetry/corrupt/<원래이름>.<사유>.<시각>`으로 옮겨 보관 (사유: `partial_write`, `invalid_json`, `missing_fields`) |
| rejected로 옮길 때 복사본 쓰기가 실패하면 원본도 삭제됨 | 복사본이 끝까지 써졌을 때만 pending 원본 삭제 |

## 2. 로그아웃·계정 전환 (`game/scripts/network_manager.gd`)

| 문제 | 수정 |
|---|---|
| 로그아웃 뒤에도 이전 회원의 `run_id`·`attempt_id`가 남아, 다음 회원 이벤트가 같은 run으로 묶일 수 있었음(원격 적재에서 `run_context_mismatch` 충돌) | 플레이어가 바뀌면 `session_id`, `run_id`, `attempt_id`, `sequence`를 모두 새로 시작(`_start_new_session_ids`) |
| `start_task()`의 `_update_player_id()`가 플레이어를 바꿔도 세션을 유지 | 모든 경로가 `_apply_player_id()`를 거쳐 바뀌면 새 세션 |
| 이미 만들어진 이벤트 | 그대로 (큐·디스크에 완성된 사전으로 있어 원래 회원·세션 유지) |

검증: Godot 4.7.2 헤드리스 시험 `game/scripts/tools/test_disk_queue.gd` 17개 항목 통과.
```
godot --headless --path game -s res://scripts/tools/test_disk_queue.gd
```
원자적 쓰기, 쓰기 실패 감지(없는 폴더), 완전한 .tmp 복구, 반쯤 쓴 .tmp·깨진 JSON의 corrupt/ 보관, A 이벤트 귀속 유지, 로그아웃 뒤 새 세션·run, B의 독립 세션을 확인합니다. 실제 디스크 부족과 실행 파일(EXE) 강제 종료는 재현하지 않았습니다(같은 실패 경로를 없는 폴더 쓰기로 확인).

## 3. 다수 사용자·역순 입력 부하 시험

`tools/dummy_data.py`에 `--reverse-sessions N`(N개 세션을 순번 역순으로 전송), `--sessions-out`을 추가했습니다.

조건: 6,000건, 초당 150건, 플레이어 300명, 577세션 중 30세션 역순, 전송 스레드 48개.

| 항목 | 역순 30세션 (319건) | 정상 547세션 (5,681건) |
|---|---|---|
| 최종 상태 | 319건 모두 stored | 5,681건 모두 stored |
| 회원이 섞인 세션 | 0 | 0 |
| 순번 불연속 세션 | 0 | 0 |
| 누락 세션 | 0 | 0 |
| queue→완료 p50 / p95 / 최대 | 14.9초 / 48.7초 / 61.1초 | 11.1초 / 31.9초 / 54.4초 |

수집 6,000건 모두 202(p95 15 ms), 전송 끝 40초 뒤 69초 만에 대기 0. 역순 세션은 앞 순번을 기다리느라 조금 늦지만 다른 세션의 처리를 막지 않았습니다(정상 세션 p50 11.1초).

## 4. 같은 배치에 같은 ID

20개 ID는 같은 본문을 두 번, 20개 ID는 본문을 바꿔 두 번, 연달아 보냈습니다(80건 모두 202).

| 경우 | Bronze | Silver | Silver가 남긴 본문 | 감사 쿼리 |
|---|---|---|---|---|
| 같은 ID·같은 본문 | 40행 | 20행 | 20/20 먼저 들어온 것 | id_conflict 0 |
| 같은 ID·다른 본문 | 40행 | 20행 | 20/20 먼저 들어온 것 | id_conflict 20, 모두 Silver에 없는 쪽 |

이번 시험에서는 Silver와 감사 판정이 일치했습니다. 다만 Silver 중복 제거(`dropDuplicatesWithinWatermark`)는 한 배치 안에서 어느 행을 남길지 보장하지 않습니다. 감사는 (파티션, 오프셋) 순서로 판정합니다. 규칙을 바꾸려면 상태 저장소와 체크포인트를 다시 만들어야 해서 별도 설계 검토로 남깁니다.

## 5. 적재 중 대시보드 조회

부하 시험 동안 1초 간격으로 `/api/admin/summary`, `/api/admin/visuals`, `/api/admin/players`, `/staff/`, `/api/admin/live/summary`를 조회했습니다. 모두 200이었고 500은 없었습니다(조회 한 바퀴가 무거워 약 35초에 1회, 3회). 적재가 끝난 뒤 15초 간격으로 두 번 읽은 summary는 같았습니다(수렴).

## 6. Delta 증거

| 확인 | 결과 |
|---|---|
| 같은 이벤트 160건 재전송 → Gold | 재전송 전후 최근 10분 `rt_minigame_1m`(attempts·successes·failures·hints) 동일 |
| 같은 이벤트 207건 재전송 (3차) | Bronze 414행(수신 이력 둘 다 보존), Silver 207행 |
| 기존 체크포인트 복구 | 3·4차 재배포와 Spark 재시작 때마다 쿼리 8개가 기존 체크포인트로 시작, 재전송 중복 없음 |
| OPTIMIZE ZORDER (복사본) | 아래 표 |

운영 Silver 테이블은 이미 파일 1개로 압축돼 있어 비교가 의미 없었습니다. 그래서 같은 23.2만 행을 스트리밍처럼 작은 파일 300개(도착 순서, `event_date` 파티션)로 다시 쓴 복사본에서 `OPTIMIZE … ZORDER BY (minigame_id, player_id, event_id)` 전후를 비교했습니다(`spark/maintain.py`와 같은 열).

| 항목 | 전 | 후 |
|---|---|---|
| 행 수 | 232,366 | 232,366 |
| 파일 수 / 크기 | 300개 / 33.7 MB | 1개 / 30.5 MB |
| event_id 200개 조회 (MERGE 키) | 299개 파일, 0.99초 | 1개 파일, 0.63초 |
| player_id 1명 | 116개 파일, 0.52초 | 1개 파일, 0.43초 |
| minigame_id 1개·최근 하루 | 300개 파일, 0.60초 | 1개 파일, 0.42초 |
| OPTIMIZE 소요 | | 4.0초 |

이 크기에서는 압축 효과(파일 수)가 대부분이고, ZORDER에 따른 파일 건너뛰기는 파일이 여러 개 남는 큰 테이블에서 의미가 생깁니다. 정기 실행은 스트리밍을 멈춰야 해서(`maintain.py` 안내) 운영 일정과 함께 정해야 합니다.

## 7. 이번에도 하지 않은 것

- 최신 배포 EXE로 하는 실제 게임 종단 시험(문제 시작·시도·오답·힌트·성공, 강제 종료·재실행, 계정 전환). 게임 코드는 헤드리스 시험으로만 확인
- 실제 디스크 부족 재현
- Silver `MERGE` 날짜 범위·워터마크·체크포인트 경로 변경, 같은 배치 선택 규칙 변경 (설계 검토)
