# 파이프라인 CPU 경량화 (2026-11-20)

codex 검토 "NODEVERSE 파이프라인 CPU 최적화 요청"을 반영한 변경과 측정 결과입니다. 측정 환경은 PC "Gil"(Win11 ARM64, WSL2 Docker, compose 프로젝트 `nodeverse`, `--profile remote`)입니다. CPU 수치는 `docker stats` 표본이며 100%가 1코어입니다.

## 1. 바꾼 것

| 항목 | 이전 | 이후 | 파일 |
|---|---|---|---|
| Kafka 헬스체크 | 브로커마다 10초에 한 번 `kafka-topics.sh --list` (JVM 기동) | `nc -z localhost 9092` (포트 수신 확인만) | `pipeline/build_compose.py` |
| topic-init | 브로커가 healthy면 바로 토픽 생성, 실패 시 종료 | 생성 전체를 5초 간격 최대 36회(약 3분) 재시도. 실제 Kafka API(메타데이터 쿼럼, RF3용 브로커 3대)가 응답해야 통과 | `pipeline/build_compose.py` |
| MongoDB 헬스체크 | 10초마다 `mongosh` ping (Node.js 기동) | `bash` `/dev/tcp/127.0.0.1/27017` 연결. mongod는 저장소 복구가 끝난 뒤 27017을 엽니다 | `pipeline/build_compose.py` |
| rt_summary (10초) | 어제·오늘 파티션 전체(약 21만 행)를 집계 | 파티션 필터는 유지하고 `received_at` 최근 5분으로 먼저 거른 뒤 두 열만 집계 | `pipeline/spark/periodic.py` |
| 버전 보급률 (60초) | 매분 Delta MERGE + Mongo `rt_versions` 재쓰기 | 매분 다시 계산(24시간 창 만료는 그대로 반영)하되 (종류, 버전, 설치 수)가 지난번과 같으면 MERGE·재쓰기 생략. Mongo 쓰기가 실패하면 다음 분에 다시 씀 | `pipeline/spark/periodic.py`, `pipeline/spark/sinks.py` |
| 원격 적재 상태 집계 (10초) | 전체 outbox `$group` (COLLSCAN) + 사유 집계 | 상태별 개수는 `(state, …)` 인덱스로 `distinct` + `count_documents`. 사유 집계는 `(state, reason)` 인덱스 커버 쿼리로 60초마다 | `pipeline/remote/requeue.py`, `pipeline/remote/worker.py` |
| archive 중복 | `process_job()`과 `Projector.project()`가 같은 이벤트를 두 번 archive (두 번째는 find_one + 비교만) | 행마다 한 번. 재시도된 행은 `remote_archive_stored_at`이 있으면 건너뜀. `project(event, archived=True)` | `pipeline/remote/worker.py`, `pipeline/remote/projection.py` |
| 관측 | 주기 작업 소요 시간 기록 없음 | `rt_summary in …s`, `version adoption in …s (written/unchanged)` 로그 | `pipeline/spark/periodic.py` |

테스트: `pytest` 60개 통과(1개 skip). 새 테스트 2개를 추가했습니다. 하나는 인덱스 개수 집계가 예전 `$group`과 같은지, 다른 하나는 archive가 행마다 한 번만 쓰이는지(재시도 포함) 확인합니다.

## 2. 유휴 5분 측정 (부하 없음)

| 컨테이너 | 이전 중앙값 / p95 / 최대 / 1코어 이상 표본 | 이후 |
|---|---|---|
| kafka-1 | 2.1 / 118.4 / 153.5 / 9 | 3.7 / 6.2 / 16.1 / 0 |
| kafka-2 | 2.4 / 80.9 / 131.8 / 5 | 2.9 / 5.1 / 15.1 / 0 |
| kafka-3 | 2.6 / 123.3 / 152.8 / 13 | 3.2 / 6.4 / 17.4 / 0 |
| mongo | 2.0 / 38.8 / 47.1 / 0 | 1.7 / 5.3 / 6.7 / 0 |
| spark | 11.1 / 147.0 / 309.6 / 11 | 11.8 / 56.9 / 321.4 / 5 |
| remote-writer | 0.9 / 2.1 / 7.1 / 0 | 0.8 / 2.2 / 6.6 / 0 |
| collector | 1.2 / 5.6 / 10.4 / 0 | 1.3 / 5.7 / 12.9 / 0 |

표본은 컨테이너당 약 142개(약 2초 간격)입니다. 이후 유휴 상태에서 `rt_summary`는 34회 중앙값 0.22초, 버전 집계는 6회 모두 `unchanged`(약 0.9초, MERGE 없음)였습니다. Silver audit 배치는 이전 유휴 30분 기준 중앙값 0.4초, 최대 1.9초였습니다.

## 3. 부하 시험 (같은 조건: 6,002건, 초당 100건, 플레이어 300명, 592세션)

| 항목 | 이전 | 이후 |
|---|---|---|
| 수집 응답 p50 / p95 | 11 / 15 ms | 12 / 16 ms |
| Silver learning 배치 중앙값 (MERGE) | 5.55초 (5.30초) | 5.50초 (5.10초) |
| gold_1m 배치 중앙값 | 2.45초 | 3.75초 |
| audit 배치 중앙값 | 1.30초 | 2.50초 |
| rt_summary 소요 | 기록 없음 | 중앙값 0.41초, 최대 1.26초 |
| 대시보드 지연 (probe_latency 3회) | 최대 11.1초, 15초 통과 | 최대 12.4초, 15초 통과 |
| 원격 적재 완료 (전송 시작부터 남은 행 0까지) | 114초 | 120초 |
| 원격 적재 queue→완료 평균 / 최대 | 6.0초 / 52.9초 | 6.5초 / 65.7초 |
| 최종 outbox 잔량 (이번 부하분) | 0 (6,002건 stored) | 0 (6,002건 stored) |
| sink 실패 / 쿼리 재시작 | 0 / 0 | 0 / 0 |
| 부하 중 kafka 1코어 이상 표본 (3대 합) | 20 | 1 |

부하 중 Spark CPU(중앙값 400~490%)와 Silver MERGE 시간은 사실상 같습니다. 이번 변경은 유휴·주기 작업 비용을 줄이는 것이고, 부하 처리량 병목(Silver MERGE, 원격 적재)은 그대로입니다. gold_1m·audit 배치와 원격 완료 시간의 차이는 한 번씩만 잰 값이라 잡음 범위로 봅니다(같은 쿼리를 바꾸지 않았습니다).

### rt_summary 결과 비교 (같은 시각, 같은 테이블에서 이전·이후 식을 번갈아 3회)

| 회차 | events_last_1m | active_sessions_5m | 이전 식 소요 | 이후 식 소요 |
|---|---|---|---|---|
| 부하 직후 (이전 측정) | 4,758 / 4,758 | 592 / 592 | 5.75 · 1.07 · 0.46초 | 2.25 · 0.59 · 0.64초 |
| 부하 직후 (이후 측정) | 4,635 / 4,635 | 592 / 592 | 5.79 · 1.02 · 0.51초 | 2.26 · 0.52 · 0.58초 |

값은 매번 같고, 첫 실행(캐시 없음) 기준 약 2.5배 빠릅니다. 스캔 대상은 파티션 21.9만 행 중 최근 5분 6,002행입니다. 캐시가 데워진 뒤에는 차이가 작습니다.

### 원격 적재 상태 집계 실행 계획 (outbox 134,580행)

| 쿼리 | totalDocsExamined | totalKeysExamined | 실행 시간 |
|---|---|---|---|
| 이전: 전체 `$group` by state | 134,580 | 0 | 42 ms |
| 이후: `distinct("state")` | 0 | 6 | 0 ms |
| 이후: `count({state})` 6회 합계 | 0 | 134,586 | 약 20 ms (stored 123,838건이 대부분) |
| 사유 집계 parked / waiting / blocked | 0 / 0 / 0 | 2,262 / 20 / 2,245 | 각 1 ms 이하 |

문서를 하나도 읽지 않게 되어 캐시를 밀어내지 않습니다. 상태별 개수는 기존 `$group` 결과와 같았습니다(blocked 2,245, parked 2,262, pending 16, quarantined 6,200, stored 123,837, waiting_context 20). 사유 집계는 원래도 `(state, reason)` 인덱스를 쓰고 있어 차이가 없고, 주기만 10초에서 60초로 늘렸습니다. 남은 비용은 `stored` 인덱스 키 12만 개 스캔(10초마다 약 20 ms)이며, 완료 행 TTL(30일)로 상한이 있습니다.

## 4. 장애·지연 기동 재현

| 시나리오 | 결과 |
|---|---|
| 브로커 3대 정지 상태에서 topic-init 먼저 시작, 25초 뒤 브로커 기동 | 재시도 로그 5회 후 41초에 exit 0. 브로커 3대 healthy |
| 브로커 1대만 살아 있을 때 새 RF3 토픽 생성 | 포트 헬스체크는 통과하지만 생성은 실패하고 재시도를 계속함(3회 모두 실패, exit 1). 즉 "포트 열림"만으로는 통과하지 않음 |
| MongoDB 30초 정지 후 재기동 | 38초 만에 healthy. 이후 25초 안에 rt_summary(3.4초 전)와 원격 적재 상태(0.4초 전)가 다시 갱신됨. Spark sink 실패 0, 쿼리 재시작 0, 컨테이너 재시작 0 |
| 같은 이벤트 207건을 두 번 전송 | 수집 414회 모두 202. Bronze 414행, Silver 207행(고유 207), outbox 207행 모두 stored |

## 5. 알고 있는 한계와 남은 병목

- TCP 헬스체크는 "프로세스가 멈췄지만 포트는 열린" 상태(예: mongod 정지 신호)를 잡지 못합니다. 이 compose의 헬스체크는 기동 순서를 거는 데만 쓰이고(자동 재시작 없음), 실행 중 장애는 Spark sink 재시도·실패 수, 원격 적재 로그, 화면의 "수집 지연" 표시가 잡습니다.
- 24시간 창 만료: 버전 집계는 매분 다시 계산하므로 플레이어가 24시간을 넘기면 설치 수가 바뀌고 그때 MERGE·재쓰기가 일어납니다. 이번 측정 동안 실제로 만료되는 플레이어는 없어서 만료 시점 쓰기는 코드로만 확인했습니다.
- 유휴 Spark 최대값(약 320%)은 그대로입니다. audit·마이크로배치 트리거 쪽 비용이며 이번 범위 밖입니다.
- 부하 처리량 병목은 Silver MERGE(배치당 약 5초, UUIDv4·v7 혼합 데이터)와 원격 적재(초당 약 50건)입니다. UUIDv7만 있는 새 lake에서의 MERGE 비교는 아직 하지 않았습니다.

## 6. PL-8: 거절·격리 이벤트 표시 (같은 날 추가)

- `spark/periodic.py` `refresh_rejects()`: 60초마다(버전 집계 직후) Kafka `nv.dlq.v1`을 최근 24시간 범위로 배치 읽기(`startingTimestamp`, 해당 구간에 기록이 없으면 빈 결과)하고 `silver.quarantine`을 `detected_at` 24시간으로 읽어 MongoDB `rt_rejects/global`에 기록합니다. 필드: `dlq.last_60m`, `dlq.last_24h`, `dlq.reasons_60m`(상위 10), `dlq.recent`(최근 5건: 시각·사유·경로·첫 오류 필드·메시지, 원문 제외), `quarantine.last_60m`, `quarantine.last_24h`, `quarantine.reasons_60m`, `updated_at`. 실패해도 rt_summary·버전 집계는 계속 돕니다. 소요 약 2초/분.
- `tools/dlq_peek.py`: DLQ 원문 확인. collector 컨테이너 안에서 `python -m tools.dlq_peek --minutes 60 [--reason schema_error] [--limit 20] [--raw]`. 컨슈머 오프셋을 커밋하지 않습니다. collector 이미지에 포함(`Dockerfile`).
- 서버: `/api/admin/live/summary` 응답에 `rejects` 추가, 실시간 현황에 "거절·격리 이벤트" 카드(60분·24시간 건수, 사유, 최근 5건). 테스트 `test_rejects_from_pipeline`.
- 확인: 잘못된 요청 5건(알 수 없는 종류 3, JSON 오류 1, 필드 누락 1)을 보내고 1분 안에 카드에 같은 건수와 사유가 표시됨.
- 남은 것: DLQ 재처리 스크립트.
