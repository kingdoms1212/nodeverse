# 파이프라인 4차 보완: 신뢰성 P1 + 프로토콜 헬스체크 (2026-11-20)

codex 검토 두 건을 반영했습니다.
- `pipeline/docs/RELIABILITY_REVIEW_REQUEST_2026-11-20.md`: P1 6건
- `pipeline/docs/PROTOCOL_HEALTHCHECK_REQUEST_2026-11-20.md`: 포트 확인 대신 실제 프로토콜 응답 확인

측정 환경은 PC "Gil"(WSL2 Docker, compose 프로젝트 `nodeverse`)입니다. 새 볼륨 기동 시험만 별도 프로젝트 `nvtest`(전용 네트워크·볼륨, 시험 후 삭제)에서 했습니다.

## 1. 신뢰성 P1

| # | 문제 | 수정 | 파일 | 확인 |
|---|---|---|---|---|
| 1 | 후보를 읽은 뒤 운영자가 `park`해도 `claim_batch()`가 메모리상 `processing`으로 돌려줘 투영이 실행됨. 이전 작업자가 늦게 결과를 덮어쓸 수 있음 | 선점 때 행마다 새 `claim_id`를 기록하고, 그 토큰이 붙은 행만 다시 읽어 처리. 완료·재시도·대기·격리 기록은 `{_id, state: processing, claim_id}`가 맞을 때만 쓰고, 아니면 `lost_claim` 로그만 남김. 크래시 복구(`recover_stale`)는 토큰을 지움 | `remote/worker.py` | 메모리 테스트: 선점 직전 park된 행은 반환되지 않음. 재선점 뒤 첫 작업자의 늦은 결과는 기록되지 않음 |
| 2 | 커서를 올린 뒤 다음 순번 깨우기가 실패하면, 앞 이벤트를 재실행해도 저장된 결과만 반환하고 뒤 이벤트는 `parked`로 남음 | 깨우기를 `wake_successor()`로 분리해 정상 처리와 재실행(커서와 같은 순번) 양쪽에서 호출 | `remote/projection.py` | 메모리 테스트: 깨우기 직전 실패 주입 → 앞 이벤트 재실행 시 중복 없이(problem_runs 1건) 뒤 이벤트가 `pending` |
| 3 | `attempt_id` 없는 이벤트가 `waiting_context`에 무기한(2일, 300회). `attempts_still_open`도 끝나지 않음 | 순번이 연속인(앞 순번이 모두 처리된) 이벤트에서 고칠 수 없는 누락은 원문과 사유를 남기고 격리(`attempt_id_required`, `attempts_still_open`). 실제 순번 누락은 그대로 대기. 기한 없던 나머지 대기 사유(`member_is_not_live` 등)도 회원 대기 기한(24시간) 뒤 `parked`. 기록 잠금 대기(`other_database_writer_active`)만 기한 없음 | `remote/projection.py`, `remote/worker.py` | 메모리 테스트: 연속 순번은 격리되고 커서 전진, 순번 누락이 있으면 계속 `sequence_gap` 대기 |
| 4 | `retry_limit`로 보관된 행을 `requeue`하면 누적 시도 21로 일시 오류 한 번에 다시 `parked` | `requeue` 때 `retry_base`(그 시점 시도 수)와 `requeued_at`을 기록. 한도는 `attempts - retry_base`로 계산. `attempts`는 누적 이력으로 유지 | `remote/requeue.py`, `remote/worker.py` | 메모리 테스트: 20회 후 보관 → requeue → 일시 오류 1회는 `retrying` |
| 5 | 3차 보완에서 재시도 행의 archive를 건너뛰게 해, 원격 DB 교체·복원 뒤 원본 0건인데 `stored`가 될 수 있음 | archive는 시도마다 확인(있으면 조회 1회). 중복 절감은 `project(archived=True)`로 유지 | `remote/worker.py` | 메모리 테스트: 원격 원본 삭제 → requeue → 원본이 다시 생긴 뒤 `stored` |
| 6 | Gold 저장 후 MongoDB 게시가 최종 실패해도 반환값을 안 봄. 새 입력이 없으면 그 창은 화면에 영영 안 고쳐짐 | 게시 실패 시 창 범위를 lake 볼륨의 `metrics/gold-republish.json`에 기록하고, 주기 스레드가 MongoDB 응답 뒤 **Gold Delta에서 그 창을 다시 읽어** `_id` 기준으로 덮어씀(반복해도 값이 부풀지 않음). 진행 파일에 `gold_republish_pending` | `spark/gold.py`, `spark/periodic.py` | 실제 연동: MongoDB 약 130초 정지 중 400건 전송 → `rt_minigame_1m`(14:42 창), `rt_alerts`(14:38~14:42) 게시 실패 기록 → MongoDB 기동 후 19행·115행 재게시, 대기 0 |

## 2. 프로토콜 헬스체크

| 대상 | 이전 (3차) | 이후 |
|---|---|---|
| 공통 모듈 | 없음 | `common/health.py`: `kafka_broker()`(그 브로커만 bootstrap으로 지정해 메타데이터 요청, 응답한 브로커 이름까지 확인), `kafka_ingest()`(계약 토픽 5개의 파티션 수·리더·ISR ≥ 2), `mongo()`(PyMongo `ping`, 3초 제한), `create_topics()`(AdminClient). CLI `python -m common.health …` |
| Kafka 브로커 컨테이너 | `nc -z` | 그대로 (이미지에 Python이 없어 프로세스 생존 확인용). 프로토콜 판단은 아래로 |
| topic-init | JVM `kafka-topics.sh` 재시도 루프 | Python 이미지에서 `AdminClient.create_topics()`. 있는 토픽은 유지, 5초 간격 최대 36회, 생성 뒤 리더·ISR 확인까지 통과해야 종료 0 |
| Spark·원격 작업자 기동 조건 | Mongo 포트 healthy | 새 일회성 서비스 `deps-ready`: MongoDB `ping`과 Kafka 수집 준비가 모두 통과해야 종료 0 → 그 뒤 기동 |
| 수집기 `/health/ready` | 메타데이터 응답만 | Kafka 수집 준비(리더·ISR). MongoDB는 넣지 않음(Mongo가 내려가도 202 접수 유지) |
| 수집기 `/health/deps` (신규) | - | 브로커별 응답·지연, 수집 준비와 토픽별 상태, MongoDB ping을 따로 표시 |
| MongoDB 컨테이너 | `/dev/tcp` | 그대로 (빠른 연결 진단). 준비 판단은 `deps-ready`의 ping |

### 확인 결과

| 시험 | 결과 |
|---|---|
| 브로커 kafka-2 정지 | `/health/deps`: kafka-2 `False`(3.0초에 실패), 나머지 정상. 수집 준비 `True`(리더 6/6, ISR 2). `/health/ready` 200, 이벤트 60건 모두 202 |
| kafka-2 복구 | 약 6초 뒤 브로커 3대 정상, ISR 3 |
| 브로커 kafka-3 동결(`docker pause`, 포트는 열림) | kafka-3만 3.0초 안에 실패 표시, 전체 점검 3.1초 |
| MongoDB 동결(포트는 열림) | `deps-ready` 검사 2회 모두 실패, 13초에 종료 1(의존 서비스 기동 안 함). `/health/deps` mongo `ServerSelectionTimeoutError`(3.0초), Kafka는 정상, 이벤트 55건 모두 202 |
| MongoDB 약 130초 정지 | 수집 412건 모두 202, Gold 재게시 정상(위 P1-6), Spark·원격 작업자·수집기 재시작 0 |
| 새 볼륨 첫 기동 (`nvtest`) | topic-init 17초에 종료 0. 토픽 5개 모두 파티션 6/3/3/3/3, RF 3, min ISR 2, 보관 604800000 |
| 같은 볼륨 재기동 | topic-init 8초에 종료 0, 새로 만든 토픽 없음(기존 5개 유지) |
| 기존 스택 재배포 | topic-init·deps-ready 1회에 통과, Spark 쿼리 8개 기존 체크포인트로 시작 |
| 종단 추적 (이벤트 1건) | 202 → Bronze 1행 → Silver 1행 → outbox `stored`(132 ms) → 원격 원본 1건·`gameplay_events` 1건 |

### 유휴 5분 CPU (p95 / 최대 / 1코어 넘은 표본)

| 컨테이너 | 3차 보완 전 | 3차 보완 후 | 4차 보완 후 |
|---|---|---|---|
| kafka-1/2/3 | 118·81·123% / 154% / 27개 | 6·5·6% / 17% / 0개 | 5·8·7% / 28% / 0개 |
| mongo | 39% / 47% / 0개 | 5% / 7% / 0개 | 6% / 10% / 0개 |
| collector | 6% / 10% / 0개 | 6% / 13% / 0개 | 6% / 13% / 0개 |
| spark | 147% / 310% / 11개 | 57% / 321% / 5개 | 97% / 298% / 8개 |

Kafka·MongoDB는 3차 수준을 유지했습니다. 수집기 `/health/ready`가 10초마다 메타데이터를 조회하지만 수집기 CPU는 같습니다. Spark 수치는 표본마다 흔들립니다(주기 작업이 1분마다 DLQ·격리 집계를 추가로 읽음).

## 3. 테스트

- 파이프라인 단위 테스트 75개 통과, 1개 건너뜀(PySpark 필요). 추가: P1 8개(`test_remote_projection.py`), 헬스체크 7개(`test_health.py`)
- 서버 57개 통과

## 4. 이번에 하지 않은 것 (별도 검토)

- 게임 쪽 P2(디스크 큐 쓰기 실패·강제 종료, 로그아웃 뒤 계정 전환), 최신 EXE 종단 시험, 다수 사용자 역순 입력 부하 시험
- Silver `MERGE` 범위·워터마크·체크포인트 경로 변경, `OPTIMIZE ZORDER` 전후 비교
- 같은 배치 안 같은 ID·다른 본문의 선택 규칙
- 선점 후 처리 도중 `park`된 행: 투영은 이미 진행 중이라 멱등 기록(ensure)은 끝까지 가고, 최종 상태만 기록하지 않습니다(`parked` 유지). 투영 자체를 중간에 멈추지는 않습니다
