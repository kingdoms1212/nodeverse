# 2단계 완료 보고 — 168시간 / 48시간 비교

2026-11-23 / 판단: **비교 시험 완료, 운영 48시간 전환 보류. 운영 168시간 유지.**

## 무엇을 시험했나

운영 데이터·체크포인트를 복사하거나 변경하지 않고 네트워크 없는 별도 컨테이너에서 실행했습니다. 2 CPU 제한, 메모리 3GiB 제한, Spark local[2]·드라이버 1GiB·shuffle 2, Spark 3.5.3 / Delta 3.2.1입니다. 공용 Spark 소스를 읽기 전용으로 연결하고 기존 Ivy JAR 캐시도 읽기 전용으로 사용했습니다. 테스트 결과 디렉터리 외 업무 데이터에는 쓰지 않습니다. 원격 MySQL·MongoDB·운영 Kafka에 접속하지 않았습니다.

- 고유 ID 2,000개와 같은 내용 재전송, 다른 내용 재전송, Kafka 시각 진행용 이벤트, 오래된 Kafka 시각의 신규 이벤트.
- 168h 신규 체크포인트 / 48h 신규 체크포인트 / 168h 체크포인트에서 중간에 48h로 재개하는 세 경우.
- 운영과 같은 `withWatermark(kafka_ts).dropDuplicatesWithinWatermark(event_id)`와 RocksDB 상태 저장소를 실제 실행.
- 각 경우 중간 stop/start로 체크포인트 재개 확인.
- 승인된 출력의 고유 행을 운영 `silver.merge_condition()`으로 실제 Delta insert-only MERGE 두 번 실행. 원래 내용 유지·재실행 중복 없음 확인.
- 별도 Spark 프로세스에서 환경변수 168/48 각각의 드라이버 설정과 실제 Python 작업자 두 태스크의 감사 타임아웃 값 확인.
- 실제 `audit_state.audit()` 함수의 타임아웃 콜백·재입력을 가짜 GroupState로 검증.

**시간을 7일 기다린 시험이 아닙니다.** kafka_ts를 합성해 이벤트 시간만 전진시켰고, 감사의 49시간 무활동은 결정적 모의 시험입니다. 입력 경로도 Kafka가 아닌 격리 JSON 파일 스트림입니다. 169시간 지점은 연산자 경계 확인용이며 원래 occurred_at이 169시간 전인 게임 로그는 수집기의 6일 수용 규약을 초과하므로 정상 게임 요청 시험으로 해석하면 안 됩니다.

## 주요 실측

| 항목 | 신규 168h | 신규 48h | 168h → 48h 기존 체크포인트 |
|---|---:|---:|---:|
| 초기 고유 상태 키 | 2,000 | 2,000 | 2,000 |
| 97시간까지 시각 진행 후 상태 키 | 2,002 | 2 | 2,002 |
| 그 시점 상태 추정 바이트 | 95,616 | 102,122 | 95,600 |
| 97시간 시점 기존 ID 2개 재전송 시 추가 승인 | 0 | 2 | 0 |
| 오래된 Kafka 시각 신규 1건 | 승인 | 워터마크로 제외 | 워터마크로 제외 |
| 최종 Delta 고유 행 수* | 2,004 | 2,003 | 2,003 |
| 원래 2,000개 ID의 최종 고유 건수 | 2,000 | 2,000 | 2,000 |
| MERGE 두 번 실행 중복·원래 내용 덮어쓰기 | 없음 | 없음 | 없음 |
| 초기 제외 입력 배치 시간 중앙값 | 1,924ms | 2,188ms | 2,032ms |

\* 시각 진행용 이벤트와 오래된 신규 이벤트도 포함하고 seed는 제외합니다. 스트림 승인 건수와 최종 고유 행 수는 다릅니다. 이 시험은 의도적으로 늦은 신규 기록을 포함하므로 전체 최종 건수가 같지 않은 것이 관측 결과입니다.

초기 배치는 168h를 먼저 실행한 JVM 준비 비용이 커서 성능 비교에 사용하지 않았습니다. 나머지도 경우별 8개 입력 배치의 단일 실행이며 공유 PC의 부하·캐시 영향을 받습니다. `elapsed_ms`는 processAllAvailable와 출력 count까지 포함하므로 순수 연산 지연이 아닙니다. 표의 중앙값은 Spark triggerExecution 기준입니다. 반복·순서 교차·지속 부하가 없으므로 처리량 향상이나 통계적 우위를 주장하지 않습니다. **키 수 감소가 즉시 메모리 감소로 이어진다는 근거도 없습니다.**

## 의미와 위험

1. **48h는 ‘수신 후 정확히 48시간에 상태 삭제’가 아닙니다.** 이 시험에서는 49시간에도 기존 키를 기억했고 97시간 진행 후 신규 48h 상태가 정리됐습니다. 최대 관측 이벤트 시각·워터마크·키의 만료 시점·후속 트리거가 함께 작동합니다. 단순 벽시계 TTL로 설명하면 안 됩니다.
2. **장애·체크포인트 재처리 중 오래된 Kafka 레코드가 빠질 수 있습니다.** 신규 기록이라도 현재 워터마크보다 오래된 kafka_ts면 제외됩니다. 이는 장기 오프라인 게임 로그와는 다릅니다. 게임이 오늘 재전송해 Kafka가 오늘 찍은 시각과, Kafka 안에서 오래 대기하던 레코드를 구분해야 합니다.
3. **기존 체크포인트에서 설정만 바꾸면 신규 48h 상태와 같지 않습니다.** 이번 Spark/상태 구조에서는 재개 자체는 성공했지만 97시간 지점에서 2,002개 기존 키가 남았습니다. 이 결과는 모든 상태 변경의 호환성 보장이 아닙니다. 운영 체크포인트 삭제·강제 재작성은 금지하고 별도 섀도 환경으로 검증해야 합니다.
4. **충돌 감사 범위가 축소됩니다.** Silver는 Kafka 시각의 워터마크지만 감사는 ProcessingTimeTimeout입니다. 양쪽이 같은 환경변수를 사용해도 의미가 다릅니다.

| 감사 함수 시험 | 168h | 48h |
|---|---|---|
| 첫 입력 | first | first |
| 같은 해시 재전송 | 출력 없음 | 출력 없음 |
| 만료 전 다른 해시 | id_conflict | id_conflict |
| 49시간 무활동·타임아웃 처리 후 다른 해시 | id_conflict | **first** |
| 실제 Python 작업자 타임아웃 ms | 604,800,000 × 2태스크 | 172,800,000 × 2태스크 |

5. **insert-only MERGE만으로 전체 파이프라인 충돌 검증을 대체할 수 없습니다.** 이 시험은 기존 내용 유지와 고유 건수 불변을 확인했지만 운영 outbox는 승인 source를 별도로 stage합니다. 상태 만료 뒤 다시 승인된 기록이 추가 작업·충돌·보관을 만들 수 있습니다. 영속 ID→해시 검증 또는 장기 감사 보존 정책이 필요합니다.

## 이번에 반영한 코드

- `build_compose.py`와 생성 `compose.json`: Spark에 `DEDUP_TTL_HOURS=${DEDUP_TTL_HOURS:-168}` 전달. 기존에는 .env에 적어도 Spark environment에 전달되지 않았습니다.
- `.env.example`: 기본 168 명시. 실제 운영 .env는 변경하지 않았습니다.
- 감사 모듈과 TTL ms 계산: 0 이하 값 거부.
- 재현 도구 `tools/dedup_compare.py`, `tools/dedup_audit_probe.py`, `compose.dedup-experiment.yaml`, 설정 검사 추가.
- 근거: `docs/evidence/DEDUP_STAGE2_2026-11-23.json`. 원시 Spark 로그·체크포인트는 Git 제외 `results/dedup-stage2-20261123/`에 보관.

전체 Python 검사 **111 passed / 1 skipped**. Windows venv의 PySpark 의존 검사는 기존처럼 건너뛰었으나, 위 Spark·Delta 통합 시험은 Docker에서 별도로 실제 실행해 종료 코드 0을 확인했습니다. 워커 환경변수 시험 두 경우도 종료 코드 0입니다.

## 적용 결정과 남은 검증

**지금 운영 48h 전환은 하지 않습니다.** 운영 컨테이너 재시작, 운영 TTL·체크포인트·원격 DB 설정 변경 모두 하지 않았습니다. 마지막 관측에서 168h, 수집기·원격 MongoDB 정상, 쿼리 8개 실행 중을 확인했습니다.

운영 전환 조건:

1. Kafka 재처리·중복 재전송 최대 지연과 감사 충돌 보존 기간을 별도 합의.
2. 감사 ID/해시를 영속 보존하거나 감사 기간을 분리해 49시간 이후 충돌도 검출.
3. 장기 Kafka backlog 처리 경로와 48h 밖 기록의 재분석 경로 마련.
4. 운영과 같은 파싱·event_date 변경·실제 `_write_silver`·outbox·Gold 스트리밍까지 섀도 실행. 이번 MERGE 시험은 단순화된 컬럼과 고정 event_date이며 원래 2,000개 ID의 고유 건수는 **Gold 비교의 대용 지표**일 뿐입니다. 실제 Gold 윈도우·지표·원격 대시보드 불변은 아직 검증하지 않았습니다.
5. 반복 부하·순서 교차 시험으로 상태 메모리·디스크·처리량·지연을 비교하고 체크포인트 전환·복귀 계획 수립.

다음 3단계에서는 168h를 유지하면서 Silver MERGE의 파일 읽기·날짜/ID 범위·배치 분할을 실제 실행 계획으로 계측하고 대량 데이터 병목을 먼저 줄이는 방향을 권고합니다. **3단계는 아직 시작하지 않았습니다.**

## 재현

파이프라인 폴더에서 실행합니다. 로컬 Spark 시험 이미지와 Delta JAR가 든 읽기 전용 Ivy 캐시가 필요합니다. 다른 PC에서는 이미지 태그·캐시 볼륨을 준비하고 이름을 맞춰야 합니다. 실제 업무 lake를 결과 폴더에 연결하면 안 됩니다.

```powershell
# 매 실행마다 새 출력 디렉터리. 기존 결과를 삭제하거나 덮어쓰지 않습니다.
$env:DEDUP_EXPERIMENT_OUTPUT='./results/dedup-stage2-new-run'
docker compose -f compose.dedup-experiment.yaml run --rm compare
# 실제 드라이버·Python 작업자 및 감사 함수 시험: 168과 48 각각 실행
docker compose -f compose.dedup-experiment.yaml run --rm -e DEDUP_TTL_HOURS=48 compare --master 'local[1]' --driver-memory 768m --conf spark.driver.host=127.0.0.1 --jars /ivy/jars/io.delta_delta-spark_2.12-3.2.1.jar,/ivy/jars/io.delta_delta-storage-3.2.1.jar --py-files /app/audit_state.py /tools/dedup_audit_probe.py
```

참고: [Spark 3.5 계열 dropDuplicatesWithinWatermark 공식 API](https://spark.apache.org/docs/3.5.9/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicatesWithinWatermark.html). 문서는 3.5 계열 의미를 설명하며 실제 검증 엔진 버전은 3.5.3입니다.
