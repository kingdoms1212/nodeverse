# 1단계 완료 보고: 관측 지표 보강

기준일 2026-11-23 / 공용 저장소 feature/codex-pipeline-observatory. 다음 단계(48h 전환)는 시작하지 않았습니다.

## 완료한 것

1. Delta MERGE 커밋의 실행·스캔 단계·재작성 시간, 입력/삽입 행 수, 표본 시각·커밋 버전을 읽습니다. Parquet나 Delta 테이블을 Spark로 스캔하지 않습니다. 시작 시 체크포인트 부근 메타데이터만 확인하고 이후 새 버전 JSON만 증분 읽습니다(회당 최대 128개, 파일별 읽기 상한). 쓰기 바이트를 읽기 바이트로 바꾸어 표시하지 않습니다.
2. Kafka 브로커 최신 오프셋에서 Bronze 최근 완료 배치의 endOffset을 뺀 미처리 건수를 추정합니다. consumer group commit이나 이벤트 소비는 하지 않습니다. 오프셋 초기화/누락은 0이 아닌 미확인입니다. 일반 Kafka consumer-group lag와 구분해야 하며, 최근 진행 파일·30초 진단 캐시에 따른 관측 지연이 있습니다.
3. 고유한 stored 이벤트 최대 1,000개·최근 15분을 대상으로 수집 접수→적재 완료, outbox 생성→적재 완료의 p50/p95/최대를 계산합니다. scope별로 나누며 quarantine·미완료를 성공 지연에 섞지 않습니다. p95는 nearest-rank 방식입니다. 게임 발생·화면 표시까지의 종단 지연은 아닙니다.
4. 대기열에 관측용 복합 인덱스 `observatory_latency_v1(state, completed_at DESC, _id)` 1개를 명시적으로 추가했습니다. 인덱스가 없으면 해당 지연 집계는 하지 않습니다. 관측 API나 샘플러가 자동으로 업무 인덱스를 만들지는 않습니다.
5. Spark 프로세스 환경과 검토한 코드 기본값으로 현재 TTL을 확인합니다. 코드 SHA-256·Spark application_id·확인 시각을 비공개 운영 파일에 기록하고, 앱 ID가 바뀌면 값의 신뢰를 해제합니다. 새 Spark의 진행 파일이 제공하는 값이 있으면 그것을 우선 사용합니다.
6. 대시보드에 병목 계측 패널, scope별 지연 선택, 상태 추정 메모리 추이를 추가했습니다. 실제 측정값·메타데이터 시각·미수집 항목을 구분합니다.

## 실측 검증

- 실제 운영 TTL: **168시간**. 설정·체크포인트를 변경하지 않았습니다.
- Kafka→Bronze: **12개 파티션**, 시험 후 미처리 추정 **0건** 확인.
- 최신 학습 MERGE: **v28 / 2026-11-20 15:20:49 KST**, 실행 782ms, 엔진 보고 scanTimeMs 0, rewriteTimeMs 777. 이것은 과거 입력 배치의 커밋 표본이며 지금 부하 성능이 아닙니다. 실제 스캔 파일 수·바이트는 이 엔진 기록에 없어 미수집입니다. scanTimeMs=0도 실제 읽기 바이트 0의 증명이 아닙니다.
- 이 운영 커밋의 predicate에는 날짜 제한이 없었습니다. 공용 main의 개선된 MERGE와 운영 중인 11/20 코드가 다른 근거이며, 실제 풀스캔을 직접 측정해 확정한 결과는 아닙니다.
- `observatory-metrics-probe` 이름의 런타임 heartbeat **3개**를 공개 8000 경로로 전송하고, 모두 stored 및 원격 Mongo pipeline_runtime_events의 동일 ID 저장을 확인했습니다. 회원·플레이 문서는 생성하지 않았습니다.
- 해당 3개 표본: **수집 접수→적재 완료 p50 7.595초 / p95 8.408초**, **outbox→적재 완료 p50 0.424초 / p95 0.458초**. 소량 기능 검증값으로, 부하 성능이나 학습 기록의 p95로 일반화하지 않습니다.
- 지연 조회 실행 계획: 반환 3개 / 문서 검사 3개 / 인덱스 키 검사 3개 / **COLLSCAN 없음**.
- 전체 검사 **102 passed, 1 skipped**. 기존 PySpark 필요한 MERGE 검사는 이 Windows venv에 PySpark가 없어 건너뛰었습니다. 메타데이터 읽기·오프셋 차이·중복 표본·시각 오류·앱 변경 시 설정 무효화는 새 단위 검사로 확인했습니다.

## 실행·조회

주소: http://127.0.0.1:8011/ . 병목 계측 패널에서 ‘런타임 원본’을 선택하면 이번 시험 표본을 볼 수 있습니다. 15분 창을 지나면 해당 표본은 지연 통계에서 빠지며 오류가 아닙니다.

```powershell
# 파이프라인 폴더, Docker 접근이 가능한 운영자 환경
python -m tools.capture_observatory_runtime --container <실제Spark컨테이너> --output results/observatory/runtime.json
# 관측 서비스 이미지에서 명시적으로 인덱스 준비. --execute가 없으면 계획만 출력
# 기존 인덱스/실행 대상 DB를 먼저 확인할 것
# docker exec <관측소컨테이너> python -m observatory.setup_indexes --execute
docker compose -f compose.observatory.attach.yaml up -d --build
```

새 설정: OBSERVATORY_SPARK_UI_URL, OBSERVATORY_DEPLOYMENT_PATH, attached compose의 OBSERVATORY_KAFKA_BOOTSTRAP. 프로파일에서 관련 메타데이터와 lake는 읽기 전용 마운트입니다. runtime.json에는 키·비밀번호를 넣지 않으며 Git에서 제외합니다. application_id가 바뀌는 Spark 재기동 후에는 운영 설정 확인 파일을 다시 생성해야 합니다.

## 경계·남은 제한

- 기존 수집기·Kafka·Spark·적재 작업자를 재시작하거나 로직을 바꾸지 않았습니다. 관측 서비스만 갱신했습니다. 관측 인덱스와 명시한 시험 원본 3개, 자체 관측 시계열만 추가했습니다.
- 드라이버 내부의 정확한 outbox 쓰기 구간은 기존 Spark에 계측 코드가 없어 아직 미수집입니다. 새 공용 Spark 코드에 계측은 준비돼 있으며 차후 검증된 재배포 시 표시됩니다. outbox 대기열→완료 지연과 혼동하지 않습니다.
- 실제 MERGE 파일/바이트 스캔량, 전체 종단 지연은 여전히 미수집입니다. 다음 Silver 성능 비교에서 별도 실행 계획·엔진 계측으로 확인해야 합니다.
- 화면 경고와 외부 메신저 발송은 다릅니다. 외부 알림·자동 복구는 4단계 과제로 남깁니다.

## 다음 시작 예정: 2단계 168h / 48h 비교 시험

실행 전에 운영 소스와 공용 소스의 차이를 확정합니다. 별도 시험 스택에서 같은 데이터·자원으로 비교하고, DEDUP_TTL_HOURS의 Compose 전달과 드라이버/감사 작업자 실제 반영을 확인합니다. 기존 체크포인트 재개, 48시간 이후 재전송, 충돌 감지 범위, Gold 집계 불변을 검증한 뒤 적용 여부를 결정합니다. 아직 운영 TTL을 48로 바꾸지 않았습니다.

근거 파일(로컬, Git 제외): results/observatory/runtime.json, probe-20261123T012749.json, step1-explain.json, step1-live-verification.json.
