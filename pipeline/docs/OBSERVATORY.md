# 실시간 파이프라인 관측소

2026-11-23 / `feature/codex-pipeline-observatory` / 공용 저장소 `main d4693de` 기반.

## 무엇을 볼 수 있나

- 수집기 준비 응답, Bronze/Silver/원격 Mongo 연결 상태
- 쿼리별 입력·처리 속도, 최근 배치 시간, 상태 키·추정 메모리
- 적재 대기·처리 중·재시도·조건 대기·parked·blocked·격리·완료 건수
- 처리 대기와 조건 대기의 최고 나이
- 15분/1시간/6시간 추이, 새로고침·화면 일시 정지
- 화면 내 경고·복구 이력 및 중복 억제

현재 사용 주소: **http://127.0.0.1:8011/**. 이 PC에서 보는 로컬 주소입니다. 기존 `8010/incoming` 또는 팀원 서버의 `staff/live`를 대체·변경하지 않습니다.

## 구조·부하

`Spark 진행 JSON(read-only) + 로컬 Mongo 요약/상태 + 준비 응답 + 원격 Mongo ping` → 약 10초 간격 샘플러 → 메모리 스냅샷 → HTTP API → 브라우저.

- 요청마다 Spark·Delta를 읽지 않고 백그라운드 스냅샷을 반환합니다. SparkSession을 새로 만들거나 Delta 파일을 스캔하지 않습니다.
- Mongo의 신선한 `pipeline_sink_status`를 우선 사용하고, 없거나 30초 넘으면 state 인덱스 기반 count로 전환합니다. 최고 대기 조회는 1초 maxTimeMS, DB 연결은 관측 서비스만 짧게 제한합니다. 대규모에서는 작업자 요약을 사용하는 것이 권장됩니다.
- 수집기 준비 조회는 30초마다, Mongo 응답은 샘플 간격마다 확인합니다. 원격 Mongo ping은 이벤트별 저장 증명이 아닙니다.
- 기존 lake 볼륨은 읽기 전용으로 마운트합니다. 업무 문서·outbox·회원·체크포인트를 수정하는 API는 없습니다.
- 관측 이력만 로컬 Mongo `observatory_samples`에 저장합니다. 48시간 TTL, 화면은 최근 6시간까지이며 약 2,160표본을 메모리에 유지합니다. 재시작 시 최근 이력을 읽습니다. 경고 이력은 프로세스 메모리 100건이며 재시작 때 초기화됩니다.

## 실행

### 현재 PC의 기존 live 스택에 붙이기

```powershell
cd <공용 저장소>/pipeline
docker compose -f compose.observatory.attach.yaml up -d --build
```

이 파일은 **관측 서비스만** 시작합니다. 기본 연결은 `nodeverse-v2-live_default` 네트워크와 `nodeverse-v2-live_lake` 볼륨이며, 외부 리소스가 없으면 생성하지 않고 실패합니다. 실제 프로젝트에 맞춰 다음 설정을 `.env`에서 지정할 수 있습니다.

- `OBSERVATORY_NETWORK_NAME`, `OBSERVATORY_LAKE_VOLUME`
- `OBSERVATORY_MONGO_URI`, `OBSERVATORY_MONGO_DB`, `OBSERVATORY_COLLECTOR_URL`
- 원격 Mongo ping을 사용하려면 기존 비공개 `.env.remote`에 `DIRECT_MONGO_URI`, `DIRECT_MONGO_DATABASE`를 준비합니다. 없어도 로컬 지표는 동작하며 원격은 미설정으로 표시됩니다.

### 공용 compose의 기존 프로젝트에서 실행

```powershell
python build_compose.py
# <기존프로젝트>는 실제 가동 중인 compose 프로젝트 이름으로 바꿉니다.
docker compose -p <기존프로젝트> --env-file .env -f compose.json --profile observatory up -d --no-deps --build observatory
```

기본값은 localhost 8011입니다. 다른 PC 접속이 필요할 때만 LAN 바인딩과 별도 `OBSERVATORY_KEY`를 비공개 설정으로 지정합니다. 비 loopback 바인딩에 키가 없으면 앱이 시작되지 않습니다. 키는 브라우저 메모리에만 유지하며 새로고침 시 다시 입력합니다. 키·URI·쿠키를 Git에 넣지 않습니다. 이 화면에는 원본이나 회원 정보를 노출하지 않습니다.

## 경고 규칙(MVP 고정 기준)

| 신호 | 표시 |
|---|---|
| 진행 파일 없음/30초 초과 지연, 로컬 DB·수집기·원격 DB 응답 실패, 비활성 쿼리 | 즉시 오류 |
| 대기 데이터가 있는데 배치 완료가 60초 넘게 오래됨 | 연속 2표본 후 오류 |
| 입력 있는 최근 60초 내 배치가 5초 초과 | 연속 3표본 후 주의 |
| 처리 가능 대기열 최고 나이 60초 초과 | 연속 3표본 후 주의 |
| 조건 대기 최고 나이 10분 초과 | 연속 2표본 후 주의 |
| blocked / parked 존재 | 오류 / 주의 |

같은 신호는 하나의 활성 경고로 갱신하고 해소 시 복구 이력을 남깁니다. 무입력과 오래된 완료 배치를 장애로 단정하지 않습니다. 외부 메신저/메일 발송, 자동 재처리, 자동 설정 변경은 이번 범위가 아닙니다. 누적 실패·재시작 수의 증가율 기반 알림, 알림 정책 설정화는 후속 과제입니다.

## 계측과 한계

`common/observability.py`는 드라이버에서 이미 측정하는 구간을 기록합니다. `spark/silver.py`는 입력 있는 성공 배치에만 정제·MERGE 구간, outbox 게시 구간, 정비 구간 시간을 남기고 `spark/periodic.py`가 진행 JSON에 포함합니다. 추가 Spark count나 쿼리는 실행하지 않습니다. 정제·MERGE 구간에는 그룹 탐색·경계 집계가 포함되고 정비 시간에는 캐시 해제가 포함됩니다.

현재 PC의 가동 Spark는 11/20 코드이므로 **새 계측을 배포하기 전까지** 세부 MERGE/outbox 시간, 실제 중복 제거 설정, 누적 재시작은 미수집/미확인입니다. 기존 Spark는 이번 작업에서 재시작·수정하지 않았습니다. 기본 배치·상태 지표는 기존 JSON만으로 표시됩니다. 새 공용 코드를 Spark에 배포하면 추가 필드가 나타납니다.

- 전체 전달 지연 p95, 실제 MERGE 스캔 바이트·파일 수, Kafka consumer lag는 아직 수집하지 않습니다. 0으로 보이거나 가짜 측정값을 넣지 않습니다.
- inputRowsPerSecond와 processedRowsPerSecond는 쿼리별 최근 배치 지표이며 동일 이벤트의 종단 전달 속도나 지연이 아닙니다.
- 갱신 간격은 샘플 작업 시간 + 설정 대기 시간입니다. DB 장애 시 실제 간격이 늘 수 있습니다.
- `DEDUP_TTL_HOURS`는 이번 작업에서 변경하지 않았습니다. 48h 비교 시험과 Compose 전달 수정은 별도 작업입니다.
- 현재 대기열의 기존 19건을 재처리·격리하거나 DB 데이터를 추가 생성하지 않았습니다.

## 검증 결과

- 전체 pytest **94 passed, 1 skipped**. skip은 PySpark가 필요한 기존 Silver MERGE 검사입니다. 새 관측 검사는 12개입니다.
- 무입력·unknown·비밀값 비노출·알림 중복/복구·키 보호·기간 제한·재시작 이력·단계 레지스트리·외부 바인딩 키 강제를 검사했습니다.
- 실제 local live 연결: 쿼리 8개, 조건 대기 19, 격리 28, 완료 166 건 확인. 샘플 시점에 재시도 처리 중 1건/대기 18건으로 잠깐 바뀔 수 있습니다. 새 데이터 없는 상태의 수신 속도 0을 정상 대기로 표시했습니다.
- 원격 Mongo·로컬 Mongo·수집기 준비 응답을 확인했습니다. 기존 수집·적재 서비스 컨테이너를 재생성하지 않았습니다.
- 브라우저에서 차트 기간 선택·일시 정지/재개·새로고침과 실제 수치를 점검했습니다. 관측 이력은 실제 관측 이후부터 쌓이며 데모 데이터가 아닙니다.

주요 파일: `observatory/app.py`, `sampler.py`, `static/`, `Dockerfile.observatory`, `compose.observatory.attach.yaml`. 공용 generator의 `observatory` 프로필과 설정 예제도 포함했습니다.

## 1단계 계측 보강(후속)

현재 추가된 MERGE 커밋 시간·Kafka→Bronze 미처리 추정·scope별 p50/p95·운영 TTL 확인·인덱스 준비와 실측은 [OBSERVATORY_STEP1_2026-11-23.md](OBSERVATORY_STEP1_2026-11-23.md)를 우선 참고하세요. 위 초기 MVP의 Kafka lag 미수집 설명은 이 후속 변경으로 대체됩니다. 전체 종단 지연과 실제 스캔 바이트는 여전히 별도 계측이 필요합니다.


## 내부망 접속과 최근 수신 목록

`compose.observatory.attach.yaml`은 기본적으로 localhost에만 연결합니다. 팀 내부망에서 사용하려면 Git에 포함되지 않는 설정 파일에 `OBSERVATORY_BIND=<서버의 내부 IP>`, `OBSERVATORY_PORT=8011`, 길고 임의의 `OBSERVATORY_KEY`를 설정하고 `docker compose --env-file <설정파일> -f compose.observatory.attach.yaml up -d --build`로 관측소를 시작합니다. Windows 방화벽은 해당 TCP 포트를 신뢰하는 내부망으로만 제한합니다. 키는 별도 안전한 경로로 전달합니다. HTTP는 전송을 암호화하지 않으므로 신뢰할 수 없는 네트워크에서는 HTTPS 등을 사용해야 합니다. 브라우저 알림은 HTTP 환경에서 제한될 수 있습니다.

최근 목록은 원격 outbox에 도달한 항목 중 대기열 진입 시각으로 최대 20건을 표시합니다. 동일 배치 시각은 임의 순서이며 수신 순서가 아닙니다. 거부된 요청, 아직 outbox에 도착하지 않은 이벤트는 빠집니다. 참조는 이벤트 ID의 SHA-256 앞 12자리입니다. 원격 원본 저장은 대시보드 반영을 뜻하지 않습니다. 읽기 부담을 제한하기 위해 `python -m observatory.setup_indexes --execute`로 `observatory_recent_v1` 인덱스를 명시적으로 준비해야 합니다. 인덱스가 없으면 목록을 조회하지 않습니다.
