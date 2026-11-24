# Silver MERGE 파일 배치 개선 검증 요청

Silver 학습 테이블의 **날짜 파티션 안에서 `event_id` 단독 ZORDER가 `MERGE`의 파일 읽기와 처리 시간을 줄이는지** 확인해 주세요. 기존에는 `minigame_id`, `player_id`, `event_id`로 ZORDER하고, `MERGE`는 날짜 범위와 배치 UUID 범위로 대상 파일을 좁힙니다. 이번 요청은 같은 데이터로 파일 배치를 비교한 뒤 적용 여부를 판단하는 작업입니다. [기존 ZORDER 설정](../spark/maintain.py#L9) · [기존 MERGE 조건](../spark/silver.py#L52)

## 비교 조건

| 구분 | 비교할 내용 |
|---|---|
| A. 현행 기준 | 날짜 파티션과 `ZORDER BY (minigame_id, player_id, event_id)`를 사용합니다. |
| B. 개선 후보 | 같은 날짜 파티션에서 `ZORDER BY (event_id)`를 시험합니다. 후보 SQL은 `OPTIMIZE silver.learning_events WHERE event_date = DATE '2026-11-20' ZORDER BY (event_id)`입니다. 날짜는 시험 데이터에 맞게 지정합니다. |
| 초기 데이터 | 같은 Silver 스냅샷을 복제해 A와 B를 구성합니다. 날짜당 파일이 여러 개 남을 만큼 현실적인 데이터 규모를 사용합니다. UUIDv4·v7이 섞인 경우와 UUIDv7 위주인 경우를 각각 같은 데이터끼리 비교합니다. |
| 입력과 실행 환경 | 두 후보에 같은 신규·재전송 이벤트 배치를 사용합니다. Spark 자원과 설정, 대상 날짜 범위, 캐시 조건을 맞추고 실행 시간을 반복 측정합니다. 쓰기 시험마다 동일한 초기 상태에서 시작합니다. |
| 기존 조회 성능 | 미니게임과 플레이어 필터가 붙은 대표 Silver 조회도 비교해 주세요. `event_id`만 기준으로 파일을 배치하면 이 조회의 읽기 비용이 달라질 수 있습니다. |

앞선 UUIDv4·v7 시험은 초기 Silver 테이블 크기가 달랐고, 기존 `OPTIMIZE ZORDER` 시험에서는 테이블별 파일 수가 `1 → 1`이었습니다. 두 결과를 기준으로 성능 향상을 판정하기는 어렵습니다. [기존 MERGE 측정](../../docs/changes/2026-11-20_pipeline.md#L103) · [기존 정비 결과](TECHNICAL_REVIEW.md#L130) · [Delta Lake 파일 건너뛰기와 ZORDER](https://docs.delta.io/optimizations-oss/)

## 유지할 처리 조건

- **Bronze 원본 append → Silver insert-only `MERGE` → Gold 집계** 경로를 유지합니다. Silver의 일치 키는 전체 `event_id`이며, 기존 날짜 검색 범위, UUID 범위, 워터마크, 체크포인트를 그대로 사용해 파일 배치 효과를 비교합니다. [Silver MERGE](../spark/silver.py#L52) · [Silver 스트림](../spark/silver.py#L98)
- 날짜가 바뀐 뒤 같은 ID가 다시 처리되는 상황을 검증합니다. 워터마크가 재전송을 먼저 제거할 수 있으므로, **별도 Delta 시험 테이블에서 재전송 행을 실제 `MERGE` 입력까지 보내는 시험**을 포함합니다. 이전 날짜의 원본을 찾고, Silver에 고유 행 1건과 기존 본문이 유지되는지 확인합니다.
- 파일 정비 전후 행 수와 고유 ID 수를 대조하고, 정비 후 기존 시험 체크포인트로 스트림이 이어지는지 확인합니다. 이 비교는 별도 시험 테이블과 볼륨에서 수행합니다. [기존 정비 도구](../spark/maintain.py#L16) · [체크포인트 복구 검증](TECHNICAL_REVIEW.md#L143)

## 제출 결과와 적용 판단

| 항목 | 기록할 값 |
|---|---|
| 파일 읽기 | 대상 날짜별 파일 수, 가능한 경우 Delta 통계나 Spark SQL 작업 화면에서 실제 스캔한 파일 수·바이트 |
| 처리 시간 | `MERGE` 실행 시간과 배치 전체 시간의 반복 측정 결과, `OPTIMIZE` 실행 시간과 파일 재작성 비용 |
| 영향 | 미니게임·플레이어 조회 시간, 날짜를 넘긴 재전송 결과, Gold 집계와 스트림 재개 여부 |

`event_id` 단독 ZORDER가 반복해서 스캔량과 `MERGE` 시간을 줄이고, 기존 조회와 복구 동작이 허용 범위에 있으면 적용을 검토해 주세요. 개선 폭이 작거나 정비 비용이 크면 현행 설정을 유지하고 측정 결과를 남겨 주세요. 적용할 경우 기존 `maintain.py`에서 시험한 설정을 명시적으로 선택할 수 있게 하고 운영 기본값 변경 여부와 근거를 함께 정리해 주세요.
