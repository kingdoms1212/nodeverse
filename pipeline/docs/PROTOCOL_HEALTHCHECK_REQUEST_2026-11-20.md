# Kafka·MongoDB 프로토콜 헬스체크 변경 요청

현재 포트 기반 헬스체크는 Kafka CLI의 JVM과 MongoDB 셸을 반복 실행하는 비용을 줄였습니다. 다음 변경에서는 **기존 Python 이미지에 설치된 클라이언트로 실제 프로토콜 응답을 확인**해 주세요. 브로커별 응답과 Kafka 수집 가능 여부를 구분하고, Spark와 원격 적재 작업자는 MongoDB가 명령에 응답한 뒤 기동하도록 보완해 주세요.

## 확인한 현재 상태

| 대상 | 현재 검사 | 실행 중인 로컬 스택에서 확인한 결과 |
|---|---|---|
| Kafka | 브로커는 `nc -z`로 TCP 연결 검사. `topic-init`은 Kafka CLI로 토픽을 만들고 조회. 수집기 `/health/ready`는 기존 Kafka 클라이언트로 메타데이터 조회. | 기존 `confluent-kafka`로 브로커 1·2·3에 개별 요청을 보내 각각 약 10.7ms, 9.6ms, 8.1ms에 응답을 받았습니다. `nv.learning.v2`의 6개 파티션 모두 리더가 있었고 최소 ISR은 3이었습니다. |
| MongoDB | Bash로 TCP 연결 검사. Spark와 원격 적재 작업자가 이 결과를 기동 조건으로 사용. | 기존 PyMongo `admin.command("ping")`이 약 4.4ms에 `ok: 1`을 반환했습니다. |

이 수치는 읽기 전용 단발 시험의 응답 시간입니다. 변경 후 실제 CPU 비용은 5분 유휴 측정으로 비교해 주세요. [Compose 생성기](../build_compose.py#L42) · [수집기 준비 확인](../collector.py#L157) · [설치된 클라이언트](../requirements.txt#L4)

## 변경 범위

### 1. Kafka는 기존 클라이언트로 브로커별 응답과 토픽 상태 확인

- 기존 `confluent-kafka`를 재사용하는 공통 검사 모듈을 두고, 대상 브로커를 개별 bootstrap 주소로 지정해 제한 시간 안에 메타데이터 응답을 받는지 확인해 주세요. 다른 브로커의 응답으로 대상 브로커가 정상으로 표시되지 않도록 응답 브로커도 확인해 주세요.
- 브로커별 응답, 필수 토픽·파티션의 리더 유무, 쓰기 정책에 필요한 ISR 상태를 구분해 표시해 주세요. 브로커 한 대가 다운돼도 필수 파티션의 리더와 최소 ISR을 충족하는 경우가 있으므로, 브로커 개별 장애와 클러스터의 수집 준비 상태를 같은 값으로 판단하지 않습니다. 메타데이터 상태는 실제 쓰기 성공을 확인하는 게임 전송 시험과 함께 검증해 주세요.
- 반복 검사에 JVM CLI를 사용하지 말아 주세요. 기동 시 `topic-init`까지 JVM CLI를 제거한다면 기존 `AdminClient.create_topics()`를 활용하고, 준비 지연 시 재시도와 이미 존재하는 토픽 처리, 생성 후 메타데이터 확인을 유지해 주세요. 초기화는 수집기 프로세스가 먼저 실행되기를 기다리지 않도록 구성해 주세요. [현재 토픽 초기화](../build_compose.py#L47) · [Confluent Kafka 클라이언트 문서](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)

### 2. MongoDB는 기존 PyMongo로 실제 `ping` 확인

- 기존 PyMongo로 `client.admin.command("ping")`을 실행하고, 제한 시간 내 `ok: 1` 응답을 받았을 때 준비된 것으로 처리해 주세요. Spark·원격 적재 작업자 기동 경로와 실행 중 점검에서 같은 검사 함수를 재사용할 수 있습니다. 포트 검사는 빠른 연결 진단에 활용할 수 있습니다.
- MongoDB 상태는 Kafka 수집 준비 상태와 독립적으로 판단해 주세요. MongoDB만 내려간 동안 Kafka가 정상이면 수집기는 계속 이벤트를 접수해야 합니다. `ping`은 명령 응답 여부를 보여 주므로 복구 이후 실제 outbox 처리 재개도 별도 확인해 주세요. [기동 의존성](../build_compose.py#L96) · [MongoDB 연결 코드](../common/db.py#L12) · [MongoDB `ping` 문서](https://www.mongodb.com/docs/manual/reference/command/ping/)

### 3. 원래 파이프라인 목표 유지

- 현재 토픽 이름과 파티션 수, 복제 계수 3, 최소 ISR 2, 보관 7일과 Kafka 접수 후 `202` 응답을 유지해 주세요. Spark의 시간 창·워터마크·체크포인트와 Delta Lake Bronze·Silver `MERGE`·Gold 처리 경로도 그대로 검증합니다. [토픽 설정](../topics.py) · [Spark 쿼리 구성](../spark/delta_stream.py#L61)
- Compose 구성을 수정하면 `build_compose.py`에서 관리하고 `compose.json`을 다시 생성해 주세요. [Compose 생성기](../build_compose.py)

## 변경 후 완료 기준

| 시험 | 기대 결과 |
|---|---|
| 브로커 또는 MongoDB 포트 연결은 되지만 프로토콜 요청이 무응답 | 정해진 시간 안에 실패 또는 준비 중으로 표시합니다. 의존 서비스는 실제 준비가 확인된 뒤 시작합니다. |
| Kafka 브로커 한 대 중지와 복구 | 해당 브로커 이상을 표시합니다. 필수 토픽의 리더와 ISR을 함께 기록하고, 수집 상태가 해당 조건에 맞게 표시되는지 확인합니다. |
| MongoDB만 중지한 뒤 복구 | Kafka가 정상인 동안 새 이벤트의 `202` 접수가 이어지고, MongoDB 복구 후 적재와 화면 반영이 재개됩니다. |
| 처음 기동과 기존 볼륨 재기동 | 필수 토픽이 기존 설정대로 생성되며, 기존 토픽·데이터·체크포인트를 보존한 채 다시 시작합니다. |
| 5분 유휴 CPU 비교와 종단 확인 | Kafka·MongoDB 검사 주기와 CPU 중앙값·95백분위·최댓값을 변경 전후로 비교합니다. 시험 이벤트 하나를 Kafka에서 Delta Lake와 서버 화면까지 추적합니다. |

지연 기동과 장애 시험은 별도 시험 스택에서 진행하고, 변경 파일과 측정 결과를 함께 남겨 주세요.
