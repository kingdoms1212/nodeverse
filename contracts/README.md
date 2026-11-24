# contracts/ — 세 파트가 공유하는 약속

게임이 보내고, 파이프라인이 받아 검증하고, 서버가 읽는 **이벤트 형식과 Kafka 토픽**을 한곳에 둡니다.
이 폴더를 바꾸는 PR은 게임·파이프라인·서버 담당자 모두의 승인을 받습니다.

| 파일 | 내용 | 출처 (2026-11-20 복사) |
|---|---|---|
| `topics.py` | Kafka 토픽 이름·파티션 수·보존 기간 | `pipeline/topics.py` |
| `contract.py` | 이벤트 모델(pydantic): `LearningEventV2`, 런처 이벤트 v1, 미니게임 카탈로그, 토픽 라우팅 | `pipeline/contract.py` |
| `json-schema/behavior-event.schema.json` | 서버 쪽이 정리한 학습 이벤트 v2 JSON Schema | `server/docs/contracts/` |
| `json-schema/behavior-event.example.json` | 위 스키마의 예시 | `server/docs/contracts/` |

## 현재 상태: 아직 복사본입니다

지금은 파이프라인 코드가 여전히 `pipeline/contract.py`, `pipeline/topics.py`를 직접 읽습니다.
이 폴더를 실제 단일 기준으로 만들려면 다음 중 하나를 정해야 합니다.

1. 파이프라인이 `contracts/`를 import 하도록 바꾸고 `pipeline/` 쪽 사본을 지운다 (Docker 빌드 컨텍스트 조정 필요).
2. `contracts/`를 기준으로 두고, CI에서 `pipeline/` 사본과 내용이 같은지 검사한다.

## ⚠️ 맞춰야 할 차이 (2026-11-20 기준)

파이프라인의 `LearningEventV2`와 서버의 JSON Schema가 서로 다릅니다.

- `schema_version`: 파이프라인은 정수 `2`, 서버 스키마는 문자열 `"2.0"`
- 파이프라인에만 있는 필드: `player_id`, `minigame_id`, `category`, `build_version`, `content_version`, `details`
- 서버 스키마에만 있는 필드: `payload`, `operation`, `state_before`, `state_after`, `hint_id`, `hint_step`, `failure_reason`
  (파이프라인은 이 중 일부를 하위 객체 안에 둡니다)
- 서버 스키마 제목에 "ingestion endpoint not implemented"라고 되어 있어, 서버 쪽 스키마가 더 이전 초안일 가능성이 높습니다.

어느 쪽을 기준으로 삼을지 세 파트가 정한 뒤, 이 README의 이 절을 지우세요.
