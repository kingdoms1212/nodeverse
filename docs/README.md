# docs/ — 팀 공통 문서

- [SETUP_GUIDE.md](SETUP_GUIDE.md) — 통합 저장소 clone 후 파트별 개발환경 다시 구성하기
- [LOCAL_RUN_WINDOWS_ARM.md](LOCAL_RUN_WINDOWS_ARM.md) — 개발 PC 한 대(Windows ARM + WSL2)에서 세 파트 모두 실행하기
- [DATA_FLOW_AND_GAPS.md](DATA_FLOW_AND_GAPS.md) — 합친 코드 기준 데이터 흐름과 보완·추가 구현 목록 (우선순위별)
- [INTEGRATION_REVIEW_2026-11-20.md](INTEGRATION_REVIEW_2026-11-20.md) — 게임·파이프라인·서버 연동 종합 검토

## changes/ — 파트별 변경 구현서 (각 파트 AI 에이전트 학습용)

- [2026-11-20_game.md](changes/2026-11-20_game.md) — UUIDv7 이벤트 ID, 전송 큐·만료, 미등록 미니게임, player-login, 테스트 봇
- [2026-11-20_pipeline.md](changes/2026-11-20_pipeline.md) — 모듈 구조, 원격 적재 병렬화, Silver MERGE 조건, 측정 결과
- [2026-11-20_server.md](changes/2026-11-20_server.md) — 마이그레이션 순서, recovery_key 해시

## reports/ — 보고서

- `2026-11-20_NODEVERSE_변경리포트.md` — 공용 git 정리 시점 → 1차 보완 → 2차 보완(PL-9)까지의 파이프라인 상태와 그 밖의 모든 변경

- `NODEVERSE_중간보고서_v1.0.pdf` (2026-11-10)
- `NODEVERSE_델타레이크_구현과정_오류수정내역_v1.0.pdf` (2026-11-20)
- `NODEVERSE_로그인_클라우드세이브_구현정리.pdf` (2026-11-20)

## 파트별 문서

- 게임: [`game/docs/`](../game/docs/), [`game/README.md`](../game/README.md), `game/MINIGAME_GUIDE.md`, `game/MAP_EDIT_GUIDE.md`
- 파이프라인: [`pipeline/docs/`](../pipeline/docs/) — 설계 결정, 통합 현황, E2E 시험 보고서, 델타레이크 요구사항 검토 보고서, 구현 결과서(`TEAM_AI_HANDOFF_2026-11-20.pdf`), 구현계획서 v1.2·v1.3
- 서버: [`server/docs/`](../server/docs/) — DB 구조, MongoDB 연동, 게임 로그인 연동, 팀 인수인계
- 공유 이벤트 형식: [`contracts/`](../contracts/)
