---
name: langgraph-agent-build
description: LLM 소비 컴포넌트를 구현할 때 사용. LangGraph 멀티에이전트 리포트 생성(05번, SQS 워커)과 챗봇(12번, FastAPI WebSocket 직결·비스트리밍)을 다룬다. RAG·가드레일 공용 모듈과 ai_client(Ollama Qwen3 MoE)를 조립한다. LangGraph 그래프·멀티에이전트·리포트 생성·챗봇 WebSocket·세션 작업 시 사용.
---

# LangGraph / Agent Build

RAG·가드레일·ai_client를 **조립**해 사용자 가치를 만드는 두 컴포넌트. 공용 모듈을 재구현하지 않고 `aicore-engineer`의 공개 API를 호출한다.

## A. 리포트 생성 (05번) — SQS 워커
`src/report_worker/` (파이프라인 + 진입점 `__main__.py`).

- 진입: `report-job` SQS 소비(`sqs-worker-patterns` 따름).
- 파이프라인: **입력 가드레일 → LangGraph 멀티에이전트 생성(생성 가드레일 적용) → 출력 가드레일(LLM Judge 포함)**.
- `state.claim_id`가 있으면 `load_context`가 클레임의 문서 전체(`ocr_results`)를 문서 경계 헤더로 병합해 `masked_text`/`entities`를 구성한다(`_merge_claim_texts`/`_merge_claim_entities`) — 대표 문서 1개만 읽지 않는다. 엔티티 병합은 뒤 문서의 추출 실패(`None`)가 앞 문서의 성공값을 지우지 않도록 `None`을 건너뛴다.
- LLM은 `ai_client`(Qwen3 MoE, 별도 GPU 노드). EXAONE은 라이선스(상업 사용 금지)로 제외. `ai_client.chat()`은 `num_ctx`를 명시하지 않고 서버 기본값에 맡긴다 — 문서를 아주 많이 병합하는 등 프롬프트가 커지는 변경을 할 때는 실측(서버 `num_ctx`, 실제 토큰 소비량)으로 잘림 여부를 확인한다(VLM 표 전사가 #60에서 이 문제로 `vlm_num_ctx`를 명시 고정한 선례가 있다).
- `pii_dek_unavailable`·`pii_decrypt_failed`·가드레일 입력 차단 시 `reports.status='BLOCKED'`로 종결한다(`persist_blocked`).
- 결과: AI 리포트 초안 JSONB로 **영구 보존**(손해사정사 검수 근거).
- 그래프 노드 구성(에이전트 역할·엣지·상태)은 `.claude/docs/05_langGraphAgent.md`(Notion 05번 동기화본)와 `src/report_worker/state.py`(`ReportState`) 기준. 구조를 바꿀 때는 먼저 `_workspace/`에 설계해 확인받는다.

LangGraph 원칙:
- 상태(State)를 명시적 타입으로 정의(`ReportState` TypedDict). 노드는 단일 책임, `safe_node` 데코레이터로 부분 실패 격리.
- 노드 실패는 부분 결과 + `errors`에 실패 사유 표기로 진행(전체 실패 회피).
- 각 사실 주장에 인용을 달도록 생성 가드레일과 맞물린다.

## B. 챗봇 (12번) — FastAPI WebSocket 직결, 비스트리밍
`src/chatbot/` — 순수 로직 + FastAPI 진입점 `app.py`(WS·세션).

- ALB(/ws/chat)를 통해 FastAPI가 **WebSocket 직접 수락**. on-connect **JWT(RS256) 스테이트리스 검증**. 동일 session_id 중복 연결 시 기존 해제.
- **Redis**로 다중 Pod 세션 상태·멀티턴 컨텍스트(이전 N턴 요약) 공유, 24h 만료.
- 처리 순서: 입력 가드레일 → RAG 검색(멀티턴 컨텍스트 구성) → `ai_client`(Qwen3 MoE) **완성 응답 생성** → 출력 가드레일.
- **비스트리밍**: 완성 응답을 `ChatServerMessage(type="message", citations=...)`로 **1회 전달**. 토큰 청크·`stream`/`done` 신호를 만들지 않는다.
- 세션 생성·종료는 REST, 대화 이력 PG 저장(90일, 윈도우 초과 시 오래된 턴 요약·압축).

구현 원칙:
- WebSocket·세션·JWT는 `src/chatbot/app.py`, 순수 처리(가드레일·RAG·LLM 조립)는 `src/chatbot/`의 별도 모듈로 분리 — 로직 단위 테스트를 위해.
- 멀티 Pod 가정: 세션 상태를 인메모리에 두지 않는다(Redis 공유). 재연결이 다른 Pod로 가도 컨텍스트 복구.
- LLM/RAG 실패 시 안전 폴백 메시지 + 고지문 반환, 연결 유지.

## 검증
- 리포트: 그래프 end-to-end, 가드레일 3단계 적용, 인용 검증 동작.
- 챗봇: WS 연결·JWT 거부·중복연결 해제, Redis 세션 복구, 완성 응답 1회 전달(스트리밍 없음 확인), 가드레일 적용.

## 산출물
`src/report_worker/*`, `src/chatbot/*`(`app.py` 포함) + 테스트. 요약을 `_workspace/03_agent.md`에 기록.
