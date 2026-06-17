# report_worker — LangGraph 리포트 워커 (05)

`report-job` Kafka 메시지를 소비해 **LangGraph 멀티에이전트**로 리포트 초안을 생성하는 워커. LLM은 외부 GPU 노드(Ollama/vLLM)를 HTTP 호출하므로 **범용 노드**에 배포한다.

## 처리 흐름

1. `report-job` 소비 (`ReportJob`)
2. **입력 가드레일** → **LangGraph 멀티에이전트 생성**(생성 가드레일) → **출력 가드레일(LLM Judge 포함)**
3. AI 리포트 초안 저장 (JSONB, 영구 보존 — 손해사정사 검수 근거)

## 입력 / 출력 (계약)

- **입력**: `core.contracts.ReportJob` (consume `report-job`)
- **출력**: 리포트 초안 DB 저장

## 의존

- `core.kafka`·`core.db`·`core.ai_client`(LLM, **모델 미정**) · `rag`(검색) · `guardrail`(3단계)
- `.[report]` extra (langgraph·langchain-core)

## ⚠️ 설계 필요

[Notion 05](../../.claude/docs/05_langGraphAgent.md)가 **미작성** 상태다. LangGraph 그래프 구성(에이전트 역할·엣지·상태)을 먼저 설계·합의한 뒤 구현한다.

## 참고

- 배포: `src/report_worker/Dockerfile` (slim) → 범용 노드 · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
