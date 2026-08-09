# report_worker — 리포트 생성 Kafka 워커 (05)

`report-job` Kafka 메시지를 소비해 **LangGraph 멀티에이전트**로 손해사정 리포트 초안을 생성·저장하는 워커. LLM은 외부 GPU 노드(Ollama/vLLM)를 HTTP 호출하므로 **범용 노드**에 배포한다.

## 실행

```bash
uv run python -m report_worker          # report-job 컨슈머 기동
# uv 없이:  PYTHONPATH=src python -m report_worker
```
- 진입점 `__main__.py`: `configure_logging` → `init_pool` → `KafkaConsumer(report-job, ReportJob, handle_job).run()`.
- 소비→검증→재시도→DLQ(`report-job.dlq`)→오프셋 커밋·우아한 종료는 `core.kafka.KafkaConsumer`가 담당.
- **컨슈머 그룹**: settings 기본값 `ocr-worker` → `KAFKA_CONSUMER_GROUP=report-worker`로 오버라이드(Dockerfile ENV에 설정됨).

## 처리 흐름

1. `report-job` 소비 (`core.contracts.ReportJob`)
2. `worker.handle_job` — `ReportJob`을 그래프 초기 state로 매핑 → `build_graph().ainvoke(...)`
3. 리포트 초안을 `report_drafts`(JSONB, 영구 보존) + `reports`에 저장. **`report_id`로 멱등**(ON CONFLICT 업서트).
4. **하드 실패**(load_context/persist)만 예외로 승격 → 재시도/DLQ. 소프트 에러(rag_empty·input_blocked 등)는 부분결과로 커밋.

## 그래프 (순차 + 조건 분기)

```
load_context → input_guardrail ─[차단?]→ END
 → diagnosis ─[약관 DB 有無]→ terms_parse / coverage_parse
 → coverage_analysis → case_search
 ─[후유장해 검토?]→ disability_rag → disability_calc ─┐
 └────────(아니면)────────────────────────────────────┴→ payment_calc
 → report_compose → output_guardrail(고지문 + LLM Judge) → persist → END
```
- 노드: `nodes/agents.py` · 조립: `graph.py` · 상태: `state.py` · 장해 합산(순수): `disability_rules.py`.
- RAG: `rag/hybrid.py`(BM25+pgvector+RRF, insurer/product 필터) · 가드레일: `guardrail`(공용) · LLM: `core.ai_client`.

## 의존
`core.kafka`·`core.db`·`core.ai_client`·`core.contracts`·`core.logging` · `guardrail` · RAG(현재는 로컬 `rag/hybrid`, 추후 `src/rag` 통합). `.[report]` extra(langgraph·langchain-core).

## dev 하네스 (배포 제외)
- `scripts/battery.py` — 검색 배터리 + e2e 시나리오 A~G. `PYTHONPATH=src python scripts/battery.py` (tempVectorDB + Ollama 필요).
- `scripts/trace_state.py` — 노드별 state 델타 추적.
