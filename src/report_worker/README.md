# report_worker — 리포트 생성 SQS 워커 (05)

`report-job` SQS 메시지를 소비해 **LangGraph 멀티에이전트**로 손해사정 리포트 초안을 생성·저장하는 워커. LLM은 외부 GPU 노드(Ollama/vLLM)를 HTTP 호출하므로 **범용 노드**에 배포한다.

## 실행

```bash
uv run python -m report_worker          # report-job 컨슈머 기동
# uv 없이:  PYTHONPATH=src python -m report_worker
```
- 진입점 `__main__.py`: `configure_logging` → `init_pool` → `SqsConsumer(sqs_report_job_queue_url, ReportJob, handle_job).run()`.
- 소비→검증→ack(DeleteMessage)→실패=삭제 안 함(재전달)→poison 스킵·우아한 종료는 `core.sqs.SqsConsumer`가 담당.
- **큐 URL**: `SQS_REPORT_JOB_QUEUE_URL`(전체 URL)로 주입. 자격증명은 워커 IAM Role(로컬은 LocalStack + 더미 키). DLQ 미도입 — poison은 수신 횟수 상한(`SQS_MAX_RECEIVE_COUNT`)으로 스킵.

## 처리 흐름

1. `report-job` 소비 (`core.contracts.ReportJob`)
2. `worker.handle_job` — `ReportJob`을 그래프 초기 state로 매핑 → `build_graph().ainvoke(...)`
3. 리포트 초안을 `report_drafts`(JSONB, 영구 보존) + `reports`에 저장. **`report_id`로 멱등**(ON CONFLICT 업서트).
4. **하드 실패**(load_context/persist)만 예외로 승격 → 재전달(visibility timeout)·poison 스킵. 소프트 에러(rag_empty·input_blocked 등)는 부분결과로 커밋.

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
`core.sqs`·`core.db`·`core.ai_client`·`core.contracts`·`core.logging` · `guardrail` · RAG(현재는 로컬 `rag/hybrid`, 추후 `src/rag` 통합). `.[report]` extra(langgraph·langchain-core·boto3).

## dev 하네스 (배포 제외)
- `scripts/battery.py` — 검색 배터리 + e2e 시나리오 A~G. `PYTHONPATH=src python scripts/battery.py` (tempVectorDB + Ollama 필요).
- `scripts/trace_state.py` — 노드별 state 델타 추적.
