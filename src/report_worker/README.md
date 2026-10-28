# report_worker — 리포트 생성 SQS 워커 (05)

`report-job` SQS 메시지를 소비해 **LangGraph 멀티에이전트**로 손해사정 리포트 초안을 만들고 저장하는 워커다. LLM은 외부 GPU 노드(Ollama/vLLM)를 HTTP로 호출하므로 **범용 노드**에 배포한다.

## 실행

```bash
uv sync --extra report               # langgraph·langchain-core·boto3
uv run python -m report_worker       # uv 없이: PYTHONPATH=src python -m report_worker
```

- 진입점 `__main__.py`: `configure_logging` → `init_pool` → `SqsConsumer(SQS_REPORT_JOB_QUEUE_URL, ReportJob, handle_job).run()`
- 소비·검증·ack·재전달·poison 스킵·우아한 종료는 `core.sqs.SqsConsumer`가 맡는다.
- 이 워커는 **마이그레이션을 실행하지 않는다**. `ai.*`는 `ocr_worker`가, `core.*`(`reports` 등)는 backend Flyway가 관리한다.

## 처리 흐름 (`worker.handle_job`)

1. `ReportJob.ocr_quality == "needs_reupload"`이면 그래프를 돌리지 않는다. `reports.status = 'NEEDS_REUPLOAD'`만 기록하고 끝낸다.
2. 그 외에는 `ReportJob`을 초기 state로 매핑해 `graph.ainvoke()`를 실행한다. 그래프는 import 시점에 1회 컴파일해 재사용한다.
3. 실행 결과의 `errors`로 하드 실패인지 판정한다(아래 표).

| 실패 종류 | 예 | 처리 |
|---|---|---|
| **하드 실패** | `load_context_failed`, `persist_failed`, `ocr_result_missing` | `ReportWorkerError`를 던져 재전달. 상한을 넘기면 poison 스킵 |
| 소프트 실패 | `rag_empty`, `input_blocked`, `*_llm_failed` | 부분 결과로 저장하고 ack |
| 차단 기록 실패 | `persist_blocked_failed` | 하드 실패로 올리지 않는다. 재처리해도 같은 차단이 반복될 뿐이다 |

## 그래프 (`graph.py`)

```
load_context → input_guardrail ─┬─▶ persist_blocked → END                    (1)
                                └─▶ diagnosis ─┬─▶ terms_parse ─┐            (2)
                                               └────────────────┴─▶ coverage_parse
 → coverage_analysis → case_search ─┬─▶ disability_rag → disability_verify → disability_calc ─┐
                                    └───────────────────────────────────────────────────────┤  (3)
 payment_calc ◀─────────────────────────────────────────────────────────────────────────────┘
 → report_compose → output_guardrail → persist → END
```

1. 입력 가드레일이 차단하면(보험·법률 외 질문, 또는 `load_context`의 PII 복호화 실패) 초안 없이 `reports.status = 'BLOCKED'`만 기록한다.
2. 가입 약관이 `policy_chunks`에 없을 때만 런타임 파싱(`terms_parse`)을 거친다.
3. 후유장해 검토가 필요하면 장해 서브그래프를 거친다. 지급률 합산은 LLM이 아니라 `disability_rules.py`의 **순수 함수**가 한다.

| 파일 | 역할 |
|---|---|
| `nodes/agents.py` | 노드 구현과 분기 함수 |
| `state.py` | `ReportState` 정의 |
| `disability_rules.py` | 장해 지급률 결정론 합산(순수 함수) |
| `rag/hybrid.py` | `src/rag.search()` 어댑터. `RagResult`를 노드용 dict와 인용 문자열로 변환 |

## 저장 (`persist`, 한 트랜잭션)

| 테이블 | 내용 |
|---|---|
| `report_drafts` | 초안 전체(JSONB, 영구 보존). `report_id`로 `ON CONFLICT` 업서트 → **멱등** |
| `reports` | 적용 담보, 누락 특약, 근거 약관·판례, 예상 금액 범위. `status = 'AWAITING_ADOPTION'` |
| `report_issues` | 쟁점 목록. 해당 리포트의 기존 행을 지운 뒤 다시 넣는다 |

`reports.status`는 이 워커에서 `AWAITING_ADOPTION`·`BLOCKED`·`NEEDS_REUPLOAD` 셋 중 하나로 바뀐다.

## 입력 / 출력 (계약)

- **입력**: `core.contracts.ReportJob` (`report-job`)
- **출력**: 위 3개 테이블. 다음 단계로 메시지를 발행하지 않는다.

## 의존 / 배포

- `core`(sqs·db·ai_client·contracts·logging·crypto) · `rag` · `guardrail`
- 배포: `src/report_worker/Dockerfile`(slim) → ECR → K8s-manifests `envs/dev/report.values.yaml`의 이미지 태그 커밋 → **Argo CD**
- `src/report_worker/**`, `src/rag/**`, `src/guardrail/**`, 공용 코드가 바뀌면 재배포된다.

## dev 하네스 (배포 제외)

- `scripts/battery.py`: 검색 배터리와 e2e 시나리오 A~G. `PYTHONPATH=src python scripts/battery.py` (tempVectorDB와 Ollama 필요)
- `scripts/trace_state.py`: 노드별 state 변화 추적

## 참고

- [Notion 05 LangGraph](../../.claude/docs/05_langGraphAgent.md) · [아키텍처: langgraph](../../docs/architecture/langgraph.md) · [report-worker 문서](../../docs/report-worker/README.md)(후유장해 파이프라인·알려진 이슈) · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
