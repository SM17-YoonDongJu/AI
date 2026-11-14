# report_worker — 리포트 생성 SQS 워커 (05)

`report-job` SQS 메시지를 소비해 **LangGraph 멀티에이전트**로 손해사정 리포트 초안을 만들고 저장하는 워커다. LLM(`LLM_PROVIDER`, 기본 Claude)과 임베딩(`EMBEDDING_PROVIDER`)을 HTTP API로 부르므로 GPU 없이 **범용 노드**에 배포한다. 외부 API에는 마스킹된 텍스트만 보낸다(아래 "외부 전송 전 PII 마스킹").

## 실행

```bash
uv sync --extra report               # langgraph·langchain-core·boto3
uv run python -m report_worker       # uv 없이: PYTHONPATH=src python -m report_worker
```

- 진입점 `__main__.py`: `configure_logging` → `init_pool` → `run_migrations(pool, "migrations/ai")` → `SqsConsumer(SQS_REPORT_JOB_QUEUE_URL, ReportJob, journaled_handler(pool), on_poison=_poison_journal(pool)).run()` → `close_pool`
- 소비·검증·ack·재전달·poison 스킵·우아한 종료는 `core.sqs.SqsConsumer`가 맡는다.
- **기동 때 `migrations/ai`를 적용한다**(ai_owner, OCR과 같은 디렉터리). GPU EC2의 `ocr_worker`가 꺼져 있어도 이 워커가 자기 테이블(`ai.report_job_failures`)을 보장하기 위해서다. 두 워커가 동시에 기동해도 `core.db.run_migrations`의 디렉터리별 advisory lock이 한 번에 하나만 적용한다. 디렉터리가 없거나 적용이 실패하면 기동하지 않는다. `core.*`(`reports` 등)는 backend Flyway가 관리한다.

## 처리 흐름 (`worker.journaled_handler` → `worker.handle_job`)

1. `ReportJob.ocr_quality == "needs_reupload"`이면 그래프를 돌리지 않는다. `reports.status = 'NEEDS_REUPLOAD'`만 기록하고 끝낸다. 기록이 실패하면 저장 실패(`ReportWorkerError`, `persist_failed`)로 재전달한다.
2. 그 외에는 `ReportJob`을 초기 state로 매핑해 `graph.ainvoke()`를 실행한다. 그래프는 import 시점에 1회 컴파일해 재사용한다.
3. 실행 결과(`errors` 표식, `llm_failure`)로 메시지 처분을 정한다(아래 표, 위에서부터 순서대로 판정).
4. 컨슈머 핸들러는 `handle_job`을 감싼 `journaled_handler(pool)`다. 처분은 바꾸지 않고 실패 저널 기록만 덧붙인다(아래 "실패 저널").

**실패한 리포트는 저장하지 않는다.** 컨텍스트 하드 실패와 LLM 시스템 실패·거절은 그래프가 `persist` 전에 멈춘다(아래 "실패 단락").

| 실패 종류 | 예 | 처리 | 실패 저널(`failure_class`) |
|---|---|---|---|
| **하드 실패(컨텍스트)** | `load_context_failed`, `ocr_result_missing` | 초안 없음. `ReportWorkerError`로 재전달. 상한을 넘기면 poison 스킵 | 비종결 업서트 `context_unavailable` |
| **하드 실패(저장)** | `persist_failed`, `persist_refused_failed`, NEEDS_REUPLOAD 기록 실패 | 〃 | 비종결 업서트 `persist_failed` |
| **LLM 시스템 실패** | 노드 안의 `AiClientError`(429·5xx·타임아웃·연결 오류, 키·모델 누락, `max_tokens` 잘림) → `{노드}_failed:AiClientError` | 첫 실패에서 그래프 종료(이후 LLM 호출 없음). 초안 없음. `ReportWorkerError`로 재전달 | 비종결 업서트 `llm_unavailable` |
| 분류 밖 예외 | 그래프 밖에서 난 예외 | 그대로 올라가 재전달 | 비종결 업서트 `unknown`(error_type=클래스명) |
| **LLM 거절** | `LlmRefusalError`(서버 측 폴백 모델까지 거절) → `{노드}_failed:LlmRefusalError` | 초안 없이 `reports.status = 'BLOCKED'`만 기록. `ReportRefusedError`(`NonRetryableError`)로 즉시 ack | 남기지 않는다(BLOCKED로 보인다). 이전 행은 지운다 |
| 소프트 실패 | `rag_empty`, `case_data_missing`, `input_blocked`, `*_llm_failed`(JSON 파싱 실패), `pii_mask_failed:<필드>`, `collect_metrics_failed:*`(리포트 수치 계산 실패 — `metrics` 없이 저장, `confidence_level`은 안 쓴다), 그 밖의 노드 예외 | 부분 결과로 저장하고 ack | 행 삭제(회복) |
| Judge 실패 | 출력 가드레일 Judge의 `AiClientError`(거절 포함) 또는 판정 불가 응답(`chat_json` 파싱 실패 `{}`, `failures`가 없거나 list가 아님 등) | `judge_failures=[JUDGE_UNAVAILABLE]`을 남기고 저장. 본문은 완성됐다. 리포트 수치엔 `judge.unavailable=true`, 신뢰도는 MEDIUM 이하 | 행 삭제(회복) |
| 차단 기록 실패 | `persist_blocked_failed` | 하드 실패로 올리지 않는다. 재처리해도 같은 차단이 반복될 뿐이다 | 행 삭제(정상 반환이라서 — 아래 한계) |

- 재전달 한도는 `SQS_MAX_RECEIVE_COUNT`(report 운영값 3)다. 같은 실패가 3번 처리된 뒤 4번째 수신에서 poison 스킵된다. Claude SDK도 호출마다 자체 재시도(최대 2회)를 한 뒤에 `AiClientError`를 낸다.
- **poison 스킵 직전에 `_poison_journal` 훅이 저널 행을 `terminal=true`로 확정한다.** `reports.status`는 그대로(`AWAITING_INSPECTION`)지만, Spring이 이 행을 보고 FAILED를 표시한다.
- 하드 실패·LLM 실패 때 worker가 `report not saved → redeliver`(warning, `failure`·`errors`), `report status not saved → redeliver`(NEEDS_REUPLOAD 기록 실패, `error_type`) 또는 `report refused by llm → blocked`(warning)를 남긴다. 컨슈머 로그에는 report_id와 사유가 없기 때문이다. 필드는 errors 표식(노드명·예외 클래스명)뿐이다.
- 한계: `persist_blocked_failed`(입력 차단 뒤 BLOCKED 기록 실패)는 정상 반환(ack)이라 poison까지 가지 않는다. 상태가 안 바뀐 채 저널에도 남지 않으므로 Spring에선 PROCESSING에 머문다(이번 범위 밖, 기존 결정).

## 실패 저널 (`ai.report_job_failures`, `migrations/ai/012`)

재전달 한도를 다 써 poison으로 걷힌 리포트가 Spring에서 영원히 "처리 중"으로 남지 않게 하는 기록이다. OCR 실패 저널(`ai.ocr_job_failures`, 008)과 같은 방식이다. 코드는 `failure_journal.py`.

| 시점 | 함수 | 동작 |
|---|---|---|
| 하드 실패로 재전달될 때마다 | `record_report_failure`(`journaled_handler`) | `report_id`로 비종결 행 업서트. 반복하면 `attempts` +1, `failure_class`·`error_type`은 최신 실패로 |
| 종결 상태에 이르면(저장·BLOCKED·NEEDS_REUPLOAD, 거절 포함) | `clear_report_failure` | 그 `report_id`의 행 삭제(terminal 행이어도 — 실제로 끝났으면 실패가 아니다) |
| poison으로 걷기 직전 | `mark_report_failure_terminal`(`__main__._poison_journal`) | `terminal=true` 확정. 기존 `failure_class` 보존, `attempts`=max(실패 횟수, 수신 횟수). 기존 행이 없으면 `unknown` |
| 본문 역직렬화 실패·`report_id`가 UUID 아님 | 〃 | `report_id` 없이 `message_id`로 `schema_invalid` 행(Spring은 조인 못 함 — 알려진 한계) |

- `terminal`은 단방향(false→true)이다. 비종결 업서트 SQL은 `terminal`을 SET 절에 두지 않는다.
- `failure_class`: `context_unavailable` · `llm_unavailable` · `persist_failed` · `schema_invalid` · `unknown`(`FailureClass`, 마이그레이션 CHECK와 1:1 — 테스트가 고정한다).
- 예외 메시지는 저장하지 않는다. `error_type`은 클래스명만이다(§9). 문서 0건(`ocr_result_missing`)처럼 예외가 없으면 `NULL`이다.
- 저널 쓰기 실패: 기록·삭제는 삼키고 타입만 경고로 남긴다(`report_job_failure_journal_failed`·`report_job_failure_clear_failed`). 처분(재전달·ack)은 바뀌지 않는다. poison 훅의 실패는 삼키지 않는다 — 컨슈머가 메시지를 지우지 않고 다음 수신에서 다시 시도한다(OCR 훅과 같다).
- Spring 계약: `app_owner`에 테이블 SELECT만 준다. `report_id`로 조인하고 `terminal` 행만 FAILED로 본다. 종결 상태(BLOCKED·NEEDS_REUPLOAD·`applicable_guarantees` 있음) 판정이 저널보다 먼저다.

## 그래프 (`graph.py`)

```
load_context → input_guardrail ─┬─▶ persist_blocked → END                    (1)
                                └─▶ diagnosis ─┬─▶ terms_parse ─┐            (2)
                                               └────────────────┴─▶ coverage_parse
 → coverage_and_disability ┌ 보장: coverage_analysis → case_search                             (3)
                           └ 장해: disability_rag → disability_verify → disability_calc   (동시에)
 → payment_calc → report_compose → output_guardrail → collect_metrics → persist → END        (4)
```

1. 입력 가드레일이 차단하면(보험·법률 외 질문, 또는 `load_context`의 PII 복호화 실패) 초안 없이 `reports.status = 'BLOCKED'`만 기록한다.
2. 가입 약관이 `policy_chunks`에 없을 때만 런타임 파싱(`terms_parse`)을 거친다.
3. `coverage_and_disability` 한 노드가 보장 줄기와 장해 줄기를 **동시에** 돌린다(성능 실험 R6).
   - 장해 줄기는 진단이 후유장해 검토 대상일 때만 돈다(`route_after_case` — 진단만 본다). 지급률 합산은 LLM이 아니라
     `disability_rules.py`의 **순수 함수**가 한다.
   - 두 줄기는 서로의 결과를 쓰지 않는다. 장해 노드가 보장 쪽에서 읽는 것은 인용 청크를 이어 붙일
     `retrieved_clauses`뿐이다. 결과는 순차 실행(보장 → 장해)과 같은 순서로 합친다.
     - `errors`·`node_trace`는 줄기 순서대로 잇는다
     - 청크는 보장 결과 뒤에 장해가 새로 더한 것만 붙인다
   - 한 줄기가 LLM 시스템 실패·거절로 멈추면 다른 줄기를 바로 취소한다. 순차였다면 부르지 않았을 Claude 호출이
     결과를 버릴 채로 과금되지 않게 하기 위해서다. 다음 간선의 실패 단락이 그래프를 멈춘다.
   - 노드별 시간은 그대로 `node_trace`에 남고 `group`·`lane` 표식이 붙는다. 총 소요 시간(`total_ms`)은 묶음마다 가장
     긴 줄기만 더한 임계 경로다(`metrics.critical_path_ms`).
4. `collect_metrics`는 LLM·DB 없이 리포트 수치와 신뢰도 등급을 계산한다(아래 "리포트 수치"). 실패해도 소프트 실패로 저장까지 간다.

### 실패 단락

종료 노드(`persist`·`persist_blocked`·`persist_refused`)를 뺀 **모든 간선**이 다음 노드보다 `agents.failure_route`를 먼저 본다.

```
(모든 비종료 노드) ─┬─ context_failure    ─▶ END                        (load_context_failed·ocr_result_missing)
                    ├─ llm_system_failure ─▶ END                        (AiClientError)
                    ├─ llm_refused        ─▶ persist_refused ─▶ END     (LlmRefusalError → BLOCKED)
                    └─ (실패 없음)         ─▶ 위 그림의 다음 노드
```

- 예외 분류는 `safe_node`가 예외 타입(`isinstance`)으로 한 번 한다. 결과는 `state["llm_failure"]`에 남는다. errors 표식은 기존 형식 `{노드}_failed:{예외 타입}` 그대로다.
- 단락은 노드가 아니라 간선에 걸린다. 그래서 LLM을 부르는 노드가 몇 개든, 몇 번째 호출에서 실패하든 바로 다음 간선에서 멈춘다.
- 간선은 `graph.py`의 `_edge`·`_branch`로만 잇는다. `g.add_edge`로 직결하면 그 간선에서 단락이 빠진다. `tests/test_report_failure_routing.py`의 구조 테스트가 이를 막는다.
- `persist_refused`는 `persist_blocked`와 같은 UPDATE를 쓴다. 다만 기록에 실패하면 하드 실패(`persist_refused_failed`)로 재전달한다. 기록 없이 즉시 ack하면 리포트가 영원히 처리 중으로 남기 때문이다.

| 파일 | 역할 |
|---|---|
| `nodes/agents.py` | 노드 구현과 분기 함수 |
| `state.py` | `ReportState` 정의. 선언하지 않은 키는 LangGraph가 조용히 버린다 |
| `disability_rules.py` | 장해 지급률 결정론 합산(순수 함수) |
| `metrics.py` | 리포트 수치·신뢰도 등급·Judge 근거 조립(순수 함수) |
| `rag/hybrid.py` | `src/rag.search()` 어댑터. `RagResult`를 노드용 dict와 인용 문자열로 변환 |

## 외부 전송 전 PII 마스킹 (CODE_CONVENTIONS §13)

LLM(Claude)과 임베딩(Voyage 전환 예정)은 외부 API다. 그래서 Spring DB에서 복호화한 **자유입력 필드**는 `load_context`가 복호화 직후 `guardrail.mask_pii`(OCR 입력단과 같은 규칙)를 한 번 거친 값만 `case_info`에 담는다. 원문은 state·로그·예외 어디에도 남기지 않는다.

| `case_info` 키 | 출처 | 지금 쓰는 곳 |
|---|---|---|
| `question` | `reports.question` | `coverage_analysis` RAG 쿼리(→ 임베딩 API), `report_compose` 본문 프롬프트(→ LLM) |
| `description` | `user_claims.description` | 없음(마스킹본만 들고 있다) |
| `additional_information` | `user_claims.additional_information` | 없음(마스킹본만 들고 있다) |

- `None`(행·값 없음)은 `None` 그대로다. `mask_pii`에 넘기지 않는다.
- **`MaskingError`**(마스킹 뒤에도 주민·외국인등록번호·카드가 남음): 원문으로 폴백하지 않는다. 그 필드만 `None`으로 비우고 `errors`에 `pii_mask_failed:<필드>`를 남긴 뒤 리포트를 계속 만든다. 이 표식은 리포트 `7_추가확인필요`에도 보인다. 로그(`free text pii masking failed`)에는 필드명과 예외 타입만 남는다.
- **`ModuleNotFoundError`**(`USE_NER=true`인데 이미지에 transformers 없음): 설정 오류라 잡지 않는다. `load_context_failed:ModuleNotFoundError` 하드 실패로 드러난다.
- OCR `masked_text`에는 다시 걸지 않는다. OCR 워커가 같은 규칙으로 이미 가렸고, `mask_pii`는 멱등이 아니라 다시 걸면 날짜·금액까지 가린다. `input_guardrail`의 `guard_input`은 별개 규칙이다.
- `mask_pii`는 동기 함수라 `asyncio.to_thread`로 뗀다(NER을 켜면 CPU 추론이 낀다).
- 한계: 운영 report는 `USE_NER=false`라 라벨 없는 자유문 속 이름("저는 홍길동입니다")은 그대로 나간다. 다른 한계는 `src/guardrail/README.md`의 `mask_pii` 절에 있다.

외부로 나가는 나머지 입력(2026-11-01 전수 점검):

| 입력 | 나가는 곳 | 처리 |
|---|---|---|
| `masked_text`(OCR) | `diagnosis` 프롬프트 | OCR 단계에서 마스킹됨 + `guard_input` 정규식 |
| 진단명(`diagnosis` 노드 결과, 없으면 `case_info.diagnosis`)·ICD | 특약 분석·판례 필터·장해표·장해 감수·본문 프롬프트, RAG 쿼리 전부, Judge(`1_사건요약`) | 마스킹 안 함 — 건강정보는 §13상 전송 허용 |
| `subscribed_coverages`(`user_insurances.coverages`) | `coverage_analysis` 프롬프트 | 마스킹 안 함 — 특약명(식별자 아님) |
| `offered_amount` → `estimated_range` | `report_compose` 프롬프트 | 금액(식별자 아님) |
| 앞 노드의 LLM 결과(적용·누락 특약, 분석, 장해 항목, 본문) | 뒤 노드 프롬프트, Judge | 마스킹된 입력과 공개 코퍼스로만 만든 값 |
| `insurer`·`product_name`·`enrolled_at` | RAG의 로컬 SQL 필터뿐 | 외부로 안 나감 |
| `entities` | `payment_calc` 로컬 계산뿐 | 외부로 안 나감 |
| 약관·판례·장해표 청크 | 각 프롬프트, Judge(2026-11-11부터 판례도 근거로 넘긴다) | 공개 코퍼스 |
| `errors` → `7_추가확인필요` | Judge(리포트 앞 12,000자 — `guardrail.JUDGE_MAX_REPORT_CHARS` — 안에 들면) | 분류 표식만(PII 없음) |

## 저장 (`persist`, 한 트랜잭션)

| 테이블 | 내용 |
|---|---|
| `report_drafts` | 초안 전체(JSONB, 영구 보존). `metrics`(리포트 수치, 계산에 실패하면 키 없음) 포함. `report_id`로 `ON CONFLICT` 업서트 → **멱등** |
| `reports` | 적용 담보, 누락 특약, 근거 약관·판례, 예상 금액 범위. `status = 'AWAITING_ADOPTION'`. 신뢰도 등급 `confidence_level`(`HIGH`·`MEDIUM`·`LOW`)은 세이브포인트로 따로 쓴다(아래) |
| `report_issues` | 쟁점 목록. 해당 리포트의 기존 행을 지운 뒤 다시 넣는다 |

`reports.status`는 이 워커에서 `AWAITING_ADOPTION`·`BLOCKED`·`NEEDS_REUPLOAD` 셋 중 하나로 바뀐다.

| 경로 | `reports.status` | 초안 | `ai.report_job_failures` | Spring `analysis_state` |
|---|---|---|---|---|
| 정상·부분 저장 | `AWAITING_ADOPTION` | 있음 | 행 삭제 | COMPLETED |
| 입력 차단, LLM 거절 | `BLOCKED`(Spring: 사유 구분 없이 "처리할 수 없는 요청" 안내, 종료 상태) | 없음 | 행 삭제(거절은 기록 안 함) | BLOCKED |
| OCR 품질 미달 | `NEEDS_REUPLOAD` | 없음 | 행 삭제 | NEEDS_REUPLOAD |
| 하드 실패·LLM 시스템 실패(재전달 중) | 바뀌지 않음(Spring 생성값 `AWAITING_INSPECTION`) | 없음 | 비종결 행(`terminal=false`) | PROCESSING |
| 위 실패가 한도를 넘겨 poison 스킵 | 바뀌지 않음 | 없음 | **terminal 확정** | **FAILED** |

**`reports.confidence_level`** — backend가 고객 상세·검수 화면에 내려주고 프론트가 추정 보상 범위 옆 신뢰도 게이지로 보여주는 컬럼이다(값은 대문자, `draft.metrics.confidence.level`과 같다).

- backend 소유 컬럼이라 `ai_owner`(prod `ai_owner_prod`)에 이 컬럼의 UPDATE 권한이 따로 필요하다(`deploy/schema_split*.sql`).
- persist는 이 UPDATE만 같은 트랜잭션 안의 **세이브포인트**(중첩 `conn.transaction()`)로 뗀다. 권한이 없으면(`InsufficientPrivilegeError`) 세이브포인트만 되돌리고 경고 `report confidence level not saved`(`error_type`)를 남긴다. 초안·`reports`·쟁점은 그대로 커밋된다. 그 밖의 DB 오류는 지금처럼 `persist_failed`(하드 실패, 재전달)다.
- **배포 순서와 무관하다.** GRANT 전에 워커를 배포해도 리포트는 저장되고 경고만 남는다. GRANT를 적용하면 재배포 없이 다음 리포트부터 채워진다. 그 전에 저장된 리포트는 재처리해야 채워진다.
- 수치 계산이 실패했거나(`collect_metrics_failed:*`) 등급이 세 값이 아니면 쓰지 않는다. 같은 리포트를 다시 처리했는데 그때만 계산이 실패하면 이전 값이 남는다.

## 리포트 수치 (`draft.metrics`, `metrics.py`)

리포트를 만드는 동안 나온 기술 수치를 구조화해 `report_drafts.draft["metrics"]`에 남긴다(심사·설명가능성). 등급(`confidence.level`)은 `reports.confidence_level`로도 나간다(위 "저장"). 이 절이 스키마의 단일 출처다(`.claude/docs/contracts.md` §4가 여기를 가리킨다). 키를 바꾸거나 빼면 `metrics.METRICS_VERSION`을 올린다.

- 계산: `collect_metrics` 노드(`output_guardrail` 뒤, `persist` 앞)가 `metrics.build_report_metrics(state)`를 부른다. LLM·DB 호출이 없는 순수 계산이다.
- 재료는 앞 노드가 state에 남긴다: `node_trace`(safe_node — 노드마다 ms·ok/failed), `input_documents`(load_context — `ai.ocr_results`의 `ocr_confidence`·`doc_type_confidence`), `case_funnel`(case_search), `judge_stats`(output_guardrail — `OutputGuardResult.judge_*`).
- 이 재료가 없거나 모양이 어긋나도 그 항목만 0·빈 값·null이 되고 계산은 계속한다. 수치에는 유한한 숫자만 들어간다(NaN·무한대가 하나라도 있으면 PG jsonb가 초안 전체를 거부해 persist가 하드 실패한다). 계산 자체가 예외로 끝나면 `collect_metrics_failed:*` 소프트 실패다(초안은 `metrics` 없이 저장). 읽는 쪽은 `metrics` 키가 없을 수 있음을 허용해야 한다.
- PII 없음: 분류값(doc_type·마스킹 라벨·노드명)·건수·글자 수·정규화한 조항 번호뿐이다. 예외는 `verification.disability.reclassified[].injury`(LLM이 마스킹된 문서에서 뽑은 손상명 — 건강정보, `draft.disability`에 이미 있는 값)다.

### 스키마 (version 1)

예시는 `tests/test_report_metrics.py`의 골든 픽스처 출력이다(`pipeline.nodes`는 줄였다).

```json
{
  "version": 1,
  "input": {
    "document_count": 3,
    "doc_types": ["policy", "diagnosis", "medical_receipt"],
    "ocr_confidence": {"min": 0.93, "avg": 0.94},
    "doc_type_confidence": {"min": 0.8, "avg": 0.9},
    "pii_masked": {"total": 6, "by_label": {"주민등록번호": 1, "전화번호": 2, "이름": 3}, "documents": 4, "free_text": 2}
  },
  "retrieval": {
    "policy_matched": true,
    "evidence": {"terms": 2, "schedule": 1, "case": 2},
    "case_funnel": {"candidates": 12, "deduped": 9, "relevant": 2, "kept": 2},
    "disability_schedule_source": "terms"
  },
  "verification": {
    "disability": {"extracted": 2, "verified": 2, "combined_rate": 20.0, "confidence": "low",
                   "pending_reeval": false, "reclassified": [{"injury": "추간판 탈출", "from": 30.0, "to": 20.0}]},
    "clause_citations": {"cited": 3, "grounded": 2, "ungrounded": ["제99조"], "statute_mentions": 1},
    "judge": {"ran": true, "unavailable": false, "flagged": 1, "checked_chars": 12000, "total_chars": 12480,
              "coverage": 0.96, "evidence_chunks": 5}
  },
  "pipeline": {"nodes": [{"node": "load_context", "ms": 140, "status": "ok"}, "..."],
               "node_count": 12, "failed_nodes": [], "total_ms": 73043},
  "confidence": {"level": "MEDIUM", "reasons": ["가입금액 미보유 — 제시액 기준 어림 범위", "근거 미확인 주장 1건",
                 "인용 검증 범위 일부(96%)", "근거에 없는 조항 인용 1건", "후유장해 지급률 추정 불확실"]}
}
```

| 키 | 값 | 출처 |
|---|---|---|
| `input.document_count`·`doc_types` | 문서 수, 문서 순 doc_type(중복 포함) | `input_documents`. 대표 문서 단건 경로의 유형은 ReportJob `doc_type` |
| `input.ocr_confidence`·`doc_type_confidence` | `{min, avg}`(소수 3자리) 또는 null | 같은 이름의 `ai.ocr_results` 컬럼. null·숫자 아닌 값·NaN·무한대는 빼고 나머지는 [0, 1]로 자른다. 남는 값이 없으면 null |
| `input.pii_masked` | `total`·`by_label`(한국어 라벨별)·`documents`·`free_text` | `guardrail.count_masked_pii`. documents = OCR `masked_text`, free_text = `case_info`의 `question`·`description`·`additional_information`(mask_pii 마스킹본) |
| `retrieval.policy_matched` | bool | errors에 `policy_not_in_db*`가 없으면 true |
| `retrieval.evidence` | `{terms, schedule, case}` 건수 | Judge 근거(`metrics.judge_evidence` — 중복 제거 후). schedule = namespace `level` 또는 chunk_type `schedule`, case = `legal_references` |
| `retrieval.case_funnel` | `{candidates, deduped, relevant, kept}` 또는 null | case_search(검색 후보 → 사건 단위 중복 제거 → 관련성 필터 → 상위 4). 결과 0건이면 전부 0, case_search가 예외로 끝났으면 null. 필터는 판정 실패·전부 탈락이면 그대로 통과시키므로 relevant = deduped가 "걸러낼 게 없었다"는 뜻은 아니다 |
| `retrieval.disability_schedule_source` | `terms`·`standard`·null | 가입 약관 장해분류표 / 표준표 폴백(`disability_fallback_standard_schedule`) / 장해 경로 아님·표 미검색 |
| `verification.disability` | 객체 또는 null | `disability_analysis`. `reclassified` = disability_verify가 과대분류로 낮춘 항목(`rate_original` → `rate`). 장해 경로가 아니면 null |
| `verification.clause_citations` | `cited`·`grounded`·`ungrounded`(본문 첫 등장 순)·`statute_mentions` | `sections["6_본문"]` 정규식(아래) |
| `verification.judge` | `ran`(= checked > 0)·`unavailable`(JUDGE_UNAVAILABLE 있음)·`flagged`(그 표식 뺀 판정 수)·`checked_chars`·`total_chars`·`coverage`(checked/total, 소수 2자리, total 0이면 0.0)·`evidence_chunks` | `judge_stats`·`judge_failures` |
| `pipeline` | `nodes`(`{node, ms, status}`)·`node_count`·`failed_nodes`·`total_ms` | `node_trace`. output_guardrail까지다(collect_metrics·persist는 계산 시점에 안 끝났다). ms·깔때기 값은 음수 아닌 정수로 정규화한다 |
| `confidence` | `{level, reasons}` | `metrics.grade_confidence`(아래 표) |

**조항 인용 실재율**(LLM 없이 정규식): 본문에서 `제\s*(\d+)\s*조(?:\s*의\s*(\d+))?`를 찾아 `제12조`·`제12조의2`로 정규화하고 중복을 뺀다. 그중 법령 조문은 `statute_mentions`로만 센다. 법령 조문은 둘 중 하나다.

1. **법령 이름이 인용 바로 앞에 붙어 있다.** 인용 앞 20글자 안에서 `법`·`법률`·`시행령`·`대통령령`·`총리령`·`부령`·`시행규칙`·`규칙`으로 끝나고, 그 뒤엔 `상`·닫는 괄호(`」』)`)·공백만 오는 경우다(정규식 `(?:(?<!방)법|[예소]방법|법률|시행령|대통령령|총리령|부령|시행규칙|규칙)상?[」』)]?\s*$`). 예: 상법 제651조, 민법상 제750조, 보험업법 시행령 제5조, 동법 제7조, 「약관의 규제에 관한 법률」 제6조. 맨 `령`은 넣지 않는다("보험금 수령 제5조"·"가입 연령 제3조"는 약관 인용). `방법`도 법령 이름이 아니다("청구 방법 제9조"). 단 `방법`으로 끝나는 실제 법령인 `예방법`·`소방법`(감염병예방법 제49조, 소방법 제3조)은 법령으로 본다. "대법원은 제12조"·"보험업법상 약관 제3조"처럼 사이에 다른 말이 끼면 약관 인용이다.
2. **법령 조문에 열거로 이어진다.** 바로 앞 인용이 법령 조문이고 그 사이가 `및`·`와`·`과`·`또는`·`내지`·`,`·`·`·`ㆍ`·`~`·`제N항`·`제N호`뿐이면 같은 법령이다. 예: "상법 제651조 및 제652조", "제651조 제1항, 제652조".

나머지가 `cited`이고, 그중 근거 청크의 `article_number`나 `text`에 같은 조항이 있으면 `grounded`, 없으면 `ungrounded`다.

### 신뢰도 등급 (`grade_confidence`)

프론트 게이지는 **추정 보상 범위**의 신뢰도다(HIGH "보수적 범위" / MEDIUM "추가 자료 시 조정" / LOW "자료 부족·변동 폭 큼"). HIGH에서 시작해 걸리는 조건마다 상한을 낮추고 사유를 모은다. LOW 조건이 하나라도 있으면 LOW, 아니면 MEDIUM 조건이 있으면 MEDIUM이다. 사유는 화면에 그대로 보여도 되는 문구이고 LOW 사유가 먼저 온다.

| 상한 | 조건 | 사유(문구 그대로) |
|---|---|---|
| LOW | errors에 `rag_empty` | 약관 근거를 찾지 못함 |
| LOW | errors에 `policy_not_in_db` 접두 | 가입 상품 약관이 DB에 없어 일반 약관으로 분석 |
| LOW | `estimated_range.max` ≤ 0 또는 없음(payment_calc 실패 포함) | 보상 범위 산출 불가(제시액·가입금액 없음) |
| MEDIUM | `payment_breakdown` 비어 있음 | 가입금액 미보유 — 제시액 기준 어림 범위 |
| MEDIUM | `judge.ran`이 false(판정 불가, 또는 근거가 없어 Judge를 안 돌림) | 인용 검증 미수행 |
| MEDIUM | `judge.flagged` > 0 | 근거 미확인 주장 n건 |
| MEDIUM | `judge.ran`이고 `checked_chars` < `total_chars` | 인용 검증 범위 일부(xx%) — %는 내림 |
| MEDIUM | `clause_citations.ungrounded` 있음 | 근거에 없는 조항 인용 n건 |
| MEDIUM | 장해 경로(`requires_disability_review` 또는 분석 있음)인데 분석 없음·표 미검색·재평가 대상·confidence≠high·하향 교정 | 후유장해 지급률 추정 불확실 |
| MEDIUM | `input.ocr_confidence.min` < `OCR_LOW_CONFIDENCE_THRESHOLD` | 문서 인식 신뢰도 낮음 |
| MEDIUM | errors에 `pii_mask_failed:` 접두 | 개인정보 보호로 일부 입력 제외 |
| MEDIUM | errors에 `diagnosis_llm_failed` | 진단 정보 자동 추출 실패 |
| MEDIUM | `pipeline.failed_nodes` 있음 | 일부 분석 단계 실패 |

### 한계

- **운영에선 지금 항상 MEDIUM 이하다.** `load_context`가 `coverage_details`(특약별 가입금액)를 채우지 않아 `payment_breakdown`이 늘 비기 때문이다. 제시액 배수 어림 범위라는 사실을 그대로 드러낸 의도된 결과다.
- **LOW 문구와 원인이 어긋나는 경우가 있다(2026-11-12 결정: 그대로 둔다).** LOW 사유 중 "약관 근거를 찾지 못함"(`rag_empty`)·"가입 상품 약관이 DB에 없어 일반 약관으로 분석"(`policy_not_in_db`)은 우리 약관 코퍼스가 비어서 생긴다. 그런데 프론트 LOW 문구는 "자료가 부족해 변동 폭이 커요. 자료 보강을 권해요."로 고객에게 행동을 요구한다(`terms_parse`는 스텁이라 고객이 자료를 더 내도 등급이 바뀌지 않는다). 그래도 신뢰도를 실제보다 높게 보이지 않도록 LOW를 유지한다. 사유별 문구는 2단계(backend가 `draft.metrics.confidence.reasons`를 내려주고 프론트가 사유를 보여줌)에서 해결한다.
- `pipeline.total_ms`는 노드 소요 시간의 합이다. 라우터(`policy_in_db`의 DB 조회)·LangGraph 오버헤드·collect_metrics·persist는 빠진다(벽시계 시간이 아니다).
- 조항 인용은 휴리스틱이다. `제`가 없는 인용("12조")은 세지 않는다. 법령 이름과 인용 사이에 괄호 개정일 같은 말이 끼면("상법(2014. 3. 11. 개정) 제651조") 약관 인용으로 센다(근거에 없으면 `ungrounded`). 거꾸로 `법`·`규칙`으로 끝나는 일반 명사가 인용에 바로 붙으면("해석 규칙 제5조") 법령으로 빠진다(`방법`만 뺐다. "도수치료 요법 제3조"·"비급여 치료법 제4조"도 법령으로 빠진다). `grounded`는 그 조항 번호가 근거에 있다는 뜻일 뿐이고, 인용한 내용이 맞는지는 Judge 몫이다.
- Judge 근거 상한은 20건(약관 8 + 장해분류표 ≤8 + 판례 4)이다. 어느 검색의 top_k라도 늘리면 맨 뒤 판례부터 조용히 빠진다. `judge.evidence_chunks` < `retrieval.evidence` 합계로만 드러난다.
- `pii_masked`는 `[분류]` 토큰 수다. 원문에 처음부터 있던 `[이름]` 글자도 세고, `guard_input`의 `***` 가림과 마스킹 실패로 비운 필드는 세지 않는다.
- OCR 신뢰도 임계값은 이 워커의 `OCR_LOW_CONFIDENCE_THRESHOLD`(기본 0.90)다. OCR 워커에서 이 값을 다시 맞추면 report 배포 env에도 같은 값을 줘야 판정이 같아진다. 비교는 표시값(소수 3자리)으로 한다(`real` 왕복 오차 0.9 → 0.8999999를 흡수). `0.0`은 인식 라인이 하나도 없던 문서다.

## 입력 / 출력 (계약)

- **입력**: `core.contracts.ReportJob` (`report-job`)
- **출력**: 위 3개 테이블 + 실패 저널 `ai.report_job_failures`(Spring이 읽기 전용으로 소비). 다음 단계로 메시지를 발행하지 않는다.

## 의존 / 배포

- `core`(sqs·db·ai_client·contracts·logging·crypto) · `rag` · `guardrail`
- 배포: `src/report_worker/Dockerfile`(slim) → ECR → K8s-manifests `envs/dev/report.values.yaml`의 이미지 태그 커밋 → **Argo CD**
- `src/report_worker/**`, `src/rag/**`, `src/guardrail/**`, `migrations/ai/**`, 공용 코드가 바뀌면 재배포된다.
- 이미지에 `migrations/ai`를 싣는다(Dockerfile `COPY migrations/ai ./migrations/ai`, WORKDIR `/app` 기준 — `__main__._MIGRATIONS_DIR`). 마이그레이션만 바꿔도 이미지를 다시 빌드해야 반영된다. `migrations/corpus`는 다른 owner 소유라 싣지 않는다(§14).
- DB 계정은 `ai_owner`여야 한다. 기동 마이그레이션은 `ai` 스키마의 CREATE 권한과 `ai` 테이블 소유권이 필요하다(아니면 `001`의 `CREATE TABLE IF NOT EXISTS`에서 permission denied로 기동 실패).
- `guardrail.mask_pii`는 OCR 마스커(`src/ocr_worker/masking/**`, import 체인 `src/ocr_worker/{__init__,ocr,storage}.py`)를 그대로 쓴다. 이 경로만 바뀌어도 report를 다시 배포해야 두 워커의 마스킹 규칙이 갈라지지 않는다(`.github/workflows/deploy.yml`의 `report` 필터). 같은 이유로 Dockerfile의 `COPY src ./src`를 좁히면 안 된다.

## dev 하네스 (배포 제외)

- `scripts/battery.py`: 검색 배터리와 e2e 시나리오 A~G. `PYTHONPATH=src python scripts/battery.py` (tempVectorDB와 Ollama 필요)
- `scripts/trace_state.py`: 노드별 state 변화 추적

## 참고

- [Notion 05 LangGraph](../../.claude/docs/05_langGraphAgent.md) · [아키텍처: langgraph](../../docs/architecture/langgraph.md) · [report-worker 문서](../../docs/report-worker/README.md)(후유장해 파이프라인·알려진 이슈) · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
