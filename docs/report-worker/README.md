# 리포트 워커(05번) — 구조·구현·데이터 기반 문서

> 대상 코드: `src/report_worker`, `src/rag`, `tempVectorDB/`
> 최종 점검일: 2026-07-15 (브랜치 `11-feature-langgraph-멀티에이전트-구현`)
> 이 문서는 원본 코드·원문 데이터와 직접 대조해 작성했다. 파일:라인 참조는 위 브랜치 기준.

## 문서 구성

| 문서 | 내용 |
|------|------|
| **README.md** (본 문서) | 전체 구조·노드 그래프·데이터 출처 한눈에 |
| [disability-pipeline.md](./disability-pipeline.md) | 후유장해 분류·지급률 계산 서브파이프라인 심층 |
| [schedule-data.md](./schedule-data.md) | 표준 장해분류표 데이터의 출처·추출·버전·검증 |
| [known-issues.md](./known-issues.md) | 확인된 버그·미구현·TODO (우선순위 포함) |

---

## 1. 이 워커가 하는 일

**보험 손해사정 리포트 초안을 자동 생성하는 Kafka 워커.** OCR·마스킹이 끝난 문서를 받아,
진단 분류 → 약관/특약 분석 → (필요 시) 후유장해 지급률 산정 → 보상 추정 → 리포트 조립 →
가드레일 → DB 저장까지 한 번의 LangGraph 실행으로 처리한다.

- **입력:** Kafka `report-job` 토픽의 `ReportJob` 메시지 (ID만 실림)
- **출력:** `report_drafts`(초안 JSON) + `reports`(요약 컬럼 갱신) + `report_issues`(쟁점)
- **멱등성:** `report_id` 기준. `persist`가 `ON CONFLICT (report_id)` 업서트
- **비스트리밍:** 완성된 초안을 1회 저장 (챗봇과 달리 실시간 스트림 아님)

### 메시지 계약 (`src/core/contracts.py:77`)

`ReportJob`은 **데이터를 나르지 않고 ID만 나른다.** 실제 내용은 워커가 DB에서 조회한다.

```
report_id      리포트 식별자(UUID, 멱등 키)
ocr_result_id  ocr_results.id 참조
job_id         원 OCR 작업 추적
doc_type       DocType (diagnosis|policy|payout_notice|claim|other)
user_ref       사용자 참조
claim_id       user_claims.id 패스스루(옵셔널)
created_at     발행 시각(UTC, ISO-8601)
```

---

## 2. 실행 진입점

```
Kafka report-job
   └─> KafkaConsumer (소비·검증·재시도·DLQ·오프셋 커밋)  ← core.kafka
         └─> worker.handle_job(job)                      ← src/report_worker/worker.py
               └─> _graph.ainvoke(state)                 ← LangGraph 그래프
```

- 그래프는 **import 시 1회 컴파일**해 재사용한다 (`worker.py:26`). DB에 접촉하지 않고 조립되므로 안전.
- `handle_job`은 그래프 실행만 담당하고, **하드 실패 시 예외를 올려** Kafka 소비자가
  재시도/DLQ 처리하게 한다 (`worker.py:33`).
- **하드 실패 판정** (`worker.py:23`): `load_context_failed` · `persist_failed` ·
  `ocr_result_missing` 마커만 재시도 대상. 그 외(`rag_empty` · `input_blocked` ·
  `*_llm_failed` 등)는 **부분결과로 커밋**한다 (이슈 #11 방침).

---

## 3. 노드 그래프 (전체 구조)

`src/report_worker/graph.py`에서 조립. 순차 실행 + 3개 조건 분기.

```
START
  │
  ▼
load_context ─────────── DB 4개 테이블 조회로 사고 컨텍스트 조립
  │                       (ocr_results, reports, user_claims, user_insurances)
  ▼
input_guardrail ──────── PII 마스킹 + 도메인 외 질문 차단
  │
  ├─[차단]──► persist_blocked ──► END   (reports.status='BLOCKED', 초안 없음)
  │
  ▼ [정상]
diagnosis ───────────── LLM: 진단명·ICD·사고유형·requires_disability_review 추출
  │
  ├─[약관 DB 없음]──► terms_parse ─┐   (현재 런타임 파싱 스텁)
  │                                │
  └─[약관 DB 있음]─────────────────┤
                                   ▼
                            coverage_parse ── 가입 특약 확정
                                   │
                                   ▼
                            coverage_analysis ── Hybrid RAG(terms) + LLM: 적용/누락 특약
                                   │
                                   ▼
                            case_search ──────── RAG(case): 판례·분쟁조정 근거
                                   │
       ┌───[후유장해 검토 필요]────┴───[불필요]───┐
       ▼                                          │
  disability_rag ── RAG(terms→level 폴백) + LLM   │
       │            장해 분류·지급률 추출          │
       ▼                                          │
  disability_calc ── 결정론 합산 (LLM 없음)        │
       │                                          │
       └──────────────► payment_calc ◄────────────┘   (2 incoming, 재합류)
                             │       보상 추정 범위(단정 금지)
                             ▼
                       report_compose ── LLM: 8섹션 본문 + issues + 생성 가드레일
                             │
                             ▼
                       output_guardrail ── 법적 고지문 + LLM Judge 인용 검증
                             │
                             ▼
                          persist ──────── report_drafts/reports/report_issues 저장
                             │
                             ▼
                            END
```

### 조건 분기 3개

| 분기 함수 | 위치 | 판단 기준 | 경로 |
|-----------|------|-----------|------|
| `route_after_input` | `agents.py:195` | `errors`에 `input_blocked` 있나 | `persist_blocked` / `diagnosis` |
| `policy_in_db` | `agents.py:171` | `policy_chunks`에 해당 insurer/product 있나 | `terms_parse` / `coverage_parse` |
| `route_after_case` | `agents.py:279` | `diagnosis.requires_disability_review` | `disability_rag` / `payment_calc` |

---

## 4. 상태(State) 구조

`ReportState` (`src/report_worker/state.py`) — `TypedDict(total=False)`. **전 노드 순차 실행이라
동시 쓰기가 없어 reducer 불필요.** 각 노드는 부분 dict만 반환하고 LangGraph가 머지한다.

주요 키:
- 입력: `report_id`, `ocr_result_id`, `claim_id`, `user_ref`, `doc_type`
- 컨텍스트: `case_info`, `masked_text`, `entities`, `subscribed_coverages`
- 분석: `diagnosis`, `retrieved_clauses`, `applicable_coverages`, `missing_coverages`,
  `coverage_analysis`, `disability_analysis`, `legal_references`, `estimated_range`
- 산출: `sections`, `issues`, `report`, `judge_failures`
- 운영: `errors` (노드별 폴백/실패 기록 — 부분결과 표기용)

### 오류 처리 패턴 — `@safe_node` (`agents.py:30`)

거의 모든 노드에 붙는 데코레이터. **노드가 던지면 그래프 전체가 죽는 대신 `{"errors": [...]}`만
머지**하고 다음 노드로 진행한다. 다운스트림은 전부 `state.get(key, default)`로 읽어 누락 키에
안전하다. → "한 노드 실패 = 전체 실패"가 아니라 "부분결과 리포트"가 나온다.

예외: `policy_in_db`·`route_after_input`·`route_after_case`는 분기 함수라 `@safe_node` 없음.

---

## 5. 데이터 기반 — 무엇이 어디서 오나

| 데이터 | 소스 | 사용 노드 | 성격 |
|--------|------|-----------|------|
| 사고 컨텍스트 | `ocr_results`·`reports`·`user_claims`·`user_insurances` | `load_context` | **런타임 DB 조회** |
| 진단 분류 | LLM(EXAONE 계열) 추출 | `diagnosis` | **LLM 생성** |
| 약관 조항 | `policy_chunks` (Hybrid RAG) | `coverage_analysis` | **RAG 검색** |
| 판례·분쟁조정 | `case_chunks` (Hybrid RAG) | `case_search` | **RAG 검색** |
| **표준 장해분류표** | `schedule_chunks` (금감원 시행세칙 원문) | `disability_rag` | **RAG 검색 (원문 기반)** ★ |
| 장해 분류·지급률 | LLM 추출 + 결정론 검증 | `disability_rag` | **LLM 생성 + 원문 대조** |
| 지급률 합산 | 순수 규칙 함수 | `disability_calc` | **결정론 (LLM/IO 없음)** ★ |
| 보상 추정 | 산술(제시금액 × 지급률 배수) | `payment_calc` | **결정론** |
| 리포트 본문 | LLM 작성 + 가드레일 | `report_compose`·`output_guardrail` | **LLM 생성** |

★ 표시가 후유장해 계산의 핵심. **"분류는 LLM, 합산은 결정론"** 원칙으로 분리돼 있고,
지급률 숫자는 원칙적으로 **원문 장해분류표에서만** 나온다. 상세는
[disability-pipeline.md](./disability-pipeline.md), 원문 데이터 출처는
[schedule-data.md](./schedule-data.md) 참조.

### RAG namespace ↔ 테이블 매핑 (`src/rag/search.py:40`)

| namespace | 테이블 | 내용 | 버전 필터 |
|-----------|--------|------|-----------|
| `terms` | `policy_chunks` | 가입 약관 조항 | 없음 |
| `case` | `case_chunks` | 판례·분쟁조정 | 없음 |
| `level` | `schedule_chunks` | 표준 장해분류표 | **있음** (계약일→개정판) |

---

## 6. 모듈 맵

```
src/report_worker/
├── __main__.py            엔트리(python -m report_worker)
├── worker.py              Kafka 핸들러 handle_job — 그래프 실행·하드실패 승격
├── graph.py               StateGraph 조립 (build_graph)
├── state.py               ReportState TypedDict
├── disability_rules.py    ★ 결정론 지급률 합산 (순수 함수, 단위테스트 가능)
├── nodes/
│   └── agents.py          14개 노드 전부 (async)
└── rag/
    └── hybrid.py          src/rag 어댑터 (RagResult → 노드용 dict 변환)

src/rag/                   공용 Hybrid RAG (report_worker·chatbot 공유)
├── router.py              쿼리 라우터(namespace 조합)
├── typo.py                pg_trgm 오타보정
├── search.py              tsvector+pgvector+RRF, 버전 필터(_version_filter)
└── fusion.py              RRF 통합

tempVectorDB/              장해분류표·판례 데이터 적재 도구 (일회성 ingest)
├── load_schedule.py       md → schedule_chunks 적재(임베딩 포함)
├── schedule_manifest.yaml 버전별 적용기간·파일 매핑
├── schedule/{2005,2018_04,2025_06}/disability_schedule.md  적재 대상 원문 md
└── schedule_data/         1차 원본(HWP·PDF)·추출 스크립트·검증 리포트
```

관련: `report_worker/rag/hybrid.py`는 자체 검색 구현이 아니라 **`src/rag`를 호출하는 얇은
어댑터**다(중복 제거 완료). pydantic `RagResult`를 리포트 노드가 쓰는 dict로 변환하고
`source_ref`를 사람이 읽는 인용 문자열로 포맷하는 것만 담당한다 (`rag/hybrid.py:1`).
