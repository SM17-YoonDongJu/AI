# 리포트 워커(05번) — 후유장해 지급률 시스템 문서

> 자동 합본: README + disability-pipeline + schedule-data + known-issues
> 노션에 이 파일 전체를 복사→붙여넣기 하면 헤딩·표·코드블록으로 변환됩니다.

---

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


---

# 후유장해 지급률 산정 파이프라인 — 심층

> 관련 코드: `src/report_worker/nodes/agents.py`, `src/report_worker/disability_rules.py`,
> `src/rag/search.py`
> 상위 문서: [README.md](./README.md) · 원문 데이터: [schedule-data.md](./schedule-data.md)

이 문서는 리포트 워커에서 **후유장해 지급률을 어떻게 산정하는가**를 코드 흐름 그대로 설명한다.

---

## 0. 설계 원칙 — "분류는 LLM, 합산은 결정론"

후유장해 산정은 두 단계로 엄격히 분리돼 있다.

1. **분류·추출 (`disability_rag`)** — 사고가 장해분류표의 어느 항목에 해당하고 지급률이 몇 %인지
   찾는다. 이건 자연어 이해가 필요해 **LLM**이 한다. 단, **지급률 숫자는 원문 표에서만** 나오도록
   결정론 백스톱으로 검증한다.
2. **합산 (`disability_calc` → `combine_disability_rate`)** — 여러 장해의 지급률을 총칙 규칙에 따라
   합산한다. 이건 **LLM·DB·IO가 전혀 없는 순수 함수**라 재현·감사·단위테스트가 된다.

이렇게 나눈 이유: 지급률은 **돈에 직결**되므로, 확률적으로 흔들리는 LLM이 최종 숫자를 만들면 안 된다.
LLM은 "어느 항목인가"까지만, 최종 계산은 검증 가능한 코드가 한다.

---

## 1. 진입 조건 — 언제 장해 분기를 타나

`case_search` 다음의 조건 분기 `route_after_case` (`agents.py:279`):

```python
def route_after_case(state):
    if (state.get("diagnosis") or {}).get("requires_disability_review"):
        return "disability"        # → disability_rag → disability_calc → payment_calc
    return "payment_calc"          # 장해 건너뛰고 직행
```

`requires_disability_review`는 `diagnosis` 노드에서 **LLM이 판단**한다 (`agents.py:151`).
진단서에 "영구 후유장해가 예상되며 장해지급률 평가가 필요" 같은 소견이 있으면 True.

> **라우팅 불변식** (`scripts/battery.py:212`가 검증): `requires_disability_review`가 True면
> 반드시 장해 분기가 실행돼 `disability_analysis`에 결과를 남겨야 한다.

---

## 2. disability_rag 노드 — 분류·지급률 추출

`agents.py:382`. 4단계로 진행한다.

### 2-1. 쿼리 구성 + 1차 검색 (가입 약관)

```python
query = f"{dx_name} {icd} 후유장해 장해분류표 지급률"
res = await hybrid.search(query, namespaces=["terms"], top_k=8,
                          insurer=ci.get("insurer"), product=ci.get("product_name"))
sched = _select_schedule(res.get("ranked_chunks", []))
```

먼저 **가입 약관(`terms`)** 안에 장해분류표가 있는지 찾는다. `_select_schedule` (`agents.py:292`)가
`chunk_type == "schedule"`인 청크를 우선 고르고, 없으면 "장해의 분류" 헤더 휴리스틱으로 폴백.

### 2-2. 폴백 — 표준 장해분류표(level)

가입 약관에 분류표가 없으면(대부분의 경우) **금감원 표준 장해분류표**로 재검색:

```python
if not sched:
    res = await hybrid.search(query, namespaces=["level"], top_k=8,
                              contract_date=enrolled_at)     # ★ 계약일 전달
    sched = res.get("ranked_chunks", [])
    is_fallback = bool(sched)
```

- `namespaces=["level"]` → `schedule_chunks` 테이블 검색 (금감원 시행세칙 원문).
- `contract_date=enrolled_at` → **계약 체결일이 속하는 개정판만** 반환 (버전 매칭).
  `enrolled_at`은 `user_insurances.enrolled_at`에서 온다 (`load_context`, `agents.py:109`).
  버전 필터 상세는 아래 §5, 데이터 출처는 [schedule-data.md](./schedule-data.md).

### 2-3. 둘 다 비면 — 빈 결과 + 마커

`terms`·`level` 모두 실패하면 기존 동작 유지 (`agents.py:409`):
`disability_analysis`를 `combined_rate: 0.0`, `confidence: "low"`, `caveat: "장해분류표 미검색"`로
두고 `errors`에 `disability_schedule_missing` 기록.

### 2-4. LLM 추출 + 결정론 백스톱 (`_extract_schedule_items`, `agents.py:300`)

찾은 장해분류표 원문을 LLM에 주고 항목을 추출한다. **핵심은 지급률 검증.**

LLM에 요구하는 항목별 필드:
```
injury          부상/장해명
body_region     신체부위 (눈·귀·코·씹기말하기·척추·체간골·팔·다리·손가락·발가락·흉복부장기·신경계정신)
category_label  원문 항목 텍스트 그대로 복사
rate            지급률 % (number)
rate_quote      rate 숫자가 등장한 원문 구절 그대로 복사
temporary       한시장해 여부 (bool)
temporary_years 존속기간 (number|null)
citation        원문 source_ref
```

**결정론 백스톱** (`agents.py:351`) — LLM이 지어낸 지급률을 거르는 장치:
```python
verified = bool(quote) and (str(int(rate_f)) in sched_text)
```
지급률 숫자가 실제 원문(`sched_text`)에 존재해야 `verified=True`. 미검증 항목은
`rule_notes`에 "미검증 지급률 제외"로 남기고 합산에서 뺀다.

> ⚠️ **이 백스톱은 현재 헐겁다 — [known-issues.md](./known-issues.md) 버그 3 참조.**
> (1) `rate_quote`가 원문에 있는지 검사하지 않고 비었는지만 봄. (2) `int()`가 소수를 버리고
> 부분 문자열 매칭이라 "제12조"·"12개월"에도 걸림. 두 자리 숫자면 사실상 무조건 통과.

**신뢰도 산정** (`agents.py:370`):
- `high`: 모든 항목이 verified + LLM이 uncertain=false
- `medium`: 일부만 verified
- `low`: verified 0건

### 2-5. 폴백 시 신뢰도 캡 + 캐비앗

표준표 폴백이면 (`agents.py:427`):
- `caveat = "표준 장해분류표 기준(가입 약관 미확보) — 개별 약관 확인 필요"`
- `enrolled_at`이 None이면 `" · 가입일 미상 — 현행판 기준"` 추가
- **신뢰도 high → medium으로 캡** (표준표는 개별 약관과 다를 수 있으므로)
- `errors`에 `disability_fallback_standard_schedule` 마커 (운영 추적용)

---

## 3. disability_calc 노드 — 결정론 합산

`agents.py:456`. 단순하다 — verified 항목만 골라 순수 함수에 넘긴다:

```python
da = state.get("disability_analysis", {})
verified = [i for i in da.get("items", []) if i.get("verified")]
result = combine_disability_rate(verified)
# → disability_analysis에 combined_rate, normalized_items, rule_notes 병합
```

**verified=True인 항목만 합산에 산입**한다. §2-4의 백스톱을 통과하지 못한 지급률은 여기서 배제된다.

---

## 4. combine_disability_rate — 합산 규칙 (`disability_rules.py:36`)

MVP 4대 규칙. **각 규칙이 금감원 총칙 원문과 어떻게 대응하는지** 함께 표기한다
(원문: `tempVectorDB/schedule_data/b3_rev2018.txt`).

### 규칙 1 — 한시장해 환산 (`disability_rules.py:23`)

> **총칙 1-4항 원문:** "영구히 고정된 증상은 아니지만 치료 종결 후 한시적으로 나타나는 장해에
> 대하여는 그 기간이 **5년 이상인 경우 해당 장해지급률의 20%**를 장해지급률로 한다."

- `temporary=True` + `temporary_years >= 5` → 지급률 × 0.20
- `temporary_years < 5` 또는 미상 → **미산입(0%)**
- 상수: `_TEMPORARY_MIN_YEARS = 5.0`, `_TEMPORARY_FACTOR = 0.20`

### 규칙 2 — 동일 신체부위는 최고값만 (`disability_rules.py:60`)

> **총칙 3-2항 원문:** "동일한 신체부위에 2가지 이상의 장해가 발생한 경우에는 **합산하지 않고
> 그중 높은 지급률**을 적용함을 원칙으로 한다."

- `body_region`으로 그룹핑 → 각 그룹에서 `effective_rate` 최고값만 인정
- `rule_notes`에 "동일부위(X) N건 중 최고 Y%만 인정" 기록

### 규칙 3 — 서로 다른 부위는 합산 (`disability_rules.py:72`)

> **총칙 2항 원문:** 13개 신체부위 정의 + "다만, **좌·우의 눈, 귀, 팔, 다리, 손가락, 발가락은
> 각각 다른 신체부위로 본다.**"

- 부위별 최고값들을 **합산**
- `rule_notes`에 "서로 다른 N개 부위 합산 = Z%" 기록

> ⚠️ **좌우 구분 미구현 — [known-issues.md](./known-issues.md) 버그 1.** 원문은 좌팔/우팔을
> 다른 부위로 보지만 `body_region`에 좌우 개념이 없어 같은 "팔"로 묶여 **과소산정**된다.

### 규칙 4 — 상한 100% (`disability_rules.py:78`)

- `combined > 100` → 100으로 캡, `rule_notes`에 기록
- 상수: `_MAX_RATE = 100.0`

### 반환 형태

```python
{"combined_rate": float, "rule_notes": list[str], "normalized_items": list[dict]}
```

### 미구현 규칙 (코드 주석에 "후속 단계"로 명시)

> **총칙 3-3항 (파생장해):** "하나의 장해로 둘 이상의 파생장해가 발생하는 경우 각 파생장해의
> 지급률을 합산한 지급률과 최초 장해의 지급률을 비교하여 그 중 높은 지급률을 적용."

기존장해 공제·파생장해 세부·부위별 상한도 미구현. [known-issues.md](./known-issues.md) 참조.

### 단위테스트 (`tests/test_disability_combine.py`)

6개 케이스로 규칙 1~4 커버 (부위 합산·동일부위 흡수·상한·한시장해 5년/미만·빈 입력).
**LLM/DB 없이 순수 함수만** 테스트하므로 Docker 없이도 통과한다.

---

## 5. 계약일 → 개정판 버전 매칭 (`src/rag/search.py:95`)

표준 장해분류표는 개정판마다 지급률이 다를 수 있어, **계약 체결일이 속하는 판**만 검색해야 한다.

```python
def _version_filter(namespace, args, contract_date):
    if namespace not in _VERSION_FILTER_NS:      # level namespace에만 적용
        return ""
    if contract_date is None:
        return " AND applies_to IS NULL"          # 현행판만
    args.append(contract_date)
    idx = len(args)
    return f" AND applies_from <= ${idx} AND (applies_to IS NULL OR ${idx} < applies_to)"
```

**반열림 구간 `[applies_from, applies_to)`.** `applies_to`가 exclusive라 3개 개정판이
겹침·빈틈 없이 이어진다:

| 개정판 | 적용 구간 |
|--------|-----------|
| 2005 계열 | `[2005-04-01, 2018-04-01)` |
| 2018.4 개정 | `[2018-04-01, 2025-06-30)` |
| 2025.6 개정(현행) | `[2025-06-30, NULL)` |

예: 계약일 2020-05-01 → 2018.4 판. 계약일 2018-04-01(경계) → 2018.4 판(2005의 `< 2018-04-01`은
배제, 2018의 `applies_from <= 2018-04-01`은 포함).

> ⚠️ **`enrolled_at`이 NULL이면** `contract_date=None`이 돼 **조용히 현행판(2025.6)**으로 검색.
> 구계약인데 가입일이 안 잡히면 잘못된 버전 지급률이 나온다. 캐비앗으로 고지는 되지만 값은 틀림.
> [known-issues.md](./known-issues.md) 참조.

---

## 6. 하류 소비 — 어떻게 리포트에 반영되나

### payment_calc (`agents.py:472`)

장해지급률을 보상 추정 범위 산정에 반영:
```python
rate = float((state.get("disability_analysis") or {}).get("combined_rate") or 0.0)
factor_hi = 1.0 + min(rate, 100.0)/100.0 * 0.8 if rate > 0 else 1.8
# 0%→×1.0, 100%→×1.8. 가입금액 미보유 → 절대 보험금 불가, 모두 '추정'
```

### report_compose (`agents.py:492`)

장해 결과를 리포트 섹션 `5b_장해지급률`에 서술:
```
추정 합산 장해지급률 {combined_rate}% (신뢰도 {confidence}, 근거 {citations}) —
규칙 {rule_notes}. ※ {caveat}
```
`items`가 없으면 "해당 없음(후유장해 미검토)".

### persist (`agents.py:590`)

`disability_analysis` 전체를 `report_drafts.draft.disability`에 JSON으로 보존.
장해 인용(`da_cites`)은 약관 인용·판례 근거와 합쳐 `basis_terms_precedents`에 저장.

---

## 7. 한눈 요약

```
requires_disability_review (LLM 판단)
   │ True
   ▼
disability_rag
   ├─ terms 검색 → 장해분류표 있나?
   │     └─ 없으면 level(표준표) 폴백, 계약일로 개정판 매칭
   ├─ LLM 추출 (injury/body_region/rate/rate_quote/...)
   ├─ 결정론 백스톱: rate가 원문에 있나? → verified
   └─ 신뢰도 산정 (+ 폴백 시 medium 캡)
   ▼
disability_calc
   └─ verified 항목만 → combine_disability_rate
         규칙1 한시장해 20%/미산입
         규칙2 동일부위 최고값
         규칙3 부위 간 합산
         규칙4 상한 100%
   ▼
combined_rate → payment_calc(보상범위) → report_compose(5b섹션) → persist(draft.disability)
```

**돈에 직결되는 최종 숫자(`combined_rate`)는 검증 가능한 순수 함수가 만든다.** LLM은 분류까지만.
단, 현재 백스톱·좌우 구분에 결함이 있어 [known-issues.md](./known-issues.md)의 버그 1·3을
우선 수정해야 신뢰할 수 있다.


---

# 표준 장해분류표 데이터 — 출처·추출·버전·검증

> 관련: `tempVectorDB/schedule_data/`(원본·추출), `tempVectorDB/schedule/`(적재 md),
> `tempVectorDB/load_schedule.py`, `tempVectorDB/schedule_manifest.yaml`,
> `migrations/005_schedule_chunks.sql`
> 상위 문서: [README.md](./README.md) · 소비 흐름: [disability-pipeline.md](./disability-pipeline.md)

이 문서는 후유장해 지급률의 **근거가 되는 원문 데이터가 무엇에 기반하고, 어떻게 가공돼
DB에 들어가는가**를 설명한다. 아래 내용은 원본 파일·추출 스크립트와 직접 대조해 확인했다.

---

## 1. 무엇을 기반으로 하나 — 1차 원본

**대상:** 금융감독원 「보험업감독업무시행세칙」 별표15(표준약관) `<부표 3>` 장해분류표(생명보험).

1차 원본이 `tempVectorDB/schedule_data/raw/`에 **실물로** 보관돼 있다:

| 파일 | 내용 |
|------|------|
| `byl15_pre2018_20180131.hwp` | 2005 계열 (2018.3.2 개정 직전판) |
| `byl15_rev2018_20180302.hwp` | 2018.3.2 대개정판 |
| `byl15_rev2019_20200101.hwp` | 2019.12.20 개정판 |
| `byl15_standard_terms_current.pdf` | 현행 PDF (2025.6 대조용) |
| `kiri_2017_revision.pdf` | KIRI 2017 신구대조 (개정 내용 교차 확인용) |

→ 추측·창작이 아니라 **법령 원문**에 기반한다.

---

## 2. 어떻게 가공하나 — 추출 파이프라인

`schedule_data/VERIFICATION.md`에 문서화된 경로. 각 단계 스크립트가 레포에 남아 있어 재현 가능:

```
HWP 원본
  └─(hwp5html, 표 구조 보존)─► XHTML
        └─ extract_buhyo3.py ──► b3_*.txt      (부표3 구간만 덤프)
              └─ parse_schedule.py ──► parsed_*.json  (항목·지급률 페어링 + 개수 검증)
                    └─ generate_md.py ──► schedule/*/disability_schedule.md  (최종 적재 md)
```

- `extract_buhyo3.py` — XHTML에서 부표3 장해분류표 구간만 잘라낸다.
- `parse_schedule.py` — 표의 항목과 지급률을 페어링하고, **항목수 = 지급률수가 일치할 때만**
  위치기준으로 매칭한다. 불일치 시 TODO로 남긴다 (병합셀 오정렬 방지).
- `generate_md.py` — 파서용 마크다운 생성. 2025.6 판은 rev2019에 '간질→뇌전증' 치환만 적용
  (치환 목록 = `EPILEPSY_SUBS`).

> **원칙 (VERIFICATION.md):** 지급률은 원문 그대로. 원문에 없는 값은 만들지 않는다.

---

## 3. 왜 3개 버전인가 — dedup 근거

개정 이력은 여럿이지만 **지급률 표가 실제로 바뀐 판**만 남겨 3벌로 정리했다.

| 버전 | 시행 구간 | 근거 |
|------|-----------|------|
| **2005 계열** | 2005-04-01 ~ 2018-04-01 | 2018.3.2 개정 직전판 |
| **2018.4 개정** | 2018-04-01 ~ 2025-06-30 | 2018.3.2 대개정. 2019.12.20 개정은 표 무변경이라 합본 |
| **2025.6 개정(현행)** | 2025-06-30 ~ | '간질'→'뇌전증' 명칭 개정만, 지급률 무변경 |

### dedup 판단의 독립 재검증 (2026-07-15 수행)

문서 주장을 그대로 믿지 않고 원문 diff로 재확인했다:

- **부표3 ≡ 부표9:** rev2018의 생보 부표3와 질병·상해 부표9의 장해분류 표 행이 정규화 후 완전
  일치 → 생보 부표3만 사용.
- **rev2018 vs rev2019:** 원문 diff 결과 실질 차이가 **글리프(`․`↔`ㆍ`)와 고시명 개정
  (`장애등급판정기준→장애정도판정기준`, `능력장해→능력장애`)뿐**임을 확인. 지급률 표 완전 동일
  → 표 기준 합본이 타당. ✓ 재검증 통과
- **rev2019 vs 현행:** 13개 부위 지급률 시퀀스 전부 일치. 유일 차이는 신경계·정신행동의
  간질→뇌전증 명칭 개정(지급률 70/40/10 불변) → 현행 용어 보존 위해 별도 버전.

---

## 4. 버전별 부위/항목 수 매트릭스

| 신체부위 | 2005 | 2018.4=2025.6 | 2018 개정 변화 |
|----------|:---:|:---:|----------------|
| 눈 | 10 | 10 | - |
| 귀 | 6 | **7** | 평형기능 장해 신설(+1) |
| 코 | 1 | **2** | 후각기능 신설(+1) |
| 씹어먹거나 말하는 장해 | 9 | **10** | 말하는 기능 심한 60% 신설 등(+1) |
| 외모 | 2 | 2 | - |
| 척추 | 9 | 9 | 추간판탈출증 재정의(지급률 동일) |
| 체간골 | 2 | 2 | 명칭 상세화 |
| 팔 | 9 | 9 | - |
| 다리 | 12 | 12 | "짧아진"→"짧아지거나 길어진" |
| 손가락 | 6 | 6 | - |
| 발가락 | 7 | 7 | - |
| 흉·복부장기 및 비뇨생식기 | 3 | **5** | 심장기능 상실 100% 신설 등(+2) |
| 신경계·정신행동 | 11 | **13** | 정신행동 지급률 개편·치매·뇌전증 항목화(+2) |
| **합계** | **87** | **94** | |

- 청크 수(파서): 각 버전 **14개**(총칙 + 13부위). 3버전 합 **42청크**.
- 페어링 검증: **39개 표(13부위×3판) 전부 항목수=지급률수 일치**, mismatch 0.

---

## 5. 적재 md 구조 (실제 DB에 들어가는 것)

`tempVectorDB/schedule/{2005,2018_04,2025_06}/disability_schedule.md`. **각 14개 `## 부위`
헤더**로 구성:

```
## 총칙
## 눈
## 귀
## 코
## 씹어먹거나 말하는 장해
## 외모                       ← 존재함 (프롬프트엔 누락, known-issues 버그2)
## 척추
## 체간골
## 팔
## 다리
## 손가락
## 발가락
## 흉복부장기 및 비뇨생식기
## 신경계·정신행동
```

각 부위 섹션은 마크다운 표(`| 장해의 분류 | 지급률 |`) + 판정기준 본문으로 구성. 지급률은
**bare number**(예: `| 10) 한 다리가 5cm 이상 짧아지거나 길어진 때 | 30 |`).

총칙에는 좌우 구분 규칙·13부위 정의·한시장해·파생장해 규칙이 원문 그대로 보존돼 있다
(2026-07-15 확인). 즉 **좌우·외모·파생은 데이터에 다 있고, 못 쓰는 건 소비 계층(계산)이다.**

---

## 6. 적재 로직 (`load_schedule.py`)

### 청킹 전략 (`load_schedule.py:66`)

- `## 부위` 헤더 경계로 분할 → `(body_part, section_body)` 튜플.
- **표 행 그룹은 한 청크에 통째로 유지** — 등급·지급률 행이 청크 경계로 잘리지 않게.
- 헤더 앞 서문(전문)은 무시.

### 컬럼 생성 (`load_schedule.py:120`)

| 컬럼 | 값 |
|------|------|
| `chunk_id` | `{doc_hash[:16]}:{idx}` |
| `content` | `## {부위}\n{본문}` (임베딩 원문) |
| `content_tokens` | Kiwi 형태소 (tsvector 전문검색용) |
| `embedding` | qwen3-embedding **1024d** (약관·판례와 동일 벡터공간) |
| `chunk_type` | `"schedule"` 고정 |
| `doc_hash` | 버전+부위+본문 sha256 (재적재 중복 방지) |
| `body_part` | 부위 라벨 |
| `rate` | `_single_rate` — 섹션에 지급률(%)이 **정확히 하나**일 때만 채움, 아니면 NULL |
| `version_label`·`applies_from`·`applies_to` | 매니페스트에서 (버전 매칭 메타) |
| `source_url`·`section`·`chunk_index` | 인용·복원 메타 |

### 매니페스트 (`schedule_manifest.yaml`)

버전별 `version_label`·`applies_from`·`applies_to`(exclusive)·`source_url`·md 파일 경로를 정의.
`applies_to`가 exclusive라 [disability-pipeline.md §5](./disability-pipeline.md)의 반열림 버전
필터와 정합한다.

### 스키마 (`migrations/005_schedule_chunks.sql`)

`chunk_id` PK, `embedding halfvec(1024)`, `chunk_type`·`body_part`·`version_label`·
`applies_from` NOT NULL, `applies_to` nullable(NULL=현행판). 약관(`policy_chunks`)·
판례(`case_chunks`)와 동일한 임베딩 차원·chunk_type 체계로 벡터공간을 맞췄다.

### 적재 실행

```bash
python tempVectorDB/load_schedule.py --manifest tempVectorDB/schedule_manifest.yaml
```
전제: PostgreSQL(+pgvector) + Ollama(임베딩) 기동. `ON CONFLICT (chunk_id) DO NOTHING`으로
멱등.

---

## 7. 검증 현황 — 무엇이 확인됐고 무엇이 남았나

### 확인 완료 (VERIFICATION.md + 2026-07-15 독립 재검증)

- ✅ 1차 원본(HWP 3판 + PDF + KIRI) 실물 보관
- ✅ 추출 파이프라인 스크립트 전부 재현 가능
- ✅ 3벌 dedup 근거 (rev2018↔rev2019 글리프/명칭뿐, 현행↔간질/뇌전증뿐) — diff로 재확인
- ✅ 부표3≡부표9
- ✅ 39개 표 페어링 항목수=지급률수 일치
- ✅ 무작위 10항목 md값 vs 원문 대조 10/10 일치
- ✅ 적재 md 14청크 정합, 총칙 좌우/외모/파생 규칙 보존
- ✅ 버전 필터 ↔ 매니페스트 exclusive 경계 정합

### 남은 데이터 TODO (VERIFICATION.md가 스스로 표시)

1. **[중요] 2025.6 실제 시행일 확정** — 매니페스트 `applies_from: 2025-06-30`은 개정일 기준.
   부칙상 실제 시행일 미확인. 경계 근처(2025년 6월) 계약 버전 오매칭 가능.
2. **[중요] 2025.6 판 원문 재대조** — 현행 PDF 공백 소실로, 2025.6 md는 rev2019에 검증된
   간질→뇌전증 치환만 적용해 생성. 지급률 무변경은 확인했으나 현행 HWP 원본 확보 시
   판정기준 문장부호·띄어쓰기 1회 재대조 권장.
3. **source_url 정밀화** — 현재 law.go.kr 일반 URL. admRulSeq/flSeq(개정판 직접 링크)로
   교체하면 인용 역추적 정확도 향상.
4. **판정기준 보조표** — 귀 평형기능 배점표·ADLs 제한 장해평가표가 셀 병합으로 일부 행이
   뭉쳐 있을 수 있음. 계산이 ADLs 세부배점을 파싱하려면 별도 정형화 필요.
5. **[경미] 2005판 총칙 원문자 누락** — HWP 추출 과정에 ⑩⑪⑫⑬ 글리프 누락(부위 명칭 텍스트·
   지급률은 보존, 계산 무영향). 인용 정밀화 시 해당 문장만 보정.
6. **2019.12.20 판정기준 문구 분리 여부** — 2018.4 판에 개정 후 문구를 합본. 개정 전 계약에
   개정 전 문구가 인용상 필요하면 하위판 분리 검토(지급률·계산 영향 없음).

이 TODO들은 **지급률 값 자체에는 영향이 없고**(무변경 확인됨), 시행일 경계·인용 정밀도·문장부호
수준의 정합성 과제다. 상세는 [known-issues.md](./known-issues.md).


---

# 알려진 이슈 — 버그·미구현·TODO

> 점검일: 2026-07-15 (브랜치 `11-feature-langgraph-멀티에이전트-구현`)
> 상위 문서: [README.md](./README.md) · [disability-pipeline.md](./disability-pipeline.md) ·
> [schedule-data.md](./schedule-data.md)

원본 코드·원문 데이터와 직접 대조해 확인한 결함 목록. **우선순위는 "사용자 돈에 직접 영향 +
틀린 걸 맞다고 말하는 정도" 기준.**

---

## 우선순위 요약

| # | 이슈 | 심각도 | 영향 | 위치 |
|---|------|:---:|------|------|
| 1 | 좌우 신체부위 미구분 → 과소산정 | 🔴 높음 | 지급률이 실제보다 낮게 | `agents.py:327` + `disability_rules.py:57` |
| 2 | 검증 백스톱 무력화 → 환각 통과 | 🔴 높음 | 없는 지급률이 verified로 | `agents.py:351` |
| 3 | 신체부위 목록에 "외모" 누락 | 🟡 중간 | 흉터·추상 장해 오분류 | `agents.py:327` |
| 4 | `enrolled_at` NULL → 조용히 현행판 | 🟡 중간 | 구계약 버전 오매칭 | `agents.py:404` |
| 5 | 파생장해 규칙 미구현 | 🟡 중간 | 총칙 3-3항 미반영 | `disability_rules.py` |
| 6 | test_config가 로컬 `.env` 오염 | 🟢 낮음 | 로컬서 테스트 1건 상시 실패 | `tests/test_config.py:21` |
| 7 | ruff가 `scripts/`·`tempVectorDB/`서 실패 | 🟢 낮음 | CI 게이트 불가 | (린트 설정) |

---

## 버그 1 — 좌우 신체부위 미구분 (과소산정) 🔴

### 증상
좌팔 30% + 우팔 30% 사고가 `combine_disability_rate`에서 같은 `"팔"` 그룹으로 묶여
**최고값 30%만 인정**된다. 총칙대로면 다른 신체부위라 합산 60%.

### 원문 근거
> **총칙 2항:** "다만, **좌·우의 눈, 귀, 팔, 다리, 손가락, 발가락은 각각 다른 신체부위로 본다.**"

이 규칙은 적재 md 총칙에 그대로 보존돼 있다([schedule-data.md §5](./schedule-data.md)). **데이터엔
있는데 소비 계층이 못 쓴다.**

### 원인
- `agents.py:327` LLM 프롬프트의 `body_region`이 좌우 개념 없는 단일 라벨.
- `disability_rules.py:57` region 그룹핑이 그 라벨을 그대로 써서 좌우를 합쳐버림.

### 왜 나쁜가
이 서비스는 "보험금이 적게 나온 것 같다"는 사용자를 돕는 게 목적인데, 엔진이 **과소산정 방향**으로
틀린다. 양팔·양다리 사고는 교통사고에서 드물지 않다.

### 수정 방향
LLM 항목에 `laterality`(left|right|none) 필드 추가 → region 키를 `f"{body_region}:{laterality}"`
로 구성(좌우 구분 대상 6부위만). 회귀 테스트 추가.

---

## 버그 2 — 검증 백스톱 무력화 (환각 통과) 🔴

### 증상
LLM이 지어낸 지급률이 `verified=True`를 달고 합산에 산입될 수 있다. 신뢰도도 `high`로 뜬다.

### 원인 (`agents.py:351`)
```python
verified = bool(quote) and (str(int(rate_f)) in sched_text)
```
두 군데가 샌다:
1. **`rate_quote` 미검증** — "이 숫자가 등장한 원문 구절을 복사하라"고 받은 값인데, 코드는
   `bool(quote)`(비었는지)만 본다. `quote in sched_text`인지 확인 안 함 → 통째로 지어내도 통과.
2. **숫자 검사 헐거움** — `int(rate_f)`가 소수를 버려(12.5%→"12") 원문 전체에서 부분 문자열로
   찾는다. 원문엔 "제12조"·"12개월"·"120일"이 널려 있어 **두 자리 숫자면 사실상 무조건 매칭**.

### 왜 제일 위험한가
버그 1은 값이 틀리는 거지만, 이건 **틀린 걸 자신 있게(high confidence) 맞다고 말한다.**
"표에 없는 지급률은 절대 만들지 마라"는 설계 의도가 코드에서 안 지켜진다.

### 수정 방향
```python
quote_norm = re.sub(r"\s+", "", quote)
sched_norm = re.sub(r"\s+", "", sched_text)
rate_str = f"{rate_f:g}"  # 소수 보존
verified = bool(quote_norm) and quote_norm in sched_norm and rate_str in quote_norm
```
`_extract_schedule_items`의 검증 로직에 대한 단위테스트도 신설(현재 0개).

---

## 버그 3 — 신체부위 목록에 "외모" 누락 🟡

### 증상
`agents.py:327` LLM 프롬프트의 허용 `body_region`이 12개뿐 — **⑤외모**(흉터·추상 장해)가 빠짐.
화상·얼굴 흉터 케이스에서 부위를 `"기타"`로 떨구거나 엉뚱한 부위에 우겨넣음.

### 원문 근거
총칙 2항은 13개 부위이고 ⑤가 외모. 적재 md에도 `## 외모` 섹션이 실재
([schedule-data.md §5](./schedule-data.md)).

### 수정 방향
프롬프트 목록에 "외모" 추가(13개로). `disability_rules.py`는 라벨 무관하게 동작하므로 그대로.

---

## 버그 4 — `enrolled_at` NULL → 조용히 현행판 🟡

### 증상
`user_insurances.enrolled_at`이 NULL이면 `disability_rag`가 `contract_date=None`으로 검색
(`agents.py:404`) → 버전 필터가 **현행판(2025.6)만** 반환(`search.py:112`). 구계약인데 가입일이
안 잡히면 잘못된 개정판 지급률이 나온다.

### 완화 상황
`_STANDARD_NO_DATE_CAVEAT = "가입일 미상 — 현행판 기준"`으로 **고지는 된다.** 하지만 값 자체는
틀릴 수 있다(정신행동 2005=70 vs 2018=75, 귀 평형기능·흉복부 심장 등 버전 간 실차이 존재).

### 수정 방향
`enrolled_at` NULL을 명시적 에러/경고 마커로 승격하거나, 가입일 확보 전엔 장해 지급률을
"판정 보류"로 표기. 제품 정책 결정 필요.

---

## 버그 5 — 파생장해 규칙 미구현 🟡

### 내용
`disability_rules.py` 주석에 "후속 단계"로 명시된 알려진 부채:
> **총칙 3-3항:** "하나의 장해로 둘 이상의 파생장해가 발생하는 경우 각 파생장해의 지급률을
> 합산한 지급률과 최초 장해의 지급률을 비교하여 그 중 높은 지급률을 적용."

기존장해 공제·파생장해 세부·부위별 상한도 미구현.

### 상태
알려진 MVP 스코프 밖. 버그 1·2보다 발생 빈도 낮아 후순위. **별도 이슈로 관리 권장.**

---

## 버그 6 — test_config가 로컬 `.env` 오염 🟢

### 증상
`tests/test_config.py::test_model_names_default_empty_to_force_injection`이 로컬에서 상시 실패:
```
assert cfg.llm_model == ""
AssertionError: assert 'qwen3:8b' == ''
```

### 원인
`Settings`가 `env_file=".env"`(`config.py:23`)라 테스트가 `Settings()`를 부르면 로컬 `.env`를
읽어버린다. `.env`에 `LLM_MODEL=qwen3:8b`가 있어 "기본값 빈 문자열" 단언이 성립 불가.
→ **CI(`. env` 없음)에서만 통과, 개발자 로컬에선 항상 실패.** 방치하면 "원래 하나는 빨간색"이
돼 진짜 회귀를 놓친다.

### 수정 방향
`Settings(_env_file=None)`로 파일 로딩 끄고 `monkeypatch.delenv`로 프로세스 환경변수까지 격리.

---

## 버그 7 — ruff가 `scripts/`·`tempVectorDB/`에서 실패 🟢

### 증상
`ruff check .` → 31건 에러 + 포맷 2건. 단, `ruff check src tests migrations`는 **All checks
passed.** 걸린 건 전부 실험 스크립트·데이터 적재 코드(E501 긴 줄, SIM115 컨텍스트 매니저,
S101 assert, import 정렬 등).

### 영향
기능 무관하지만 `ruff check .`가 상시 빨간불이라 커밋 훅·CI 게이트로 못 쓴다.

### 수정 방향
둘 중 택1 — (a) `pyproject.toml`에서 `scripts/`·`tempVectorDB/` 제외, (b) 실제 정리.
일회성 ingest 도구 성격상 (a)가 현실적.

---

## 검증 못 한 범위 (환경 제약)

2026-07-15 점검 시 **Docker 미기동**으로 Kafka·PostgreSQL·Ollama 필요 경로는 검증 불가:
- RAG 실검색(terms/case/level namespace)
- 리포트 워커 E2E (`scripts/battery.py`, `scripts/kafka_smoke.py`)
- `load_schedule.py` 실적재

현재 초록불인 것은 **유닛 테스트 범위까지**다(70/71 통과, 실패 1건은 버그 6). 통합 검증은
`docker compose up -d` 후 `PYTHONPATH=src python scripts/battery.py`로 별도 수행 필요.

---

## 데이터 TODO (상세는 schedule-data.md §7)

- 2025.6 실제 시행일 확정 (매니페스트 잠정값)
- 2025.6 판 현행 HWP 재대조 (판정기준 문장부호)
- source_url 정밀화 (admRulSeq/flSeq)
- ADLs 보조표 정형화 (계산이 세부배점 파싱 시)

이 데이터 TODO들은 **지급률 값에는 영향 없음**(무변경 확인). 시행일 경계·인용 정밀도 과제.

---

## 권장 처리 순서

1. **버그 1(좌우) + 버그 2(백스톱)** — 사용자 돈에 직접 영향. 데이터가 이미 준비돼 있어
   원문 근거를 그대로 붙여 수정 가능. 회귀 테스트 필수.
2. **버그 3(외모)** — 프롬프트 한 줄. 함께 처리.
3. **버그 6(test_config)** — 테스트 격리. "상시 빨간불" 제거해 회귀 감지 복구.
4. **버그 7(ruff scope)** — CI 게이트 확보.
5. **버그 4(enrolled_at)·버그 5(파생장해)** — 제품 정책 결정 후 별도 이슈.
