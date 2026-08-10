# 리포트 워커 LangGraph — 그래프 구조 · 14개 노드 · 상태 전이

> 출처: AI 엔진 아키텍처 문서 세트 · 최종 점검일 2026-07-15 · 브랜치 `11-feature-langgraph-멀티에이전트-구현`
> 상위: [README](./README.md) · 원본 코드 정독 + 적대적 교차검증(코드 재대조) 완료

> 🎯 **한 문장 요약**
> 리포트 워커는 보험 청구 한 건이 들어오면, 정해진 순서대로 14개의 작업 단계를 자동으로 거치게 해서 손해사정 리포트 초안을 만들어 주는 프로그램이다.

> 🌱 **쉽게 말하면**
> 이 문서는 "보험 리포트 자동 작성 공장"의 컨베이어 벨트 설계도다. 서류(사고 내용, 진단서 등) 한 뭉치가 벨트 입구에 올라오면, 벨트를 따라 14개의 작업대를 차례로 지나간다. 각 작업대는 한 가지 일만 한다 — 예를 들어 "진단서에서 병명 뽑기", "약관에서 관련 조항 찾기", "예상 보험금 범위 계산하기" 같은 식이다. 작업대 사이에는 갈림길(조건분기)도 있어서, 상황에 따라 어떤 작업대는 건너뛴다. 예를 들어 후유장해를 따질 필요가 없으면 장해 계산 작업대들은 그냥 지나친다. 이 컨베이어 벨트 전체를 짜고 연결하는 도구가 바로 **LangGraph**(작업 단계들을 그래프 형태로 이어 붙여 실행 순서를 관리하는 라이브러리)다. 마지막 작업대까지 무사히 지나가면 완성된 리포트가 데이터베이스에 저장된다.

리포트 워커는 `report-job` Kafka(작업 메시지를 주고받는 우체통 같은 큐) 메시지를 받아, LangGraph의 `StateGraph`(상태를 공유하며 단계들을 잇는 그래프) 파이프라인에 태워 보험 손해사정 리포트 초안을 만든다. 이 문서는 그래프를 어떻게 조립하는지(`graph.py`), 단계들이 공유하는 전역 상태는 무엇인지(`state.py`), 워커가 어떻게 시작되고 실행되는지(`worker.py`·`__main__.py`), 14개 노드(작업대)와 3개 라우터(갈림길 판단 함수)는 무엇인지(`nodes/agents.py`), 그리고 장해지급률을 계산기처럼 정해진 규칙으로 합산하는 부분(`disability_rules.py`)까지, 실제 코드 위치와 함께 하나씩 설명한다.

---

## 1. 그래프 조립 (`build_graph()`)

`build_graph()`는 컨베이어 벨트를 짜는 함수다. `ReportState`를 상태 타입으로 하는 `StateGraph`를 하나 만들고, 14개 노드(작업대)를 등록한 뒤, 노드들을 엣지(연결선)·조건분기(갈림길)로 이어서 **컴파일된**(바로 실행할 수 있게 완성된) 그래프를 돌려준다 (`graph.py:14-72`). 상태 타입은 `StateGraph(ReportState)`로 지정한다 (`graph.py:15`).

> 쉽게 말하면: 여기서 "상태(state)"란 벨트를 따라 함께 흘러가는 **공용 서류철**이다. 각 작업대는 이 서류철을 열어 필요한 걸 읽고, 자기 결과를 끼워 넣는다.

### 1-1. 노드 등록 (`graph.py:17-30`)

```python
g.add_node("load_context", agents.load_context)
g.add_node("input_guardrail", agents.input_guardrail)
g.add_node("diagnosis", agents.diagnosis)
g.add_node("terms_parse", agents.terms_parse)
g.add_node("coverage_parse", agents.coverage_parse)
g.add_node("coverage_analysis", agents.coverage_analysis)
g.add_node("case_search", agents.case_search)
g.add_node("disability_rag", agents.disability_rag)
g.add_node("disability_calc", agents.disability_calc)
g.add_node("payment_calc", agents.payment_calc)
g.add_node("report_compose", agents.report_compose)
g.add_node("output_guardrail", agents.output_guardrail)
g.add_node("persist", agents.persist)
g.add_node("persist_blocked", agents.persist_blocked)
```

작업대(노드)는 모두 14개다. 갈림길을 판단하는 라우터 함수 3개(`route_after_input`, `policy_in_db`, `route_after_case`)는 **노드로 등록하지 않는다.** 이들은 `add_conditional_edges`에서 "어느 길로 갈지" 정하는 판단 함수로만 쓰인다 (`graph.py:39,48,60`).

> 쉽게 말하면: 라우터는 작업대가 아니라, 갈림길에 서서 "이쪽으로 가세요"라고 방향만 알려주는 안내판이다.

### 1-2. 고정 엣지 (`graph.py:32-70`)

엣지는 "이 작업대가 끝나면 무조건 저 작업대로 간다"는 고정 연결선이다.

- `START → load_context` (`graph.py:32`)
- `load_context → input_guardrail` (`graph.py:33`)
- `terms_parse → coverage_parse` (`graph.py:51`)
- `coverage_parse → coverage_analysis` (`graph.py:53`)
- `coverage_analysis → case_search` (`graph.py:54`)
- `disability_rag → disability_calc` (`graph.py:63`)
- `disability_calc → payment_calc` (`graph.py:64`)
- `payment_calc → report_compose` (`graph.py:66`)
- `report_compose → output_guardrail` (`graph.py:67`)
- `output_guardrail → persist` (`graph.py:68`)
- `persist → END` (`graph.py:69`)
- `persist_blocked → END` (`graph.py:70`)

### 1-3. 조건분기 3개 (`graph.py:37-62`)

조건분기는 상황을 보고 갈 길이 나뉘는 갈림길이다. 세 군데가 있다.

**(1) `input_guardrail` 이후** — `route_after_input`이 판단한다 (`graph.py:37-41`):
```python
g.add_conditional_edges(
    "input_guardrail",
    agents.route_after_input,
    {"blocked": "persist_blocked", "diagnosis": "diagnosis"},
)
```
입력이 차단되면 LLM(대규모 언어 모델, 사람처럼 글을 읽고 쓰는 AI)을 쓰는 나머지 단계를 전부 건너뛰고 곧장 `persist_blocked`로 빠진다. 코드 주석은 그 이유를 "비용/오출력 방지"와 "status 미갱신(무한 처리중) 방지"라고 밝힌다 (`graph.py:35-36`).

> 쉽게 말하면: 애초에 처리하면 안 되는 요청(예: 보험과 상관없는 질문)이 들어오면, 비싼 AI를 돌리느라 돈 쓰지 말고 바로 "차단됨" 도장을 찍고 내보낸다.

**(2) `diagnosis` 이후** — `policy_in_db`가 판단한다 (`graph.py:46-50`):
```python
g.add_conditional_edges(
    "diagnosis",
    agents.policy_in_db,
    {"terms_parse": "terms_parse", "coverage_parse": "coverage_parse"},
)
```
사용자의 약관이 우리 DB(`policy_chunks` 테이블)에 이미 들어있지 않으면 `terms_parse`(런타임 파싱 스텁 — 아직 실제 기능이 없는 자리)를 거치고, 이미 들어있으면 `coverage_parse`로 곧장 간다 (`graph.py:43-45` 주석).

**(3) `case_search` 이후** — `route_after_case`가 판단한다 (`graph.py:58-62`):
```python
g.add_conditional_edges(
    "case_search",
    agents.route_after_case,
    {"disability": "disability_rag", "payment_calc": "payment_calc"},
)
```
후유장해를 따져봐야 하면 장해 계산용 작은 갈래(`disability_rag → disability_calc`)를 거치고, 아니면 `payment_calc`로 곧장 간다. 장해 갈래를 탄 경우에도 결국 `payment_calc`에서 다시 만난다(그래서 `payment_calc`로 들어오는 길이 2개다) (`graph.py:56-64` 주석).

### 1-4. START→END 전체 경로 ASCII 다이어그램

아래 그림은 서류철이 입구(START)에서 출구(END)까지 지나가는 전체 길을 보여준다. `◇` 표시가 갈림길이다.

```
                                    START
                                      │
                                      ▼
                               load_context
                                      │
                                      ▼
                              input_guardrail
                                      │
                          route_after_input ◇ (조건분기)
                      ┌───────────────┴────────────────┐
              "blocked"│                        "diagnosis"│
                       ▼                                   ▼
               persist_blocked                        diagnosis
                       │                                   │
                       │                       policy_in_db ◇ (조건분기)
                       │                   ┌───────────────┴───────────────┐
                       │          "terms_parse"│                "coverage_parse"│
                       │                       ▼                               │
                       │                  terms_parse                          │
                       │                       │                               │
                       │                       └──────────────┐               │
                       │                                      ▼               ▼
                       │                              coverage_parse ◄─────────┘
                       │                                      │
                       │                                      ▼
                       │                             coverage_analysis
                       │                                      │
                       │                                      ▼
                       │                                 case_search
                       │                                      │
                       │                          route_after_case ◇ (조건분기)
                       │                      ┌───────────────┴───────────────┐
                       │            "disability"│                 "payment_calc"│
                       │                        ▼                              │
                       │                  disability_rag                       │
                       │                        │                              │
                       │                        ▼                              │
                       │                  disability_calc                      │
                       │                        │                              │
                       │                        ▼                              ▼
                       │                   payment_calc ◄─────────────────────┘   ★ 재합류 (2 incoming)
                       │                        │
                       │                        ▼
                       │                  report_compose
                       │                        │
                       │                        ▼
                       │                  output_guardrail
                       │                        │
                       │                        ▼
                       │                     persist
                       │                        │
                       └────────────┐           ▼
                                    ▼          END
                                   END
```

★로 표시한 재합류(길이 다시 하나로 합쳐지는) 지점은 `payment_calc`다. 여기서 두 길이 만난다 — `coverage_analysis→case_search→route_after_case`에서 "payment_calc"로 곧장 온 길과, 장해 갈래(`disability_calc→payment_calc`)를 거쳐 온 길이다. `coverage_parse`도 들어오는 길이 2개다(`terms_parse`를 거쳐 오는 길과, `policy_in_db`가 "coverage_parse"로 곧장 보낸 길). 끝나는 작업대는 `persist`와 `persist_blocked` 둘인데, 둘 다 `END`로 간다 (`graph.py:69-70`).

---

## 2. `@safe_node` 데코레이터

`safe_node`는 각 작업대를 감싸는 "안전망"이다. 정의는 `nodes/agents.py:30-44`에 있다. 원문 그대로:

```python
def safe_node(fn: Callable[[ReportState], Awaitable[dict[str, Any]]]):
    """노드 예외를 삼켜 errors에 기록하고 부분결과로 진행한다(이슈 #11 방침).

    노드 본문이 던지면 그래프 전체가 죽는 대신 {"errors": [...]}만 머지된다.
    다운스트림 노드는 전부 state.get(key, default)로 읽으므로 누락 키에 안전하다.
    """

    @functools.wraps(fn)
    async def wrapper(state: ReportState) -> dict[str, Any]:
        try:
            return await fn(state)
        except Exception as e:
            return {"errors": _err(state, f"{fn.__name__}_failed:{type(e).__name__}:{e}")}

    return wrapper
```

무슨 일을 하냐면:
- 작업대가 도중에 `Exception`(오류)을 내뿜으면, 그걸 붙잡아서 `{"errors": [...]}`만 돌려준다. LangGraph는 이 조각을 공용 서류철에 끼워 넣고 다음 작업대로 계속 진행한다. 덕분에 작업대 하나가 넘어져도 벨트 전체가 멈추지 않는다 (`agents.py:41-42`).
- 오류를 남길 때 쓰는 표식(마커) 형식은 `"{fn.__name__}_failed:{type(e).__name__}:{e}"`다. 예를 들어 `load_context`가 오류를 내면 `load_context_failed:KeyError:...` 같은 식으로 기록된다 (`agents.py:42`).
- `_err()` 헬퍼는 기존 오류 목록을 복사한 뒤 새 메시지를 뒤에 덧붙인다: `return [*state.get("errors", []), msg]` (`agents.py:26-27`). 즉 이전 오류를 지우지 않고 그대로 두면서 하나씩 쌓는다.

> 쉽게 말하면: 작업대마다 밑에 그물을 쳐 둔 셈이다. 누가 발을 헛디뎌도 바닥까지 떨어지지 않고, "여기서 이 사람이 넘어졌음"이라는 메모만 서류철에 남긴 채 다음 작업대로 넘어간다.

**어느 작업대에 붙어 있나 (14개 노드 전부에 `@safe_node`):**
- `load_context` (`agents.py:65`)
- `input_guardrail` (`agents.py:123`)
- `diagnosis` (`agents.py:136`)
- `terms_parse` (`agents.py:203`)
- `coverage_parse` (`agents.py:211`)
- `coverage_analysis` (`agents.py:217`)
- `case_search` (`agents.py:266`)
- `disability_rag` (`agents.py:382`)
- `disability_calc` (`agents.py:456`)
- `payment_calc` (`agents.py:472`)
- `report_compose` (`agents.py:486`)
- `output_guardrail` (`agents.py:561`)
- `persist` (`agents.py:570`)
- `persist_blocked` (`agents.py:634`)

실제로 14개 작업대 **전부**에 `@safe_node`가 붙어 있다.

**어디엔 안 붙나 (라우터 3개):**
- `policy_in_db` (`agents.py:171`) — 데코레이터가 없다. 대신 함수 안에 자체 `try/except`가 있어서, DB 조회가 실패하면 안전하게 `"terms_parse"`를 돌려준다 (`agents.py:176-190`).
- `route_after_input` (`agents.py:195`) — 데코레이터 없음. 순수하게 방향만 정하는 함수다.
- `route_after_case` (`agents.py:279`) — 데코레이터 없음. 역시 방향만 정하는 함수다.

라우터는 서류철(dict)이 아니라 **어디로 갈지 알려주는 경로 문자열**(`str`)을 돌려준다. 그래서 `safe_node`가 요구하는 반환 형식(`dict[str, Any]`)과 맞지 않아 붙일 수 없다. 참고로 `_extract_schedule_items`(`agents.py:300`)와 `_select_schedule`(`agents.py:292`)는 작업대가 아니라 내부에서만 쓰는 보조 함수라, 애초에 데코레이터를 붙일 대상이 아니다.

---

## 3. 14개 노드 상세

각 작업대는 서류철 전체를 통째로 갈아끼우지 않고, 자기가 바꾼 **일부 항목만** 돌려준다. 그러면 LangGraph가 그 조각을 공용 서류철에 합쳐 넣는다 (`agents.py:3-4`).

### 3-1. `load_context` — 서류 모으기

- **(a) 파일:라인** — `agents.py:65-119`
- **(b) 하는 일** — DB를 뒤져서 사고·약관 관련 자료를 한데 모은다. OCR 결과·리포트·청구·보험가입 4개 테이블에서 데이터를 긁어와 `case_info`(사건 정보 묶음)를 만든다.
- **(c) 서류철에서 읽는 항목** — `ocr_result_id`(`agents.py:71`), `report_id`(`agents.py:76,88`), `errors`(`agents.py:91`).
- **(d) 서류철에 쓰는 항목** — 반환 dict (`agents.py:113-119`): `case_info`, `masked_text`, `entities`, `subscribed_coverages`, `errors`.
- **(e) LLM 호출** — 없음.
- **(f) DB 쿼리** — 4개를 커넥션 하나(`pool.acquire()`)로 처리한다 (`agents.py:68-89`):
  1. `SELECT masked_text, entities FROM ocr_results WHERE id = $1` — `ocr_result_id`를 UUID로 바꿔 조회한다 (`agents.py:69-72`).
  2. `SELECT accident_type, treatment, offered_amount, question, claim_id FROM reports WHERE id = $1` (`agents.py:73-77`).
  3. (rep에 claim_id가 있을 때만) `SELECT diagnosis, accident_date, accident_type, offered_amount, description, hospitalization FROM user_claims WHERE id = $1` (`agents.py:79-84`).
  4. `SELECT insurer_name, product_name, coverages, enrolled_at FROM user_insurances WHERE user_id = (SELECT user_id FROM reports WHERE id = $1) LIMIT 1` (`agents.py:85-89`).
- **(g) RAG/가드레일** — 없음.
- **(h) 남기는 errors 표식** — OCR 조회 결과가 비면 `"ocr_result_missing"`을 덧붙인다 (`agents.py:92-93`). 이 표식은 `worker.py`가 "그냥 넘어가면 안 되는 심각한 실패"로 취급하는 목록에 들어 있다(§6 참고).

`case_info`는 리포트(rep)와 청구(claim)에서 온 값을 "먼저 있는 쪽을 쓰는" 방식(fallback 체인)으로 합친다. 예를 들어 `accident_type`은 `rep["accident_type"] or claim["accident_type"]`으로 정한다 (`agents.py:100-112`). `entities`가 dict가 아니면 `json.loads`로 파싱한다 (`agents.py:94-98`). `subscribed_coverages`(가입한 특약 목록)는 `list(ins["coverages"])`, 없으면 빈 리스트다 (`agents.py:117`). `enrolled_at`은 어느 시점의 표준 장해분류표를 써야 하는지 맞추는 데 쓰는 가입일이다 (`agents.py:109-111` 주석).

### 3-2. `input_guardrail` — 입구 검문소

- **(a) 파일:라인** — `agents.py:123-129`
- **(b) 하는 일** — 입력 가드레일(안전 필터)을 돌린다. 개인정보(PII)를 가리고, 보험과 무관한 요청은 막는다.
- **(c) 서류철에서 읽는 항목** — `masked_text`(`agents.py:125`), `errors`(간접적으로 `_err`을 통해).
- **(d) 서류철에 쓰는 항목** — `masked_text`(가드레일이 다시 가린 텍스트로 갱신) (`agents.py:126`), 차단된 경우 `errors` (`agents.py:128`).
- **(e) LLM 호출** — 없음(가드레일 내부는 정규식과 NER(문장에서 이름·주소 같은 개체를 찾아내는 기술)로 처리). `await guardrail.guard_input(...)`을 부른다 (`agents.py:125`).
- **(f) DB 쿼리** — 없음.
- **(g) RAG/가드레일** — `guardrail.guard_input(state.get("masked_text", ""))`을 호출한다 (`agents.py:125`). 돌아온 객체 `g`에서 `g.masked_text`, `g.blocked`, `g.reason`을 쓴다.
- **(h) 남기는 errors 표식** — 차단 시 `f"input_blocked:{g.reason}"` (`agents.py:128`). 이 표식은 뒤의 `route_after_input`이 보고 갈림길을 정한다(§4 참고).

```python
g = await guardrail.guard_input(state.get("masked_text", ""))
out: dict[str, Any] = {"masked_text": g.masked_text}
if g.blocked:
    out["errors"] = _err(state, f"input_blocked:{g.reason}")
return out
```

### 3-3. `diagnosis` — 진단서 읽기

- **(a) 파일:라인** — `agents.py:136-167`
- **(b) 하는 일** — 의료 문서에서 병명·ICD 코드·사고유형과 "후유장해를 따져봐야 하는지" 여부를 LLM으로 뽑아낸다.
- **(c) 서류철에서 읽는 항목** — `masked_text`(`agents.py:138`), `case_info`(보조로, `agents.py:159,165`), `errors`(간접).
- **(d) 서류철에 쓰는 항목** — `diagnosis`(dict) (`agents.py:167`). LLM이 실패하면 폴백 dict인 `diagnosis`와 `errors`를 쓴다 (`agents.py:157-164`).
- **(e) LLM 호출** — 있음. `ai_client.chat_json(...)`을 부른다 (`agents.py:139`). 시스템 프롬프트는 "너는 보험 손해사정 진단 분석가다. 의료문서에서 정보를 추출해 JSON만 출력한다."다 (`agents.py:143`). 유저 프롬프트는 문서 텍스트를 주고 JSON 키 `diagnosis`, `icd_codes`, `accident_type`(7종 중 하나), `surgery`, `hospitalization`, `requires_disability_review`를 뽑아달라고 요구한다 (`agents.py:147-152`). 사고유형 목록 상수는 `_ACCIDENT_TYPES = "medical_indemnity, traffic, disability, cancer_diagnosis, fire, liability, other"`다 (`agents.py:133`).
- **(f) DB 쿼리** — 없음.
- **(g) RAG/가드레일** — 없음.
- **(h) 남기는 errors 표식** — LLM 응답이 dict가 아니거나 비면 `"diagnosis_llm_failed"` (`agents.py:163`).

LLM이 제대로 된 dict를 주면, `setdefault`로 빈 칸을 채운다 — `accident_type`이 없으면 `case_info.accident_type` 또는 `"other"`로, `requires_disability_review`가 없으면 `False`로 메운다 (`agents.py:165-166`). 이 `requires_disability_review` 값은 나중에 `route_after_case`가 갈림길을 정할 때 기준이 된다.

### 3-4. `terms_parse` — 약관 파싱 (미구현 스텁)

- **(a) 파일:라인** — `agents.py:203-207`
- **(b) 하는 일** — (설계상으로는) 사용자가 올린 약관을 PDFPlumber/VLM으로 읽어 조각내고 임시로 임베딩(문장을 숫자 좌표로 바꿔 검색에 쓰는 것)한다. 하지만 지금은 **아직 만들지 않은 빈 자리(스텁)**다.
- **(c) 서류철에서 읽는 항목** — `errors`(간접, `_err`).
- **(d) 서류철에 쓰는 항목** — `errors`만.
- **(e) LLM 호출** — 없음.
- **(f) DB 쿼리** — 없음.
- **(g) RAG/가드레일** — 없음.
- **(h) 남기는 errors 표식** — 언제나 `"policy_not_in_db:runtime_parse_stub"` (`agents.py:207`).

```python
@safe_node
async def terms_parse(state: ReportState) -> dict[str, Any]:
    # 실제: 사용자 업로드 약관을 PDFPlumber/VLM 파싱 → 청킹 → 임시 임베딩.
    # 실험에서는 무거워 스킵하고 폴백 기록.
    return {"errors": _err(state, "policy_not_in_db:runtime_parse_stub")}
```

**스텁임을 분명히 함**: 주석 그대로 실제 파싱 로직은 아직 없고, 폴백 표식만 남긴다. `policy_in_db`가 `"terms_parse"`를 돌려줄 때만 이 작업대로 들어온다.

### 3-5. `coverage_parse` — 가입 특약 확정 (사실상 통과)

- **(a) 파일:라인** — `agents.py:211-213`
- **(b) 하는 일** — 가입한 특약을 확정한다. 지금은 `subscribed_coverages`를 받은 그대로 흘려보내는, 사실상 통과 지점(패스스루)이다.
- **(c) 서류철에서 읽는 항목** — `subscribed_coverages` (`agents.py:213`).
- **(d) 서류철에 쓰는 항목** — `subscribed_coverages`(같은 값을 다시 기록) (`agents.py:213`).
- **(e) LLM 호출** — 없음. **(f) DB** — 없음. **(g) RAG/가드레일** — 없음. **(h) errors 표식** — 없음.

```python
@safe_node
async def coverage_parse(state: ReportState) -> dict[str, Any]:
    return {"subscribed_coverages": state.get("subscribed_coverages", [])}
```

**사실상 아무 처리도 안 함(no-op)**: 받은 특약을 그대로 돌려준다. 이 작업대의 진짜 쓸모는, `terms_parse`를 거쳐 온 길과 곧장 온 길이 여기서 다시 하나로 합쳐지는 재합류 지점 역할이다.

### 3-6. `coverage_analysis` — 약관 대조 분석

- **(a) 파일:라인** — `agents.py:217-262`
- **(b) 하는 일** — Hybrid RAG(키워드 검색과 의미 검색을 섞어 관련 문서를 찾아오는 방식)로 약관 조항을 찾고, LLM으로 가입 특약과 대조해서 적용되는 특약·빠진 특약·면책(보험사가 안 주는 경우) 분석을 만든다.
- **(c) 서류철에서 읽는 항목** — `case_info`(`agents.py:219`), `diagnosis`(`agents.py:220`), `subscribed_coverages`(`agents.py:245`), `errors`(간접).
- **(d) 서류철에 쓰는 항목** — 검색 결과가 없으면 빈 `retrieved_clauses`와 `errors` (`agents.py:233`); 정상일 때는 `retrieved_clauses`, `applicable_coverages`, `missing_coverages`, `coverage_analysis` (`agents.py:254-262`).
- **(e) LLM 호출** — 있음. `ai_client.chat_json(...)` (`agents.py:236`). 시스템: "너는 보험 약관 분석가다. 가입 특약과 약관 조항을 대조해 JSON만 출력한다." (`agents.py:240`). 유저는 가입특약·사고·약관조항(상위 6개 조각 각 300자)을 주고 JSON 키 `applicable`, `missing`, `analysis`를 요구한다 (`agents.py:244-249`).
- **(f) DB 쿼리** — 직접은 없음(RAG 내부에서 대신 수행).
- **(g) RAG/가드레일** — `hybrid.search(query, namespaces=["terms"], top_k=8, insurer=..., product=...)` (`agents.py:224-230`). 검색어(query)는 `f"{dx_name} {icd} {ci.get('question','')}"`다 (`agents.py:221-223`).
- **(h) 남기는 errors 표식** — 검색해서 나온 조각이 없으면 `"rag_empty"` (`agents.py:233`).

LLM이 준 `applicable`·`missing`은 `_as_str_list`로 형식을 다듬는다(`agents.py:256-257`). `_as_str_list`(`agents.py:47-61`)는 LLM이 `list[dict]`나 `str`처럼 제각각으로 답해도, dict라면 `name`/`title`/`특약`/첫 값을 뽑아 깔끔한 문자열 목록으로 통일해준다. `coverage_analysis`는 `{"analysis": str, "citations": res["citations"][:6]}` 형태다 (`agents.py:258-261`).

### 3-7. `case_search` — 판례·분쟁조정 찾기

- **(a) 파일:라인** — `agents.py:266-275`
- **(b) 하는 일** — 판례와 분쟁조정 사례를 RAG(`case` namespace)로 찾는다.
- **(c) 서류철에서 읽는 항목** — `case_info`(`agents.py:268`), `diagnosis`(`agents.py:269`), `errors`(간접).
- **(d) 서류철에 쓰는 항목** — 결과가 없으면 빈 `legal_references`와 `errors` (`agents.py:274`); 있으면 `legal_references` (`agents.py:275`).
- **(e) LLM 호출** — 없음.
- **(f) DB 쿼리** — 직접은 없음(RAG 내부).
- **(g) RAG/가드레일** — `hybrid.search(dx_name, namespaces=["case"], top_k=4)` (`agents.py:271`). 검색어는 `dx_name`(진단명) 하나만.
- **(h) 남기는 errors 표식** — 결과가 비면 `"case_data_missing"` (`agents.py:274`). 주석은 "case_chunks 미적재 → 폴백"이라고 밝힌다 (`agents.py:265`) — 아직 판례 데이터가 채워지지 않은 상태를 전제로 한 폴백 처리다.

### 3-8. `disability_rag` — 장해분류표 찾기·추출

- **(a) 파일:라인** — `agents.py:382-452`
- **(b) 하는 일** — 후유장해 장해분류표를 RAG로 찾아 LLM으로 분류·지급률을 뽑되, 숫자는 약관 원문에 실제로 있을 때만 인정한다(결정론 백스톱 — AI가 지어낸 숫자를 막는 안전장치). 가입 약관에 표가 없으면 금감원 표준표(level)로 대신 찾는다.
- **(c) 서류철에서 읽는 항목** — `case_info`(`agents.py:384`, `enrolled_at` 포함 `agents.py:401`), `diagnosis`(`agents.py:385`), `retrieved_clauses`(`agents.py:397`), `errors`(간접).
- **(d) 서류철에 쓰는 항목** — 표가 없으면 빈 골격 `disability_analysis`와 `retrieved_clauses`, `errors` (`agents.py:410-422`); 정상일 때는 `disability_analysis`와 합쳐진 `retrieved_clauses` (`agents.py:439-448`), 폴백일 때는 `errors`를 추가 (`agents.py:449-451`).
- **(e) LLM 호출** — 있음(보조 함수 `_extract_schedule_items` 안에서, `agents.py:312-336`). 시스템 프롬프트: "너는 보험 약관 장해분류표 분석가다. 제공된 [약관 장해분류표 원문]에서만 근거를 찾아 사고를 분류하고 지급률을 추출한다. 표에 없는 지급률은 절대 만들지 마라. JSON만 출력한다." (`agents.py:316-319`). 유저는 사고/진단과 장해분류표 원문(상위 6조각 각 800자)을 주고 `items`(injury, body_region, category_label, rate, rate_quote, temporary, temporary_years, citation), `uncertain`, `notes`를 요구하며 "category_label·rate_quote는 요약 말고 복사"·"추측으로 숫자 만들지 마라"를 강제한다 (`agents.py:323-333`).
- **(f) DB 쿼리** — 직접은 없음(RAG 내부).
- **(g) RAG/가드레일** — 1차: `hybrid.search(query, namespaces=["terms"], top_k=8, insurer=..., product=...)` (`agents.py:389-395`), 검색어 = `f"{dx_name} {icd} 후유장해 장해분류표 지급률"` (`agents.py:388`). 폴백: `hybrid.search(query, namespaces=["level"], top_k=8, contract_date=enrolled_at)` (`agents.py:404`).
- **(h) 남기는 errors 표식** — 표가 아예 없으면 `"disability_schedule_missing"` (`agents.py:421`); 표준표 폴백이 성공하면 `"disability_fallback_standard_schedule"` (`agents.py:451`).

**어떤 조각을 표로 쓸지 고르기** — `_select_schedule`(`agents.py:292-297`)은 `chunk_type == "schedule"`인 조각을 먼저 쓰고, 그런 게 없으면 텍스트에 "장해의 분류"가 들어간 조각을 쓰는 어림짐작(휴리스틱) 방식이다 (`agents.py:294-296`). 폴백 경로(level)는 애초에 "전부 표의 행 묶음"이라 따로 고르지 않고 그대로 쓴다 (`agents.py:405-406`).

**결정론 백스톱 (지어낸 숫자 차단)** — `_extract_schedule_items`에서 각 항목의 지급률은 `verified = bool(quote) and (str(int(rate_f)) in sched_text)`로, 그 숫자가 실제 원문에 있는지 검증한다 (`agents.py:350-351`). 검증에 실패하면 `notes`에 "미검증 지급률 제외"라고 적되, 항목 자체는 `verified: False`를 달아 목록에는 담아둔다 (`agents.py:353-367`). 신뢰도(confidence)는 `high`(모든 항목이 검증되고 uncertain도 아님)/`medium`(일부만 검증)/`low`로 매긴다 (`agents.py:369-374`).

> 쉽게 말하면: AI가 "이 장해는 30% 지급"이라고 말해도, 그 "30"이라는 숫자가 진짜 약관 원문에 적혀 있는지 프로그램이 직접 대조해서 확인한다. 원문에 없으면 "검증 안 됨" 도장을 찍어 두고 나중 계산에서 빼버린다. AI가 숫자를 지어내도 최종 금액에는 반영되지 않게 하는 안전장치다.

**폴백 신뢰도 낮추기·주의문구** — 폴백일 때 붙이는 주의문구(caveat)는 `_STANDARD_CAVEAT`("표준 장해분류표 기준(가입 약관 미확보) — 개별 약관 확인 필요", `agents.py:288`)다. `enrolled_at`이 없으면(None) `_STANDARD_NO_DATE_CAVEAT`("가입일 미상 — 현행판 기준", `agents.py:289`)를 덧붙이고, 신뢰도가 high였다면 medium으로 한 단계 **낮춘다** (`agents.py:427-433`). 폴백이 아닌 경우(terms)의 주의문구는 `_TERMS_CAVEAT`("가입금액 미보유로 절대 보험금 불가·약관표 위치정렬 한계 — 지급률은 추정", `agents.py:287,435`)다. `retrieved_clauses`는 기존 것과 새것을 합치되 같은 출처(source_ref)는 중복 제거한다 (`agents.py:437-438`).

### 3-9. `disability_calc` — 장해율 최종 합산 (LLM 없음)

- **(a) 파일:라인** — `agents.py:456-468`
- **(b) 하는 일** — 검증된 장해 항목만 골라, 정해진 규칙(계산기)으로 최종 합산 장해지급률을 계산한다. **여기엔 LLM이 없다.**
- **(c) 서류철에서 읽는 항목** — `disability_analysis` (`agents.py:458`).
- **(d) 서류철에 쓰는 항목** — `disability_analysis`(기존 dict에 `combined_rate`, `normalized_items`를 더하고 `rule_notes`를 합침) (`agents.py:461-467`).
- **(e) LLM 호출** — 없음. **(f) DB** — 없음. **(g) RAG/가드레일** — 없음. **(h) errors 표식** — 없음(정상 경로).
- **핵심** — `verified = [i for i in da.get("items", []) if i.get("verified")]`로 검증된 항목만 걸러내고 (`agents.py:459`), `combine_disability_rate(verified)`를 부른다(§7 참고) (`agents.py:460`). 기존 `disability_analysis`는 스프레드(`**da`)로 그대로 남기고 결과만 덮어쓴다:

```python
return {
    "disability_analysis": {
        **da,
        "combined_rate": result["combined_rate"],
        "normalized_items": result["normalized_items"],
        "rule_notes": list(da.get("rule_notes", [])) + result["rule_notes"],
    }
}
```

> 쉽게 말하면: 앞 작업대(`disability_rag`)에서 "진짜 원문에 있는 숫자"라고 도장 받은 항목만 추려서, 사람이 짜둔 규칙대로 더하고 상한을 씌워 최종 장해율 하나를 뽑는다. 이 계산엔 AI가 끼어들지 않으므로 결과가 매번 똑같이 나온다.

### 3-10. `payment_calc` — 예상 보험금 범위 (재합류 지점)

- **(a) 파일:라인** — `agents.py:472-482`
- **(b) 하는 일** — 예상 보험금을 "범위"로 계산한다(단정은 금지). 두 길이 만나는 재합류 지점이다(들어오는 길 2개).
- **(c) 서류철에서 읽는 항목** — `case_info`의 offered_amount (`agents.py:474`), `disability_analysis`의 combined_rate (`agents.py:478`).
- **(d) 서류철에 쓰는 항목** — `estimated_range`(`{"min": lo, "max": hi}`) (`agents.py:482`).
- **(e) LLM 호출** — 없음. **(f) DB** — 없음. **(g) RAG/가드레일** — 없음. **(h) errors 표식** — 없음.

계산 방식: `base = max(offered, 0)`으로 기준액을 잡고, 장해지급률(rate)이 있으면 상단 배수를 `factor_hi = 1.0 + min(rate,100)/100 * 0.8`로 정한다(0%면 ×1.0, 100%면 ×1.8). rate가 없으면 배수는 `1.8`이다 (`agents.py:475-479`). 그다음 `lo = int(base*1.0)`, `hi = int(base*factor_hi) if base else 0`으로 하한·상한을 낸다 (`agents.py:480-481`). base가 0이면 hi도 0이다.

> 쉽게 말하면: 보험사가 처음 제시한 금액을 바닥값으로 두고, 장해가 심할수록 위쪽 한계를 최대 1.8배까지 벌린다. "얼마다"라고 못 박지 않고 "이 정도에서 저 정도 사이"라고 범위로만 말한다.

### 3-11. `report_compose` — 리포트 조립

- **(a) 파일:라인** — `agents.py:486-557`
- **(b) 하는 일** — 8개(실제로는 키 9개) 섹션과 issues를 합쳐 하나의 리포트 Markdown으로 조립하고, 생성 가드레일을 적용한다.
- **(c) 서류철에서 읽는 항목** — `case_info`(`agents.py:488`), `coverage_analysis`(`agents.py:489`), `diagnosis`(`agents.py:490`), `disability_analysis`(`agents.py:492`), `applicable_coverages`(`agents.py:510,532,543`), `missing_coverages`(`agents.py:511,532,544`), `estimated_range`(`agents.py:513,551`), `legal_references`(`agents.py:548`), `errors`(`agents.py:554`).
- **(d) 서류철에 쓰는 항목** — `sections`(dict), `issues`(list), `report`(Markdown 문자열) (`agents.py:557`).
- **(e) LLM 호출** — 2번. (1) `ai_client.chat(...)`으로 리포트 본문을 만든다 (`agents.py:500`). 시스템: "너는 보험 손해사정 리포트 작성자다. 사실 주장에는 약관 조항 인용을 포함하고, 금액은 단정하지 말고 범위로 쓴다." (`agents.py:504`). (2) `ai_client.chat_json(...)`으로 핵심 쟁점(issues)을 뽑는다 (`agents.py:523`). 시스템: "보험 리포트의 핵심 쟁점을 JSON 배열로 추출한다. JSON만." (`agents.py:527`), 형식은 `{"issues":[{"title","description","ai_status":"CONFIRMED|TRUSTED|INFO","tags"}]}` (`agents.py:534`).
- **(f) DB 쿼리** — 없음.
- **(g) RAG/가드레일** — 생성 가드레일 `body = guardrail.guard_generation(body)` (`agents.py:521`) — 금액을 단정적으로 쓴 표현을 바꾸고, 인용을 강제한다.
- **(h) 남기는 errors 표식** — 자기 표식은 없다. 다만 섹션 `7_추가확인필요`에 그동안 쌓인 errors를 그대로 보여준다: `"; ".join(state.get("errors", [])) or "없음"` (`agents.py:554`).

`disability_line`은 장해 items가 있을 때만 합산지급률·신뢰도·근거·규칙·주의문구를 한 문장으로 엮고, 없으면 "해당 없음(후유장해 미검토)"으로 둔다 (`agents.py:493-499`). sections 키는 `1_사건요약`, `2_적용특약`, `3_누락가능특약`, `4_약관근거`, `4b_판례근거`, `5_추정보상범위`, `5b_장해지급률`, `6_본문`, `7_추가확인필요`다 (`agents.py:541-555`). `report_md`는 `"\n\n".join(f"## {k}\n{v}" ...)`로 이어 붙인다 (`agents.py:556`).

### 3-12. `output_guardrail` — 출구 검문소

- **(a) 파일:라인** — `agents.py:561-566`
- **(b) 하는 일** — 출력 가드레일이다. 법적 고지문을 끼워 넣고, LLM Judge(AI가 쓴 내용이 근거와 맞는지 다른 AI가 채점하는 검사)로 인용을 검증한다.
- **(c) 서류철에서 읽는 항목** — `report`(`agents.py:564`), `retrieved_clauses`(`agents.py:564`).
- **(d) 서류철에 쓰는 항목** — `report`(고지문 등이 반영된 최종본), `judge_failures` (`agents.py:566`).
- **(e) LLM 호출** — 가드레일 내부의 LLM Judge(`run_judge=True`) (`agents.py:564`).
- **(f) DB 쿼리** — 없음.
- **(g) RAG/가드레일** — `guardrail.guard_output(state.get("report",""), run_judge=True, chunks=state.get("retrieved_clauses", []))` (`agents.py:563-565`). 돌아온 `g.final_text`, `g.judge_failures`를 쓴다.
- **(h) 남기는 errors 표식** — 없음(문제가 있으면 `judge_failures`에 따로 적는다).

> 쉽게 말하면: 리포트를 내보내기 전에, 다른 AI가 검사관이 되어 "이 문장이 진짜 근거 조항과 맞나?"를 채점한다. 어긋난 부분은 `judge_failures`에 적어 둔다.

### 3-13. `persist` — 저장 (정상 종료)

- **(a) 파일:라인** — `agents.py:570-630`
- **(b) 하는 일** — `report_drafts`(업서트), `reports`(갱신), `report_issues`(지운 뒤 다시 삽입)에 결과를 영구 저장한다. 정상적으로 끝나는 종료 작업대다.
- **(c) 서류철에서 읽는 항목** — `coverage_analysis`(citations) (`agents.py:573`), `legal_references`(`agents.py:574-576`), `disability_analysis`(citations) (`agents.py:577`), `sections`·`estimated_range`·`judge_failures`·`issues`·`applicable_coverages`·`missing_coverages` (`agents.py:580-591`), `report_id`(`agents.py:593`), `errors`(`agents.py:591,630`).
- **(d) 서류철에 쓰는 항목** — `errors`(받은 그대로 반환) (`agents.py:630`). 실제 결과물은 DB에 남는 부수효과다.
- **(e) LLM 호출** — 없음.
- **(f) DB 쿼리** — 트랜잭션(`c.transaction()`, `agents.py:597`) 안에서 실행한다:
  1. `INSERT INTO report_drafts (report_id, draft, status) VALUES ($1,$2::jsonb,'draft') ON CONFLICT (report_id) DO UPDATE SET draft=EXCLUDED.draft, status='draft'` — **멱등 업서트**(같은 걸 여러 번 실행해도 한 번 한 것과 결과가 같음) (`agents.py:598-604`).
  2. `UPDATE reports SET applicable_guarantees=$2, omitted_special_contract=$3, basis_terms_precedents=$4, claimed_min_amount=$5, claimed_max_amount=$6, status='AWAITING_ADOPTION', updated_at=now() WHERE id=$1` (`agents.py:605-617`).
  3. `DELETE FROM report_issues WHERE report_id=$1` (`agents.py:618`).
  4. issues의 각 항목을 `INSERT INTO report_issues (id, report_id, title, description, ai_status, tags) VALUES ($1..$6)`로 넣는다 — id는 `uuid.uuid4()`, title은 200자로 자르고, ai_status가 없으면 `"INFO"`로 둔다 (`agents.py:619-629`).
- **(g) RAG/가드레일** — `guardrail.DISCLAIMER`(법적 고지문)를 draft에 끼워 넣는다 (`agents.py:583`).
- **(h) 남기는 errors 표식** — 자기 표식은 없다. 다만 `safe_node`가 여기서 예외를 잡으면 `persist_failed:...`가 되고, 이건 "그냥 넘어가면 안 되는 심각한 실패"로 취급된다(§6 참고).

> 쉽게 말하면: 트랜잭션은 "전부 되거나, 아니면 전부 없던 일로" 처리하는 묶음이다. 저장 중간에 하나라도 실패하면 앞서 한 것까지 다 되돌려, 반쪽짜리로 저장되는 사고를 막는다. 그리고 멱등 업서트 덕분에 같은 리포트를 두 번 처리해도 중복 저장 없이 최신 한 벌만 남는다.

`basis`는 `terms_cites + case_refs + da_cites`를 합친 것이다 (`agents.py:578`). draft dict에는 sections·estimated_range·disclaimer·judge_failures·issues·applicable_guarantees·omitted_special_contract·basis_terms_precedents·legal_references·disability·errors를 담고, `json.dumps(..., ensure_ascii=False)`로 문자열로 만든다 (`agents.py:580-603`).

### 3-14. `persist_blocked` — 차단 기록 (차단 경로 종료)

- **(a) 파일:라인** — `agents.py:634-658`
- **(b) 하는 일** — 입력 가드레일이 차단했을 때, 초안은 만들지 않고 `reports.status`만 `'BLOCKED'`로 바꾼다. 차단 경로가 끝나는 종료 작업대다.
- **(c) 서류철에서 읽는 항목** — `errors`(input_blocked 사유를 뽑아냄) (`agents.py:645`), `report_id`(`agents.py:648,651`).
- **(d) 서류철에 쓰는 항목** — `errors`(받은 그대로 반환) (`agents.py:658`).
- **(e) LLM 호출** — 없음.
- **(f) DB 쿼리** — `UPDATE reports SET status='BLOCKED', updated_at=now() WHERE id=$1` (`agents.py:654-657`). `report_drafts`는 만들지 않는다.
- **(g) RAG/가드레일** — 없음.
- **(h) 남기는 errors 표식** — 자기 표식은 없다. 차단 사유는 로그로만 남긴다: `logger.info("report blocked by input guardrail", report_id=..., reasons=reasons)` (`agents.py:647-649`).

**TODO/아직 안 정해진 부분** — 주석 `agents.py:642-643`에 따르면, `reports.status` enum에 `'BLOCKED'`를 아직 Spring 쪽 계약에 넣지 않았고 여기서 새로 만든 값이라, 나중에 맞춰야 한다. `safe_node`가 이 작업대에서 예외를 잡으면 `persist_blocked_failed`가 되지만, `worker.py`는 이걸 심각한 실패로 올리지 **않는다**(§6 참고).

---

## 4. 조건분기 라우터 3개

라우터는 갈림길에 선 안내판이다. 서류철을 잠깐 들여다보고 "이쪽 길"이라는 이름표(경로 문자열)만 돌려준다.

### 4-1. `route_after_input` (`agents.py:195-199`)

```python
def route_after_input(state: ReportState) -> str:
    """input_guardrail이 도메인외/차단을 표시하면 LLM 파이프라인을 건너뛴다."""
    if any(str(e).startswith("input_blocked") for e in state.get("errors", [])):
        return "blocked"
    return "diagnosis"
```

- 판단: `errors` 중 `"input_blocked"`로 시작하는 항목이 하나라도 있으면 `"blocked"`, 아니면 `"diagnosis"`.
- 경로 매핑(`graph.py:40`): `"blocked" → persist_blocked`, `"diagnosis" → diagnosis`.

### 4-2. `policy_in_db` (`agents.py:171-191`)

```python
async def policy_in_db(state: ReportState) -> str:
    insurer = state.get("case_info", {}).get("insurer")
    product = state.get("case_info", {}).get("product_name")
    if not insurer:
        return "terms_parse"
    try:
        pool = db.get_pool()
        async with pool.acquire() as c:
            if product:
                n = await c.fetchval(
                    "SELECT count(*) FROM policy_chunks WHERE insurer = $1 AND product_name = $2",
                    insurer,
                    product,
                )
            else:
                n = await c.fetchval(
                    "SELECT count(*) FROM policy_chunks WHERE insurer = $1", insurer
                )
    except Exception:  # DB 조회 실패 시 안전하게 런타임 파싱 경로로
        return "terms_parse"
    return "coverage_parse" if (n or 0) > 0 else "terms_parse"
```

- 판단: `insurer`(보험사)가 없으면 곧장 `"terms_parse"` (`agents.py:174-175`). insurer가 있으면 `policy_chunks`에서 개수를 센다 — product(상품명)까지 있으면 `insurer+product_name`으로, 없으면 `insurer`만으로 조회한다 (`agents.py:179-188`). DB에서 예외가 나면 안전하게 `"terms_parse"`로 빠진다 (`agents.py:189-190`). 개수가 0보다 크면 `"coverage_parse"`, 아니면 `"terms_parse"`다 (`agents.py:191`).
- **`@safe_node`가 안 붙는 라우터**: 대신 자체 try/except를 갖고 있다. 세 라우터 중 유일하게 async(비동기)이고, DB에 직접 손대는 라우터다.
- 경로 매핑(`graph.py:49`): `"terms_parse" → terms_parse`, `"coverage_parse" → coverage_parse`.

> 쉽게 말하면: "이 보험사·상품 약관을 우리가 이미 갖고 있나?"를 DB에서 확인한다. 있으면 바로 분석 단계로, 없으면 (아직 스텁인) 파싱 단계로 보낸다. DB 확인에 실패해도 프로그램이 멈추지 않고 파싱 쪽으로 안전하게 흘려보낸다.

### 4-3. `route_after_case` (`agents.py:279-283`)

```python
def route_after_case(state: ReportState) -> str:
    """진단이 requires_disability_review면 장해 노드로, 아니면 보험금 계산 직행."""
    if (state.get("diagnosis") or {}).get("requires_disability_review"):
        return "disability"
    return "payment_calc"
```

- 판단: `diagnosis.requires_disability_review`가 참이면 `"disability"`, 아니면 `"payment_calc"`.
- 경로 매핑(`graph.py:61`): `"disability" → disability_rag`, `"payment_calc" → payment_calc`.

---

## 5. 상태 전이 종합표

아래 표는 각 작업대가 서류철에서 무엇을 읽고, 무엇을 새로 쓰고, 옆으로 어떤 일(LLM·DB·RAG·가드레일)을 일으키는지 한눈에 정리한 것이다.

> 표 읽는 법: "읽는 키"는 이 작업대가 참고하는 서류철 항목, "추가/갱신하는 키"는 이 작업대가 채워 넣는 항목, "부수효과"는 이 작업대가 AI 호출·DB 접근·검색·안전 필터 중 무엇을 하는지다.

| 노드 | 읽는 키 | 추가/갱신하는 키 | 부수효과(LLM/DB/RAG/가드레일) |
|------|---------|------------------|-------------------------------|
| `load_context` (`agents.py:65`) | `ocr_result_id`, `report_id`, `errors` | `case_info`, `masked_text`, `entities`, `subscribed_coverages`, `errors` | DB 4쿼리(ocr_results, reports, user_claims, user_insurances). errors: `ocr_result_missing` |
| `input_guardrail` (`agents.py:123`) | `masked_text`, `errors` | `masked_text`, (차단시)`errors` | 가드레일 `guard_input`. errors: `input_blocked:{reason}` |
| `route_after_input` (`agents.py:195`) | `errors` | — (경로 문자열만) | 없음 |
| `diagnosis` (`agents.py:136`) | `masked_text`, `case_info`, `errors` | `diagnosis`, (실패시)`errors` | LLM `chat_json`. errors: `diagnosis_llm_failed` |
| `policy_in_db` (`agents.py:171`) | `case_info`(insurer, product_name) | — (경로 문자열만) | DB `count(*) FROM policy_chunks` |
| `terms_parse` (`agents.py:203`) | `errors` | `errors` | 없음(스텁). errors: `policy_not_in_db:runtime_parse_stub` |
| `coverage_parse` (`agents.py:211`) | `subscribed_coverages` | `subscribed_coverages`(동일) | 없음(no-op) |
| `coverage_analysis` (`agents.py:217`) | `case_info`, `diagnosis`, `subscribed_coverages`, `errors` | `retrieved_clauses`, `applicable_coverages`, `missing_coverages`, `coverage_analysis`, (빈검색시)`errors` | RAG `search(namespaces=["terms"])`, LLM `chat_json`. errors: `rag_empty` |
| `case_search` (`agents.py:266`) | `case_info`, `diagnosis`, `errors` | `legal_references`, (빈검색시)`errors` | RAG `search(namespaces=["case"])`. errors: `case_data_missing` |
| `route_after_case` (`agents.py:279`) | `diagnosis`(requires_disability_review) | — (경로 문자열만) | 없음 |
| `disability_rag` (`agents.py:382`) | `case_info`(insurer, product_name, enrolled_at), `diagnosis`, `retrieved_clauses`, `errors` | `disability_analysis`, `retrieved_clauses`(merged), (폴백/실패시)`errors` | RAG `search(["terms"])`+폴백`search(["level"])`, LLM `chat_json`(추출). errors: `disability_schedule_missing`, `disability_fallback_standard_schedule` |
| `disability_calc` (`agents.py:456`) | `disability_analysis` | `disability_analysis`(+combined_rate, +normalized_items, rule_notes 병합) | 없음(결정론 `combine_disability_rate`) |
| `payment_calc` (`agents.py:472`) | `case_info`(offered_amount), `disability_analysis`(combined_rate) | `estimated_range` | 없음 |
| `report_compose` (`agents.py:486`) | `case_info`, `coverage_analysis`, `diagnosis`, `disability_analysis`, `applicable_coverages`, `missing_coverages`, `estimated_range`, `legal_references`, `errors` | `sections`, `issues`, `report` | LLM `chat`(본문)+`chat_json`(issues), 생성가드레일 `guard_generation` |
| `output_guardrail` (`agents.py:561`) | `report`, `retrieved_clauses` | `report`, `judge_failures` | 출력가드레일 `guard_output(run_judge=True)` (LLM Judge) |
| `persist` (`agents.py:570`) | `coverage_analysis`, `legal_references`, `disability_analysis`, `sections`, `estimated_range`, `judge_failures`, `issues`, `applicable_coverages`, `missing_coverages`, `report_id`, `errors` | `errors`(패스스루) | DB 트랜잭션: report_drafts 업서트, reports UPDATE(status='AWAITING_ADOPTION'), report_issues DELETE+INSERT. DISCLAIMER 삽입 |
| `persist_blocked` (`agents.py:634`) | `errors`, `report_id` | `errors`(패스스루) | DB `UPDATE reports SET status='BLOCKED'`. 로그만 |

참고로, `case_info`는 `load_context`만 만들어 넣고, 그 뒤 작업대들은 읽기만 한다. `disability_analysis`는 `disability_rag`가 만들고 → `disability_calc`가 살을 붙이고 → `payment_calc`·`report_compose`·`persist`가 가져다 쓴다. 작업대가 순서대로 하나씩 돌기 때문에 두 곳에서 동시에 쓰는 일이 없고, 그래서 값을 합치는 reducer(`Annotated[..., add]`)를 쓰지 않는다 (`state.py:3-4`). 오직 `errors`만 각 작업대가 `_err`로 계속 이어 붙인다.

> 쉽게 말하면: 서류철의 항목들은 대부분 "쓰는 사람 한 명, 읽는 사람 여럿" 구조라 충돌이 없다. 예외인 `errors`는 여러 작업대가 계속 메모를 덧붙이는 공용 메모장이다.

---

## 6. `worker.handle_job` — 실행과 "심각한 실패" 승격

`worker.py`는 그래프를 실제로 돌리고, 정말 심각한 실패일 때만 예외를 위로 던진다. 그래프 조립은 **프로그램이 처음 로딩될 때 딱 한 번** 해두고 계속 재사용한다: `_graph = build_graph()` (`worker.py:26`) — 주석은 "그래프는 DB에 접촉하지 않고 조립되므로 import 시 1회 컴파일해 재사용한다"라고 밝힌다 (`worker.py:25`).

```python
async def handle_job(job: ReportJob) -> None:
    """단일 report-job 처리. 성공 시 조용히 반환(→커밋), 하드 실패 시 raise(→재시도/DLQ)."""
    bind_context(report_id=job.report_id, job_id=job.job_id)
    try:
        state = {
            "report_id": job.report_id,
            "ocr_result_id": job.ocr_result_id,
            "claim_id": job.claim_id,
            "user_ref": job.user_ref,
            "doc_type": job.doc_type.value,
        }
        final = await _graph.ainvoke(state)
        errors = final.get("errors", [])
        hard = [e for e in errors if str(e).startswith(_HARD_FAILURE_PREFIXES)]
        if hard:
            raise ReportWorkerError(f"리포트 생성 하드 실패: {hard}")
        logger.info("report generated", report_id=job.report_id, error_count=len(errors))
    finally:
        clear_context()
```
(`worker.py:33-51`)

**시작 서류철**: `ReportJob`에서 `report_id`·`ocr_result_id`·`claim_id`·`user_ref`·`doc_type.value`만 넣고 `_graph.ainvoke(state)`로 돌린다 (`worker.py:37-44`). 나머지 항목은 그래프가 지나가며 채운다.

**"심각한 실패"로 올리는 규칙** (`worker.py:45-48`): 최종 `errors` 중 `_HARD_FAILURE_PREFIXES`로 시작하는 항목이 하나라도 있으면 `ReportWorkerError`를 던진다. `str.startswith`에 튜플을 넘겨서 여러 접두어를 한 번에 맞춰본다.

```python
_HARD_FAILURE_PREFIXES = ("load_context_failed", "persist_failed", "ocr_result_missing")
```
(`worker.py:23`)

즉, **재시도하거나 DLQ(처리 실패한 메시지를 따로 모아두는 큐)로 보낼 심각한 실패**는 다음 셋이다:
- `load_context_failed:...` — 서류 모으기 자체가 실패(safe_node가 잡은 예외).
- `persist_failed:...` — 저장이 실패(safe_node가 잡은 예외).
- `ocr_result_missing` — load_context가 OCR 결과를 못 찾음.

**그냥 넘어가고 커밋하는(심각하지 않은) 경우**: `rag_empty`, `input_blocked:*`, `*_llm_failed`, `case_data_missing`, `disability_schedule_missing`, `policy_not_in_db:runtime_parse_stub`, `disability_fallback_standard_schedule` 등이다 (`worker.py:18-19` 주석). **`persist_blocked_failed`는 일부러 뺐다** — 차단 기록은 초안 없이 status만 바꾸는 부수효과라서, 실패해도 메시지가 사라지는 게 아니라 status가 안 찍힐 뿐이고, 다시 처리해도 어차피 똑같이 차단될 것이므로, 무한 재시도를 막으려고 심각한 실패로 올리지 않는다 (`worker.py:20-22` 주석).

> 쉽게 말하면: 오류에도 등급이 있다. "서류를 아예 못 읽었다"거나 "저장에 실패했다"처럼 다시 해봐야 하는 치명적 오류는 메시지를 되돌려 재시도한다. 반면 "판례 데이터가 아직 없어 비었다"처럼 지금 상태에선 어쩔 수 없는 것들은, 불완전하더라도 그대로 결과를 남기고 넘어간다.

**멱등성**: 처리는 `report_id`를 기준으로 멱등이다(persist가 `report_drafts`를 `ON CONFLICT(report_id)`로 업서트하기 때문). 그래서 같은 작업이 두 번 와도 문제가 없다 (`worker.py:4-5`). Kafka 소비·검증·재시도·DLQ(`report-job.dlq`)·오프셋 커밋·우아한 종료는 `KafkaConsumer`가 맡고, 워커는 (1) 그래프 실행 (2) 심각한 실패 시 예외 전파, 이 둘만 한다 (`worker.py:2-4`). 진입점은 `__main__.py`로, `configure_logging()` → `init_pool(settings)` → `KafkaConsumer(REPORT_JOB_TOPIC, ReportJob, handle_job, settings=settings)` → `consumer.run()` 순서로 돌고, 마지막에 `close_pool()`을 부른다 (`__main__.py:23-36`). 작업대들이 `db.get_pool()`(전역에 하나뿐인 커넥션 풀)을 쓰므로, 그 전에 `init_pool()`이 먼저 실행돼 있어야 한다 (`__main__.py:6`).

---

## 7. `disability_rules.combine_disability_rate` — 4대 규칙 (개요)

`disability_rules.py`는 LLM도, DB도, 파일 입출력도 없이 순수하게 규칙만으로 최종 장해지급률을 계산한다("분류는 LLM, 합산은 결정론") (`disability_rules.py:1-5`). `disability_calc` 작업대가 이 함수를 부른다.

> 쉽게 말하면: 이 부분은 AI가 아니라 계산기다. "이 조건이면 이렇게 더한다"는 규칙이 코드에 박혀 있어서, 같은 입력을 넣으면 언제나 같은 값이 나온다.

**상수** (`disability_rules.py:18-20`):
```python
_TEMPORARY_MIN_YEARS = 5.0
_TEMPORARY_FACTOR = 0.20
_MAX_RATE = 100.0
```

**4대 규칙** (`disability_rules.py:6-11`, 구현 `36-82`):
1. **한시장해(일정 기간만 남는 장해)** — 존속기간이 5년 이상이면 지급률의 20%만 인정하고, 5년 미만이거나 알 수 없으면 아예 안 넣는다(0%). `_effective_rate`에서 `temporary`가 참일 때, `temporary_years >= 5`면 `round(rate*0.20, 2)`, 아니면 0.0을 돌려준다 (`disability_rules.py:23-33`).
2. **같은 신체부위는 최고치만** — 한 `body_region`(신체부위)에 장해가 여러 개 있어도 더하지 않고, 그중 가장 높은 지급률만 인정한다 (`disability_rules.py:60-70`). `by_region`으로 부위별로 묶은 뒤 `max(group, key=effective_rate)`를 고른다.
3. **다른 부위끼리는 합산** — 서로 다른 부위의 장해는 더한다 (`disability_rules.py:72-73`): `combined = round(sum(effective_rate), 2)`.
4. **상한 100%** — 합이 100을 넘으면 100으로 자른다 (`disability_rules.py:77-80`).

**반환형** (`disability_rules.py:44,82`):
```python
{"combined_rate": float, "rule_notes": list[str], "normalized_items": list[dict]}
```
입력이 비어 있으면 `{"combined_rate": 0.0, "rule_notes": ["산입 항목 없음"], "normalized_items": []}`를 돌려준다 (`disability_rules.py:46-47`). 규칙을 하나 적용할 때마다 사람이 읽을 수 있는 `rule_notes` 기록을 남긴다(예: "동일부위(팔) 2건 중 최고 30%만 인정", `disability_rules.py:70`). 더 세밀한 규칙(기존장해 공제·파생장해 세부·부위별 상한)은 다음 단계로 미룬 **미구현** 항목이라고 명시돼 있다 (`disability_rules.py:11`).

> 쉽게 말하면: 같은 팔에 상처가 둘이면 둘을 더하지 않고 더 심한 쪽 하나만 센다. 대신 팔과 다리처럼 부위가 다르면 각각 더한다. 그렇게 더한 값이 100을 넘으면 100에서 멈춘다. 왜 그렇게 계산했는지는 `rule_notes`에 한 줄씩 남겨 사람이 나중에 검토할 수 있게 한다.

---

### 참고: 관련 파일 절대경로
- `C:\Users\wkdrn\project\Ai\src\report_worker\graph.py`
- `C:\Users\wkdrn\project\Ai\src\report_worker\state.py`
- `C:\Users\wkdrn\project\Ai\src\report_worker\worker.py`
- `C:\Users\wkdrn\project\Ai\src\report_worker\__main__.py`
- `C:\Users\wkdrn\project\Ai\src\report_worker\nodes\agents.py`
- `C:\Users\wkdrn\project\Ai\src\report_worker\disability_rules.py`
