# 보험·법률 AI 엔진 — LangGraph · DB 구조 통합 아키텍처 문서

> 대상 코드: `src/report_worker` · `src/rag` · `src/guardrail` · `src/chatbot` · `src/core` · `migrations` · `tempVectorDB`
> 최종 점검일: 2026-07-15 · 브랜치 `11-feature-langgraph-멀티에이전트-구현`
> 이 문서 세트는 원본 코드와 직접 대조해 작성했고, 모든 file:line 참조는 위 브랜치 기준이다.
> 각 섹션은 코드 정독 → 적대적 교차검증(코드 재대조)을 거쳐, 확인된 사실만 담았다.

> **🎯 한 문장 요약**
> 보험 서류를 받아 자동으로 보상 리포트를 써주는 AI 엔진이, 내부에서 어떤 순서로 일하고(LangGraph), 어떤 데이터베이스를 참조하며, 그 과정에서 데이터(state)가 어떻게 바뀌는지를 정리한 지도다.

> **🌱 쉽게 말하면**
> 이 엔진은 보험 청구 서류를 넘겨받아, 사람 심사자가 하듯 "진단은 뭐고 → 약관 어디에 걸리고 → 후유장해가 있으면 지급률은 얼마고 → 예상 보상금은 얼마인지"를 순서대로 훑어 한 편의 보고서를 만드는 조립 라인이다.
> 이 조립 라인의 설계도 역할을 하는 게 **LangGraph**인데, 공정마다 작업대(노드)를 두고 "이럴 땐 이쪽 라인으로 보내라" 하는 분기까지 그려 넣은 순서도라고 보면 된다.
> 작업대들이 서로 부품을 넘길 때 쓰는 공동 서랍장이 **DB(데이터베이스)**이고, 필요한 참고자료(약관·판례·장해분류표)를 찾아오는 사서 역할이 **RAG**, 위험하거나 부적절한 말이 새어나가지 않게 지키는 문지기가 **가드레일**이다.
> 중요한 건 돈에 직결되는 최종 숫자는 AI의 감이 아니라 **정해진 계산 규칙(순수 함수)**이 만든다는 점이다 — "분류는 AI가, 합산은 계산기가" 나눠 맡는다.

이 문서는 사용자 요청 — **"우리 랭그래프 그리고 DB 구조, 어디 자료를 참조하고 뭐를 하고 어떻게 동작하고 어떤 구조에선 상태가 어떻게 바뀌는지"** — 에 답한다. 핵심 질문 5개를 축으로 구성했다:

1. **랭그래프 구조** — 어떤 노드가 어떤 순서로 실행되고 어디서 분기하나 → [langgraph.md](./langgraph.md)
2. **DB 구조** — 어떤 테이블이 있고 누가 소유하며 무엇을 담나 → [database.md](./database.md)
3. **자료 참조** — 무슨 데이터를(약관·판례·장해분류표·DB) 어디서 가져오나 → 아래 §4 + [rag.md](./rag.md)
4. **동작 방식** — 워커가 실제로 어떻게 도나(Kafka·풀·LLM·가드레일) → [core.md](./core.md) · [guardrail.md](./guardrail.md)
5. **상태 전이** — 어떤 노드에서 state의 어떤 키가 바뀌나 → [langgraph.md §5 상태 전이 종합표](./langgraph.md)

> 여기서 **노드**(node)는 "한 가지 일만 하는 작업 단계", **분기**(branch)는 "조건에 따라 갈라지는 갈림길", **상태(state)**는 "작업대들이 돌려 보며 채워 넣는 공용 서류 묶음"이라고 생각하면 된다.

---

## 1. 시스템 한눈에 — LLM 소비 컴포넌트는 둘

이 엔진에는 LLM(대형 언어 모델, 즉 사람 말로 묻고 답하는 AI)을 실제로 불러 쓰는 부분이 **두 개** 설계돼 있다. 그리고 그 아래에 세 컴포넌트가 함께 쓰는 공용 모듈(RAG·가드레일·ai_client)과, 모두가 딛고 서는 토대(core)가 깔려 있다.

| 컴포넌트 | 통신 | 오케스트레이션 | 구현 현황 |
|----------|------|-----------------|-----------|
| **리포트 워커(05)** | Kafka `report-job` 소비 | **LangGraph** StateGraph (14노드·3분기) | ✅ **구현됨** |
| **챗봇(12)** | FastAPI WebSocket 직결·비스트리밍 | 단순 4단계 순차 조립(계획) | ⚠️ **미구현(스펙 단계)** — `__init__.py` 도크스트링 1줄뿐 |
| RAG(04) — `src/rag` | 함수 호출(공용) | — | ✅ 구현됨 (리포트·챗봇 공유) |
| 가드레일(06) — `src/guardrail` | 함수 호출(공용) | — | ✅ 결정론 파트 구현 / ⚠️ NER·인용강제·환각치환 미구현 |
| core — `src/core` | — | — | ✅ 구현됨 (config·contracts·kafka·db·ai_client·logging) |

> 표 읽는 법: 왼쪽이 "어떤 부품인지", 가운데 둘이 "어떻게 말을 주고받고 어떻게 지휘되는지", 맨 오른쪽이 "지금 진짜로 돌아가는지(✅) 아직 계획뿐인지(⚠️)"다.
>
> 참고로 **Kafka**는 부품끼리 쪽지를 주고받는 우체통(메시지 대기줄), **WebSocket**은 브라우저와 서버가 전화선처럼 계속 연결돼 있는 통로, **비스트리밍**은 답을 한 글자씩 흘리지 않고 완성된 문장 한 방에 돌려주는 방식, **StateGraph**는 위에서 말한 "작업 순서도"다.

> **"우리 랭그래프"의 실체는 리포트 워커의 그래프 하나다.** 챗봇은 LangGraph를 쓰지 않고(문서상 단순 4단계 순차 파이프라인) 아직 코드가 없다. 그래서 이 문서의 LangGraph 심층은 전부 리포트 워커 기준이고, 챗봇은 계획·현황으로 별도 정리했다([chatbot.md](./chatbot.md)).

### 아키텍처 배치 (CLAUDE.md 확정 사항)

```
Frontend ──REST/WS──┐
                    ▼
             Spring Boot 게이트웨이 (업로드·JWT·S3·Kafka 발행) ── 별도 범위(Python 아님)
                    │  Kafka
        ┌───────────┴────────────┐
        ▼ ocr-job-queue          ▼ report-job
   OCR Worker(02)  ──발행──►  Report Worker(05, LangGraph)  ──► DB(reports·report_drafts·report_issues)
        │                          │  함수호출
        └──► ocr_results(DB)       ├──► RAG(04, src/rag) ──► policy/case/schedule_chunks(pgvector)
                                   └──► Guardrail(06, src/guardrail) ──► ai_client ──HTTP──► Ollama(EXAONE·qwen3)

   Chatbot(12) ──WS 직결(ALB /ws/chat)──► [미구현] ──► 같은 RAG·Guardrail·ai_client를 함수호출로 조립 예정
```

> 쉽게 말하면: 사용자가 파일을 올리면 **Spring Boot 게이트웨이**(현관·경비실 역할, 로그인·파일보관·쪽지 발행 담당이며 이 저장소 밖의 Java 코드)가 접수한다. 게이트웨이는 우체통(Kafka)에 작업 쪽지를 넣고, **OCR 워커**(사진 속 글자를 텍스트로 읽어내는 부품)가 먼저 읽어 그 결과를 DB에 남긴다. 이어 **리포트 워커**가 쪽지를 받아 보고서를 만드는데, 이때 사서(RAG)와 문지기(가드레일)를 불러 쓰고, 그 둘은 다시 **ai_client**를 통해 로컬 AI 서버 **Ollama**(모델 EXAONE·qwen3)에 HTTP로 질문을 던진다. 챗봇은 같은 재료를 쓸 예정이지만 아직 빈칸이다.

---

## 2. 전체 데이터 흐름 (리포트 생성 1회)

리포트 워커는 **ID(식별 번호)만 실린 짧은 쪽지**를 받는다. 상세 내용은 쪽지에 없고, 그 ID로 DB를 뒤져 직접 가져온다. 그리고 LangGraph를 한 번 실행(`ainvoke`)하는 것만으로 컨텍스트 조립 → 진단 → 약관/특약 분석 → (필요할 때만) 후유장해 지급률 → 보상 추정 → 리포트 조립 → 가드레일 → 저장까지 한 호흡에 끝낸다.

> 쉽게 말하면: 택배 송장에 "물건 상세" 대신 운송장 번호 하나만 적혀 오는 셈이다. 워커는 그 번호로 창고(DB)에서 실물을 찾아와 작업을 시작한다.

```
Kafka report-job (ReportJob: report_id·ocr_result_id·claim_id·user_ref·doc_type)
   │
   ▼  core.kafka.KafkaConsumer (검증·재시도·DLQ·수동 오프셋 커밋·우아한 종료)
worker.handle_job(job)  →  _graph.ainvoke(초기 state)
   │
   ▼  LangGraph StateGraph
load_context → input_guardrail →⟨차단?⟩→ persist_blocked → END
                                 └→ diagnosis →⟨약관 DB?⟩→ (terms_parse) → coverage_parse
                                     → coverage_analysis → case_search →⟨후유장해?⟩
                                        → disability_rag → disability_calc ─┐
                                        └──────────────(불필요)────────────→ payment_calc
                                     → report_compose → output_guardrail → persist → END
   │
   ▼  DB 쓰기 (한 트랜잭션)
report_drafts (UPSERT) · reports (UPDATE status=AWAITING_ADOPTION) · report_issues (DELETE+INSERT)
```

> 순서도 읽는 법: 화살표를 따라 왼쪽에서 오른쪽으로 흐르고, `⟨…?⟩`가 갈림길이다. 예를 들어 입력 가드레일에서 "차단?"이 참이면 곧장 차단 기록 후 종료(`persist_blocked → END`)하고, 후유장해가 없으면 지급률 계산을 건너뛰어 `payment_calc`로 바로 간다.
>
> 여기서 **DLQ**(Dead Letter Queue)는 "몇 번 시도해도 처리 못 한 쪽지를 따로 모아두는 반송함", **오프셋 커밋**은 "여기까지 읽었다고 우체통에 도장 찍는 것", **우아한 종료**는 "하던 작업은 마치고 안전하게 멈추는 것", **UPSERT**는 "있으면 고치고 없으면 새로 넣기", **트랜잭션**은 "여러 DB 쓰기를 전부 성공 아니면 전부 취소로 묶는 것"이다.

상세: [langgraph.md](./langgraph.md). 이 흐름에서 **돈에 직결되는 최종 장해지급률은 LLM이 아니라 결정론 순수 함수(`disability_rules.combine_disability_rate`)가 만든다** — "분류는 LLM, 합산은 결정론" 원칙. 후유장해 서브파이프라인의 심층은 별도 문서 [../report-worker/](../report-worker/README.md)에 이미 정리돼 있다.

> 쉽게 말하면: **결정론 순수 함수**란 "같은 값을 넣으면 언제나 똑같은 값이 나오고, 바깥에서 무언가를 읽거나 바꾸지 않는 계산기"다. 지급률처럼 틀리면 안 되는 숫자는 AI의 즉흥 판단에 맡기지 않고 이 계산기에 맡긴다.

---

## 3. 문서 지도

**🌱 처음 오셨다면 여기부터** — 아래 두 문서가 가장 쉽습니다.

| 문서 | 핵심 질문 | 내용 |
|------|-----------|------|
| [lifecycle.md](./lifecycle.md) | **전체 흐름(스토리)** | 파일 업로드 → OCR → 리포트 → 서명 → 삭제까지, "교통사고 후유장해" 예시로 데이터가 어떻게 바뀌는지 이야기처럼 따라가기 |
| [glossary.md](./glossary.md) | **용어 사전** | RAG·임베딩·pgvector·RRF·Kafka·멱등성 등 모든 전문용어를 일상 비유로 풀이 (모르는 단어 나오면 여기 검색) |

**📚 주제별 심층**

| 문서 | 핵심 질문 | 내용 |
|------|-----------|------|
| [langgraph.md](./langgraph.md) | **랭그래프 · 상태 전이** | build_graph 조립, `@safe_node`, 14노드 각각의 읽기/쓰기 state 키, 3개 라우터, **상태 전이 종합표**, worker 하드실패 승격, 장해 합산 규칙 |
| [database.md](./database.md) | **DB 구조** | 소유권 3분할(Python 마이그레이션 / Spring / tempVectorDB), RAG 벡터 4테이블 + 업무 7테이블 전 컬럼, ERD, persist 매핑, 멱등·타입 규약 |
| [rag.md](./rag.md) | **자료 참조(검색)** | 라우터→오타보정→tsvector∥pgvector→RRF→인용역추적, 버전 필터(반열림 구간), namespace↔테이블, 입출력 계약 |
| [guardrail.md](./guardrail.md) | **안전장치** | 입력(PII 마스킹·도메인차단)/생성(단정금액 치환)/출력(고지문·LLM Judge) 3단계, report_worker 연동, 구현 현황 정직 판별 |
| [ocr.md](./ocr.md) | **OCR 워커(02, 미구현)** | 업로드 서류를 글자로 바꾸는 단계. 현재 스펙만 존재. OcrJob/DocType 계약, 데이터 흐름 10단계, 보존·삭제 정책 |
| [chatbot.md](./chatbot.md) | **챗봇(12, 계획·현황)** | 미구현 현황, 계획된 WS 아키텍처·세션(Redis 24h·PG 90일)·이벤트 스키마, 리포트와의 차이 |
| [core.md](./core.md) | **동작 배관** | ai_client(비스트리밍·1024d 강제)·config·db(asyncpg 풀)·kafka(at-least-once·DLQ)·logging |

> 표 읽는 법: 궁금한 게 "AI 작업 순서"면 langgraph, "데이터가 어디 저장되나"면 database, "자료를 어떻게 찾아오나"면 rag로 가면 된다. 낯선 용어는 언제든 [glossary.md](./glossary.md)에서 찾을 수 있다.

관련 기존 문서: 후유장해 지급률 심층은 [../report-worker/](../report-worker/README.md)(disability-pipeline·schedule-data·known-issues) 참조.

---

## 4. 무엇이 어디서 오나 — 자료 참조 매트릭스

리포트 워커가 끌어다 쓰는 데이터는 **성격이 셋**으로 나뉜다: DB에서 그때그때 조회해 오는 것, RAG로 검색해 오는 것, LLM이 새로 지어내는 것.

| 데이터 | 소스 | 성격 | 사용 노드 |
|--------|------|------|-----------|
| 사고 컨텍스트(진단·제시금액·질문·가입특약·가입일) | `ocr_results`·`reports`·`user_claims`·`user_insurances` | **런타임 DB 조회** | `load_context` |
| 약관 조항 | `policy_chunks` (namespace=`terms`) | **RAG 검색** | `coverage_analysis`·`disability_rag` |
| 판례·분쟁조정례 | `case_chunks` (namespace=`case`) | **RAG 검색** | `case_search` |
| 표준 장해분류표(금감원 별표) | `schedule_chunks` (namespace=`level`, 계약일→개정판) | **RAG 검색(원문)** | `disability_rag` 폴백 |
| 오타 보정 사전 | `search_terms` (trigram) | **DB 조회(전처리)** | RAG 내부 `typo` |
| 진단 분류·특약 대조·리포트 본문 | Ollama EXAONE(=`llm_model`) | **LLM 생성** | `diagnosis`·`coverage_analysis`·`disability_rag`·`report_compose`·`output_guardrail`(Judge) |
| 지급률 합산 | 순수 규칙 함수 | **결정론(LLM/IO 없음)** | `disability_calc` |
| 임베딩(1024d) | qwen3:embedding → BGE-M3 폴백 | **임베딩 호출** | RAG 벡터 검색 |

> 표 읽는 법: "성격" 열이 데이터의 출처 종류를 말한다. **런타임 DB 조회**는 이미 창고에 있는 사실을 꺼내 오는 것, **RAG 검색**은 방대한 문서 더미에서 관련 부분을 찾아오는 것, **LLM 생성**은 AI가 새 문장을 지어내는 것, **결정론**은 정해진 계산 규칙이다.
>
> 여기서 **namespace**는 "검색 대상 서랍의 이름표"(약관 서랍/판례 서랍/장해분류표 서랍), **폴백**은 "1순위가 안 되면 대신 쓰는 예비 수단", **임베딩**(embedding, 문장을 숫자 좌표 1024개로 바꿔 뜻이 비슷한 글끼리 가깝게 놓는 것), **1024d**는 그 좌표가 1024차원이라는 뜻이다.

**RAG namespace ↔ 테이블 매핑** (`src/rag/search.py:40-44`):

| namespace | 테이블 | 필터 특성 |
|-----------|--------|-----------|
| `terms` | `policy_chunks` | insurer/product 메타필터 적용 |
| `case` | `case_chunks` | 메타필터 없음(적재기가 안 채워 걸면 recall 0) |
| `level` | `schedule_chunks` | 계약일 버전필터 `[applies_from, applies_to)` |
| `medical` | (미존재) | 향후 확장 |

> 표 읽는 법: 왼쪽 "서랍 이름표"가 실제로 어느 DB 테이블을 뒤지는지, 그리고 검색할 때 어떤 조건으로 범위를 좁히는지를 보여준다. **메타필터**는 "보험사·상품 같은 꼬리표로 후보를 미리 추려내는 조건"이고, `case`처럼 그 꼬리표가 비어 있으면 조건을 걸수록 오히려 아무것도 안 걸려 **recall(찾아내는 비율)이 0**이 된다는 주의사항이다. **`[applies_from, applies_to)`**는 수학의 반열림 구간 — 시작일은 포함하고 끝일은 제외하는 "계약일이 이 개정판 유효기간에 드는가"를 가르는 조건이다. `medical`은 아직 만들지 않은 향후 확장용 빈칸이다.

---

## 5. 상태(state)가 어떻게 바뀌나 — 요약

리포트 워커가 공유하는 전역 상태는 `ReportState`(`TypedDict, total=False`, 즉 "키가 정해져 있되 다 채우지 않아도 되는 사전")다. **모든 노드가 한 줄로 차례차례 실행돼 동시에 같은 칸을 건드리는 일이 없다. 그래서 충돌을 조정하는 reducer(병합기)가 필요 없다** — 각 노드는 자기가 채운 부분 사전만 돌려주고, LangGraph가 그것을 전체 상태에 합쳐 넣는다. 상태가 태어나서 완성되기까지를 요약하면 이렇다:

1. **초기 주입**: `worker.handle_job`이 `report_id`·`ocr_result_id`·`claim_id`·`user_ref`·`doc_type`만 넣는다. 나머지는 그래프가 채운다.
2. **컨텍스트 생성**: `load_context`가 `case_info`·`masked_text`·`entities`·`subscribed_coverages`를 만든다(유일한 생산자, 이후 노드는 읽기만).
3. **분석 누적**: `diagnosis`→`coverage_analysis`→`case_search`→`disability_rag`→`disability_calc`→`payment_calc`가 각각 `diagnosis`·`retrieved_clauses`·`coverage_analysis`·`legal_references`·`disability_analysis`·`estimated_range`를 추가/증강한다.
4. **산출**: `report_compose`가 `sections`·`issues`·`report`를, `output_guardrail`이 `report`(고지문 반영)·`judge_failures`를 남긴다.
5. **운영 채널 `errors[]`**: 모든 노드가 실패·폴백을 `_err`로 **누적 append**한다. `@safe_node`가 예외를 이 채널로 흡수해 "한 노드 실패 = 전체 실패"가 아니라 **부분결과 리포트**가 나오게 한다. worker는 `errors` 중 하드실패 prefix(`load_context_failed`·`persist_failed`·`ocr_result_missing`)가 있을 때만 예외를 올려 Kafka 재시도/DLQ로 보낸다.

> 쉽게 말하면: 하나의 서류 묶음(state)을 작업대들이 한 명씩 이어받아 자기 칸만 채워 나간다고 보면 된다. `load_context`가 밑그림(사고 정보·가린 텍스트·추출 항목·가입 특약)을 그리면, 뒤 작업대들은 그걸 읽어 분석 결과를 한 칸씩 덧붙인다.
>
> `errors[]`는 "작업 중 삐끗한 일을 적어 두는 비고란"이다. `@safe_node`(각 작업대를 감싸는 보호 장치)가 사고를 이 비고란에 받아 적어, 한 군데가 넘어져도 라인 전체가 멈추지 않고 **채운 데까지의 보고서**를 낸다. 다만 밑그림 자체를 못 그렸거나(`load_context_failed`) 저장에 실패한(`persist_failed`) 등 **치명적 실패(하드실패)**일 때는 조용히 넘기지 않고 진짜 오류로 올려, 우체통(Kafka)이 재시도하거나 반송함(DLQ)으로 보내게 한다. **append**는 "덮어쓰지 않고 뒤에 계속 이어 붙이는 것"이다.

노드별 정확한 "읽는 키 → 쓰는 키 → 부수효과" 전체 표는 [langgraph.md §5](./langgraph.md)에 있다.

---

## 6. 구현 현황 — 정직 요약

무엇이 진짜로 돌아가고(✅) 무엇이 아직 계획·미완성(⚠️)이며 무엇이 짚어둘 사항(📌)인지를, 부풀리지 않고 있는 그대로 적었다.

- ✅ **리포트 워커 LangGraph**: 14노드·3분기 실동작. 상태 전이·하드실패 승격·멱등 저장까지 구현.
- ✅ **RAG**: 라우터·trigram 오타보정·tsvector·pgvector·RRF·버전필터·인용역추적 실동작. `medical` namespace만 미구현.
- ✅ **core 배관**: config·kafka(at-least-once·DLQ·우아한종료)·db(asyncpg 풀·pgvector 등록)·ai_client(비스트리밍·1024d 강제)·logging 실동작.
- ⚠️ **가드레일**: 결정론 파트(PII 정규식 마스킹·단정금액 치환·고지문·LLM Judge 호출)는 실동작. **문서가 약속한 NER PII·인용 강제 검증·환각 섹션 자동삭제는 미구현.** LLM Judge는 "탐지는 하되 조치는 안 하는" 관찰 전용.
- ⚠️ **챗봇**: **본체 미구현.** `app.py`·WS 핸들러·세션·JWT·Redis·PG 연동 없음. WS 계약 모델(`ChatClientMessage`/`ChatServerMessage`)도 코드에 미정의. 조립 재료(RAG·가드레일·ai_client)는 준비돼 있어 구현은 "조립" 작업.
- 📌 **DB 소유권**: Python 마이그레이션은 RAG 벡터 4테이블만 소유. 업무 7테이블(`reports` 등)은 Spring 소유이며 리포트 워커는 읽기/UPDATE만. `tempVectorDB/init`은 실험용 로컬 복제본이고 물리 FK가 없다.
- 📌 **알려진 계약 갭**: `reports.status='BLOCKED'`는 워커가 신설한 값으로 Spring status enum과 아직 미정렬(TODO).

> 용어 도우미: **trigram**(글자 3개씩 잘라 비교해 오타를 잡아내는 방식), **pgvector**(PostgreSQL에서 임베딩 좌표로 비슷한 문장을 찾게 해주는 확장), **at-least-once**("최소 한 번은 반드시 처리 — 대신 드물게 중복될 수 있음"), **asyncpg 풀**(DB 연결을 미리 여러 개 뚫어 재사용하는 대기줄), **NER**(문장에서 사람 이름·주민번호 같은 개체를 자동으로 집어내는 기술), **PII**(개인식별정보), **LLM Judge**(다른 AI의 답이 근거에 맞는지 채점하는 심판 AI), **JWT**(로그인 신분증 토큰), **FK**(테이블 간 참조 무결성을 강제하는 외래키 제약), **enum**(미리 정해둔 값 목록)이다.
>
> 쉽게 말하면: 리포트 엔진과 그 배관·검색은 실제로 돌아간다. 반면 가드레일은 "규칙 기반으로 확실히 막는 부분"은 되지만 AI로 판단해야 하는 고급 방어(NER·인용 강제·환각 자동삭제)는 아직 안 붙었고, 심판 AI는 "잘못을 발견해도 지적만 하고 손은 대지 않는" 관찰자 상태다. 챗봇은 재료만 갖춰졌고 본체는 빈 접시다.

후유장해 계산의 알려진 버그(좌우 부위 미구분·검증 백스톱 헐거움 등)는 [../report-worker/known-issues.md](../report-worker/known-issues.md)에 우선순위와 함께 정리돼 있다.

---

## 7. 이 문서의 근거·검증 방법

- 각 서브시스템을 코드 정독으로 1차 작성한 뒤, **동일 파일을 재대조하는 적대적 교차검증**을 돌려 file:line·동작 주장을 확인했다(2026-07-15).
- 검증에서 발견된 라인번호 오기·인용 출처 오류는 반영했다. "확인된 사실"만 남기고, 코드로 확인 못 한 부분(예: 챗봇 런타임, Docker 미기동 경로)은 정직하게 "미구현/미검증"으로 표기했다.
- 통합 검증(RAG 실검색·E2E)은 `docker compose up -d` 후 `PYTHONPATH=src python scripts/battery.py`로 별도 수행이 필요하다(본 문서는 정적 코드 대조 범위).

> 쉽게 말하면: 먼저 코드를 정독해 초안을 쓰고, 그다음 "일부러 트집 잡듯" 같은 코드를 다시 대조하며 줄 번호와 동작 설명이 맞는지 재확인했다(**적대적 교차검증**). 코드만 봐서는 확신할 수 없는 부분은 아는 척하지 않고 "미검증"으로 남겼다. 실제로 검색이 되는지, 처음부터 끝까지(**E2E**, end-to-end) 도는지까지 보려면 위 명령으로 컨테이너를 띄워 별도로 돌려봐야 한다 — 이 문서 자체는 실행 없이 코드를 대조한 범위까지다.
