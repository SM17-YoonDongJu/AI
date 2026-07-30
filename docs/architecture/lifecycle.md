# 전체 생애주기(End-to-End) — 파일 업로드부터 최종 리포트, 그리고 삭제까지

> 출처: AI 엔진 아키텍처 문서 세트 · 최종 점검일 2026-07-15 · 브랜치 `11-feature-langgraph-멀티에이전트-구현`
> 상위: [README](./README.md) · 아키텍처 문서 세트 + `.claude/docs/02_ocr.md` 종합

## 🎯 한 문장 요약
사용자가 보험 서류(진단서 등) 사진을 올리면, 시스템이 글자를 읽어내고 개인정보를 지운 뒤 약관·판례·장해분류표를 뒤져 손해사정 리포트 **초안**을 자동으로 써 주고, 손해사정사가 검수·서명하면 원본 개인정보는 즉시 지워지는 한 바퀴의 흐름이다.

## 🌱 쉽게 말하면
보험금 청구를 **패스트푸드 주문**처럼 생각해 보자.

- 손님(사용자)이 창구(**Spring Boot 게이트웨이**)에 서류를 내밀면, 창구는 서류를 금고(**S3**)에 넣고 주방에 "주문 들어왔어요"라고 **주문표를 우체통(Kafka)** 에 넣는다. 손님한테는 "접수됐어요(202)"라고 바로 답한다. 손님은 음식이 다 될 때까지 창구 앞에서 기다리지 않는다.
- 주방에는 요리사가 둘 있다. 첫 번째 요리사(**OCR 워커**)는 서류 사진에서 글자를 읽어내고 주민번호 같은 민감한 부분을 검게 칠한다. 두 번째 요리사(**리포트 워커**)는 그 글자를 받아 "이 사람 약관에 이게 되나? 비슷한 판례는? 후유장해면 지급률은 몇 %?"를 차례로 조사해 **리포트 초안**을 완성한다.
- 다만 요리사가 함부로 "당신은 3,000만 원 받습니다"라고 단정하지 못하게 막는 **안전요원(가드레일)** 이 옆에 붙어 있다. 초안이 완성되면 사람 전문가(**손해사정사**)가 최종 확인하고 서명한다.
- 서명이 끝나는 순간, 검게 칠했더라도 남아 있던 **개인정보 원본은 곧바로 파기**한다. 서류 사진 원본만 법이 정한 기간(3년) 동안 금고에 보관된다.

> 이 문서에서 "우체통"은 Kafka, "글자 읽기"는 OCR, "약관 뒤지기"는 RAG 검색, "안전요원"은 가드레일에 해당한다. 아래에서 각 단계를 실제 데이터가 어떻게 바뀌는지까지 따라간다.

---

## 1. 등장인물(컴포넌트) 소개

한 줄씩만 기억하면 나머지가 쉬워진다.

| 등장인물 | 한 줄 역할 | 구현 현황 |
|----------|-----------|-----------|
| **Frontend** (React Native / Next.js) | 사용자가 서류(PDF·JPG·PNG·TIFF)를 고르고 올리는 화면 | 별도 범위 |
| **Spring Boot 게이트웨이** | 파일 수신·JWT 검증·S3 저장·Kafka 발행을 맡는 정문. 응답은 곧바로 202 | 별도 범위(Python 아님) |
| **AWS S3** | 업로드된 파일 원본을 암호화해 보관하는 금고 | 인프라 |
| **Kafka** | 컴포넌트끼리 작업을 비동기로 주고받는 우체통(`ocr-job-queue`·`report-job`) | 인프라 |
| **OCR 워커(02)** | 사진에서 글자를 읽고(PaddleOCR) 문서를 분류하고 개인정보를 가린다 | ⚠️ **미구현(스펙 단계)** |
| **리포트 워커(05)** | `report-job`을 받아 LangGraph 14노드로 리포트 초안을 짓는다 | ✅ **구현됨** |
| **공용 모듈 — RAG(04)** | 약관·판례·장해분류표를 하이브리드로 검색해 근거를 찾아 준다 | ✅ 구현됨 (`medical` namespace만 미구현) |
| **공용 모듈 — 가드레일(06)** | 개인정보 마스킹·단정 금액 치환·고지문 삽입 등 안전장치 | ⚠️ 결정론 파트만 구현 (NER·인용강제·환각치환 미구현) |
| **공용 모듈 — ai_client(core)** | Ollama EXAONE(GPU 노드)를 HTTP로 호출하는 LLM 창구. 비스트리밍 | ✅ 구현됨 |
| **DB** (PostgreSQL) | 모든 상태와 결과를 담는 창고. RAG 벡터 4테이블 + 업무 7테이블 | ✅ 구현됨 |
| **챗봇(12)** | 사용자와 실시간으로 대화하는 상담원(WebSocket 직결) | ⚠️ **미구현(스펙 단계)** |
| **손해사정사** | AI 초안을 검수하고 최종 서명하는 사람 전문가 | 사람 |

> 표 읽는 법: "별도 범위"는 이 Python 엔진 밖(Spring/프론트)이라는 뜻이고, "미구현(스펙 단계)"은 문서·골격만 있고 실행 코드가 아직 없다는 뜻이다. 초록 체크(✅)만 지금 실제로 돈다.

---

## 2. 오늘의 주인공 — 교통사고 후유장해 진단서 한 장

이야기를 하나로 관통하기 위해 구체적 사용자를 세운다.

> **시나리오**: 김씨가 교통사고를 당해 팔에 후유장해가 남았다. 병원에서 받은 **진단서**를 앱으로 촬영해 올린다. 진단서에는 진단명·주민번호·사고 정황이 적혀 있다.

이 한 장이 시스템을 지나며 어떻게 **글자 → 마스킹 텍스트 → 컨텍스트 → 분석 → 리포트 초안 → 서명된 리포트 → 삭제**로 모습을 바꾸는지가 이 문서의 전부다.

---

## 3. 단계별 흐름 — 데이터가 바뀌는 순간을 따라

### 3-1. 업로드 — Frontend → Spring Boot → S3 · Kafka

김씨가 진단서를 올리면 Frontend가 멀티파트 폼(여러 파일을 한 번에 실어 보내는 HTTP 전송 형식)으로 **Spring Boot 게이트웨이**에 보낸다(ALB로 직접 진입). 게이트웨이가 하는 일은 네 가지다.

1. **JWT(RS256) 검증** — 로그인 토큰이 위조가 아닌지 서버 보관 없이(스테이트리스) 확인한다.
2. **S3 저장** — 파일명을 UUID로 바꾸고 서버사이드 암호화(SSE-S3, 저장 시 자동 암호화)로 금고에 넣는다.
3. **Kafka 발행** — `ocr-job-queue` 토픽에 "이 파일 OCR 해 주세요" 주문표를 넣는다.
4. **202 즉시 반환** — 김씨에게는 바로 "접수됨"만 답한다. 결과는 나중에 Push로 안내된다.

> 쉽게 말하면: 정문 직원이 서류를 금고에 넣고 주방 우체통에 주문표만 던진 뒤, 손님에게 "접수됐어요"라고 즉답하는 단계다. 아직 아무 글자도 읽지 않았다.

이 구간의 통신 방식은 다음과 같다.

| 구간 | 방식 |
| --- | --- |
| Frontend → Spring Boot | REST (multipart/form-data, ALB 직접 진입) |
| Spring Boot → S3 | AWS SDK (PutObject, SSE-S3) |
| Spring Boot → Kafka | Kafka Producer |

> 참고: 초기 설계에서는 이 정문을 FastAPI가 맡는 안도 있었으나, 현재 확정본은 **Spring Boot 게이트웨이**가 업로드·JWT·S3·Kafka를 담당한다(CLAUDE.md 확정 사항).

### 3-2. OCR — 글자를 읽고 개인정보를 가린다 ⚠️(미구현·스펙)

`ocr-job-queue`를 소비하는 **OCR 워커**(GPU 노드)가 처리하도록 **설계**돼 있다. 순서는 이렇다.

1. S3에서 파일을 읽어 **PaddleOCR**(로컬 GPU에서 도는 오픈소스 글자 인식 엔진)로 텍스트를 뽑는다. 한국어 모델을 쓰고 표·인감·서명 영역까지 레이아웃을 분석한다.
2. 첫 페이지로 **문서 유형**을 판정한다: 진단서·보험증권·지급결과안내문·청구서·기타(`diagnosis|policy|payout_notice|claim|other`). 김씨 서류는 `diagnosis`(진단서)로 분류된다.
3. 유형에 맞춰 **엔티티 추출** — 진단명(KCD 매핑)·보험사명·상품명·지급금액 등.
4. **PII 마스킹** — 주민번호·계좌번호·전화번호 등을 정규식 + NER(문맥으로 이름·기관 같은 개체를 찾아내는 개체명 인식)로 탐지해 가린다. **이후 파이프라인에는 마스킹된 텍스트만 넘어간다.**
5. 결과를 `ocr_results` 테이블에 저장한다(마스킹 텍스트·문서 유형·엔티티).

> 외부 OCR API(Google Vision·Azure OCR 등)는 개인정보보호법 위반 우려로 쓰지 않는다. 모든 인식은 **로컬 GPU**에서만 한다.

**상태 변화**: 진단서 사진 한 장이 → `ocr_results` 한 행(row)으로 바뀐다. 주요 컬럼은 아래와 같다.

| 컬럼 | 담기는 것 |
|------|-----------|
| `id` (UUID, PK) | 이 OCR 결과의 식별자. 다음 단계 `ReportJob.ocr_result_id`가 이걸 가리킨다 |
| `doc_type` | `diagnosis` |
| `masked_text` | 주민번호 등이 가려진 진단서 본문 |
| `entities` (JSONB) | 추출된 진단명·보험사명 등 |

> ⚠️ **정직 표기**: OCR 워커는 아직 **스펙 단계**다(`.claude/docs/02_ocr.md`). 위 5단계는 설계된 동작이며, 실제로는 `ocr_results` 행이 다른 경로(예: Spring/수동 적재)로 채워진다고 가정하고 리포트 워커가 그 뒤를 잇는다.

### 3-3. 작업 넘기기 — `report-job` 메시지 발행

OCR 결과가 `ocr_results`에 저장되면, 리포트 생성 파이프라인(05번)으로 작업이 넘어간다. 넘어가는 방식은 다시 **Kafka 우체통**이다. 이번 주문표는 `report-job` 토픽에 실리고, 내용물은 **ID만 담은 가벼운 메시지**다.

```
ReportJob { report_id · ocr_result_id · claim_id · user_ref · doc_type }
```

> 쉽게 말하면: 주문표에는 요리 재료를 통째로 싣지 않는다. "몇 번 서랍(ID)을 열어 보세요"라는 **번호표**만 적는다. 실제 재료(마스킹 텍스트·청구·가입정보)는 리포트 워커가 DB에서 직접 꺼낸다. 그래서 메시지가 가볍고, 같은 번호표가 두 번 와도 결과가 같다(멱등).

### 3-4. 리포트 워커 — LangGraph 14노드가 초안을 짓는다 ✅

여기가 시스템의 심장이다. **리포트 워커**는 `report-job`을 받아 한 번의 LangGraph 실행(`_graph.ainvoke`)으로 리포트 초안을 완성한다. LangGraph는 "각 단계(노드)를 정해진 순서와 갈림길(분기)로 잇는 작업 흐름도"라고 보면 된다. 노드는 14개, 갈림길(라우터)은 3개다.

전체 경로는 다음과 같다.

```
load_context → input_guardrail →⟨차단?⟩→ persist_blocked → END
                                 └→ diagnosis →⟨약관 DB?⟩→ (terms_parse) → coverage_parse
                                     → coverage_analysis → case_search →⟨후유장해?⟩
                                        → disability_rag → disability_calc ─┐
                                        └──────────────(불필요)────────────→ payment_calc
                                     → report_compose → output_guardrail → persist → END
```

김씨 사례(교통사고·후유장해)를 이 흐름에 태워 노드별로 따라가 보자.

**① `load_context` — 재료 꺼내기.** DB 4개 테이블에서 컨텍스트를 조립한다(`agents.py:65-119`). 번호표(ID)로 서랍을 연다.
- `ocr_results`에서 `masked_text`·`entities`
- `reports`에서 사고유형·질문·`claim_id`
- (`claim_id`가 있으면) `user_claims`에서 진단명·사고일·제안금액 등
- `user_insurances`에서 보험사·상품명·가입특약(`coverages`)·가입일(`enrolled_at`)

이걸 합쳐 `case_info`·`masked_text`·`entities`·`subscribed_coverages`를 만든다. `enrolled_at`(가입일)은 나중에 "김씨 계약 시점에 유효했던 장해분류표 판(version)"을 고르는 열쇠가 된다.
> OCR 결과가 비면 `errors`에 `ocr_result_missing`을 남긴다 — 이건 뒤에서 "재시도해야 할 하드 실패"로 취급된다.

**② `input_guardrail` — 입구 검문.** 마스킹 텍스트를 한 번 더 가리고(재마스킹), 보험·법률과 무관한 내용이면 차단한다(`agents.py:123-129`). 김씨 진단서는 통과.
- 만약 차단되면 갈림길 `route_after_input`이 `blocked`을 반환해 **뒤의 LLM 단계를 전부 건너뛰고** `persist_blocked`로 직행한다. 비용·오출력을 막고, 상태가 "처리중"에 영원히 갇히는 것을 막기 위함이다.

**③ `diagnosis` — 진단 분석(LLM).** 마스킹 텍스트를 LLM에 주고 진단명·ICD·사고유형(7종 중 하나)·**후유장해 검토 필요 여부**(`requires_disability_review`)를 JSON으로 뽑는다(`agents.py:136-167`). 김씨는 `accident_type=traffic`, `requires_disability_review=true`로 판정된다. 이 true 값이 나중 갈림길을 가른다.

**④ 갈림길 `policy_in_db` — 우리 DB에 이 약관이 있나?** 김씨 보험사(+상품명)로 `policy_chunks`를 카운트한다(`agents.py:171-191`). 있으면 `coverage_parse`로 직행, 없거나 조회 실패면 `terms_parse`를 거친다.
- `terms_parse`는 "사용자가 올린 약관을 그 자리에서 파싱"하는 자리인데 **아직 미구현 스텁**이라, 항상 `policy_not_in_db:runtime_parse_stub` 마커만 남기고 통과한다(`agents.py:203-207`).
- `coverage_parse`는 가입특약을 그대로 흘려보내는 사실상 no-op이자 **두 경로가 다시 만나는 합류점**이다(`agents.py:211-213`).

**⑤ `coverage_analysis` — 약관 대조(RAG + LLM).** 하이브리드 RAG로 약관 조항을 찾고(`namespaces=["terms"]`), LLM이 김씨의 가입특약과 대조해 적용 가능/누락 가능 특약과 면책 분석을 만든다(`agents.py:217-262`). 검색 결과가 비면 `rag_empty`를 남긴다.

> 쉽게 말하면: "김씨가 든 특약으로 이 사고가 보장되나?"를 약관 원문을 뒤져 짚어 주는 단계다. RAG는 오타 보정 → 키워드(tsvector) 검색과 의미(벡터) 검색을 함께 돌려 순위를 합치는(RRF) 하이브리드 검색이다.

**⑥ `case_search` — 판례·분쟁조정 근거(RAG).** 진단명으로 `case` namespace를 검색한다(`agents.py:266-275`). 다만 판례 데이터가 아직 적재되지 않은 상태를 전제로, 결과가 비면 `case_data_missing`을 남기고 부분결과로 넘어간다.

**⑦ 갈림길 `route_after_case` — 후유장해 검토가 필요한가?** `diagnosis.requires_disability_review`가 true면 `disability`(장해 서브파이프라인)로, 아니면 `payment_calc`로 직행한다(`agents.py:279-283`). 김씨는 **true**이므로 장해 경로로 간다.

**⑧ `disability_rag` — 장해분류표 조회·분류(RAG + LLM).** 후유장해 지급률을 정하려면 "장해분류표"라는 표가 필요하다(`agents.py:382-452`).
- 1차로 가입 약관(`terms`)에서 표를 찾고, 없으면 김씨 가입일(`enrolled_at`)에 맞는 **금감원 표준 장해분류표**(`level` namespace)로 폴백한다.
- LLM은 표 원문에 실제로 적힌 숫자만 인정하도록 강하게 제약된다("표에 없는 지급률은 절대 만들지 마라"). 각 항목은 지급률 숫자가 원문에 실재하는지 검증(`verified`)받는다.
> 이것이 "분류는 LLM, 합산은 결정론" 원칙의 앞 절반이다. AI는 "어느 부위·어느 항목인지"만 고르고, 실제 숫자는 표에서 복사·검증한다.

**⑨ `disability_calc` — 지급률 합산(결정론, LLM 없음).** 검증된 항목만 골라 순수 규칙 함수 `combine_disability_rate`로 최종 합산 지급률을 계산한다(`agents.py:456-468`). 규칙은 4가지다.

1. **한시장해** — 존속 5년 이상이면 지급률의 20%만, 5년 미만/미상은 미산입(0%).
2. **동일 부위 최고치만** — 같은 신체부위의 여러 장해는 합산하지 않고 최고치만.
3. **다른 부위 합산** — 서로 다른 부위끼리는 더한다.
4. **상한 100%** — 합계가 100%를 넘으면 100%로 캡.

> 쉽게 말하면: 돈에 직결되는 최종 숫자는 **AI가 아니라 계산기(고정된 규칙)** 가 만든다. AI가 실수로 부풀린 숫자가 그대로 보험금이 되는 사고를 막기 위해서다.

**⑩ `payment_calc` — 추정 보상 범위(단정 금지).** 제안금액을 바탕으로 하한·상한을 잡는다(`agents.py:472-482`). 장해지급률이 있으면 상단 배수를 `1.0 + min(rate,100)/100 * 0.8`로(0%→×1.0, 100%→×1.8), 없으면 ×1.8로 둔다. 이 노드는 **장해 경로와 직행 경로가 다시 만나는 합류점**(2 incoming)이다.

**⑪ `report_compose` — 리포트 본문 조립(LLM 2회 + 생성 가드레일).** 9개 섹션(사건요약·적용특약·누락가능특약·약관근거·판례근거·추정보상범위·장해지급률·본문·추가확인필요)과 쟁점(issues)을 Markdown으로 엮는다(`agents.py:486-557`). 이때 **생성 가드레일**이 "정확히 3,000만 원"처럼 단정하는 금액 표현을 "참고 추정 범위"로 바꾼다.

**⑫ `output_guardrail` — 출구 검문(고지문 + LLM Judge).** 법적 고지문을 삽입하고, LLM Judge가 인용이 실제 근거와 맞는지 검증한다(`run_judge=True`, `agents.py:561-566`). 실패는 `judge_failures`로 따로 기록한다.
> ⚠️ 정직 표기: LLM Judge는 현재 "탐지는 하되 조치는 안 하는" **관찰 전용**이다. 문서가 약속한 인용 강제·환각 섹션 자동삭제는 미구현이다.

**⑬ `persist` — 저장(정상 종료).** 한 트랜잭션 안에서 세 테이블에 쓴다(`agents.py:570-630`).

```sql
INSERT INTO report_drafts (report_id, draft, status)
   VALUES ($1, $2::jsonb, 'draft')
   ON CONFLICT (report_id) DO UPDATE SET draft = EXCLUDED.draft, status = 'draft'
```

```sql
UPDATE reports SET
   applicable_guarantees = $2, omitted_special_contract = $3,
   basis_terms_precedents = $4, claimed_min_amount = $5, claimed_max_amount = $6,
   status = 'AWAITING_ADOPTION', updated_at = now()
 WHERE id = $1
```

그리고 `report_issues`는 **먼저 지우고 다시 넣는다**(`DELETE ... WHERE report_id=$1` 후 INSERT).

> 쉽게 말하면: `report_drafts`는 "충돌 시 덮어쓰기(ON CONFLICT)"라 같은 작업을 두 번 처리해도 초안이 하나만 남는다. 이게 **멱등(같은 걸 여러 번 실행해도 결과가 한 번 한 것과 같음)** 이다. 쟁점도 "지우고 다시 넣기"라 재실행해도 중복이 쌓이지 않는다.

이 단계에서 김씨 리포트의 상태(`reports.status`)가 **`AWAITING_ADOPTION`**(손해사정사 채택 대기)으로 바뀐다.

**⑬′ `persist_blocked` — 차단 종료.** ②에서 입구 검문에 걸렸다면 초안 없이 상태만 바꾼다.

```sql
UPDATE reports SET status = 'BLOCKED', updated_at = now() WHERE id = $1
```

> ⚠️ 정직 표기: `BLOCKED`는 워커가 새로 만든 상태 값으로, Spring 쪽 status enum(`AWAITING_INSPECTION|AWAITING_ADOPTION|...`)과 **아직 정렬되지 않았다**(TODO, `agents.py:642-643`).

#### 노드별 상태 전이 한눈에

| 노드 | 읽는 키 | 추가/갱신하는 키 | 부수효과 |
|------|---------|------------------|----------|
| `load_context` | `ocr_result_id`, `report_id` | `case_info`, `masked_text`, `entities`, `subscribed_coverages`, `errors` | DB 4쿼리. errors: `ocr_result_missing` |
| `input_guardrail` | `masked_text` | `masked_text`, (차단시)`errors` | 가드레일 `guard_input`. errors: `input_blocked:{reason}` |
| `diagnosis` | `masked_text`, `case_info` | `diagnosis`, (실패시)`errors` | LLM. errors: `diagnosis_llm_failed` |
| `coverage_analysis` | `case_info`, `diagnosis`, `subscribed_coverages` | `retrieved_clauses`, `applicable_coverages`, `missing_coverages`, `coverage_analysis` | RAG(`terms`) + LLM. errors: `rag_empty` |
| `case_search` | `case_info`, `diagnosis` | `legal_references` | RAG(`case`). errors: `case_data_missing` |
| `disability_rag` | `case_info`, `diagnosis`, `retrieved_clauses` | `disability_analysis`, `retrieved_clauses`(merged) | RAG(`terms`)+폴백(`level`) + LLM. errors: `disability_schedule_missing`, `disability_fallback_standard_schedule` |
| `disability_calc` | `disability_analysis` | `disability_analysis`(+combined_rate 등) | 결정론 `combine_disability_rate` |
| `payment_calc` | `case_info`, `disability_analysis` | `estimated_range` | — |
| `report_compose` | 다수 | `sections`, `issues`, `report` | LLM 2회 + 생성 가드레일 |
| `output_guardrail` | `report`, `retrieved_clauses` | `report`, `judge_failures` | 출력 가드레일(LLM Judge) |
| `persist` | 다수 | `errors`(패스스루) | DB 트랜잭션(drafts UPSERT, reports UPDATE=`AWAITING_ADOPTION`, issues DELETE+INSERT) |
| `persist_blocked` | `errors`, `report_id` | `errors`(패스스루) | DB `UPDATE reports SET status='BLOCKED'` |

> 표 읽는 법: `case_info`는 `load_context`만 만들고 이후 노드는 읽기만 한다. 노드가 전부 순차 실행이라 동시 쓰기가 없어, 각 노드는 부분 dict만 반환하고 LangGraph가 합쳐 준다. `errors`만 모든 노드가 누적해서 덧붙인다.

#### 한 노드가 실패해도 리포트는 나온다 — `@safe_node`

14개 노드에는 모두 `@safe_node`가 붙어 있다(`agents.py:30-44`). 노드가 예외를 던지면 그래프 전체가 죽는 대신 `{"errors": [...]}` 한 줄만 남기고 다음 노드로 넘어간다.

> 쉽게 말하면: 요리 한 접시가 타도 **전체 코스를 엎지 않고** "이 접시는 문제 있었음"이라고 메모만 남긴 채 나머지를 마저 낸다. 그래서 판례 데이터가 비어도, 약관을 못 찾아도, **부분결과 리포트**는 나온다.

단, 예외가 있다. 아래 세 가지는 "빈손 리포트"라 다시 처리해야 하므로 워커가 예외를 올려 Kafka 재시도/DLQ(처리 실패 메시지를 따로 모으는 큐)로 보낸다(`worker.py:23`).

```python
_HARD_FAILURE_PREFIXES = ("load_context_failed", "persist_failed", "ocr_result_missing")
```

반대로 `rag_empty`·`case_data_missing`·`disability_schedule_missing`·`input_blocked:*` 등은 **정상적인 부분결과**로 보고 그대로 커밋한다. `persist_blocked`가 실패해도 재시도하지 않는다(차단은 여러 번 해도 같은 결과라 무한 재시도를 피한다).

### 3-5. 손해사정사 검수·서명

리포트 상태가 `AWAITING_ADOPTION`이 되면, **손해사정사**(사람 전문가)가 초안을 검토한다. AI가 만든 것은 어디까지나 **초안**이며, 각 쟁점(`report_issues`)은 `ai_status`(`CONFIRMED|TRUSTED|INFO`)와 손해사정사의 검토 상태(`review_status`: `PENDING|ACCEPTED|MODIFIED|EXCLUDED`)를 갖는다. 손해사정사가 의견을 달거나 수정한 뒤 **서명**하면 리포트가 확정된다.

> 이 서명이 다음 단계(삭제)의 방아쇠다.

### 3-6. 서명 완료 → 개인정보 원본 삭제

손해사정사 **서명 완료 이벤트**가 발생하면, 해당 `ocr_results` 레코드(마스킹 텍스트·엔티티)를 **즉시 삭제**한다. 마스킹을 했더라도 개인정보에 가까운 원자료는 목적을 다하는 즉시 지운다는 원칙이다.

> 쉽게 말하면: 요리가 손님상에 확정돼 나가는 순간, 주방에 남은 **재료 부스러기(개인정보)** 를 바로 버린다. 완성된 요리(리포트)는 남기지만, 재료 자체는 오래 두지 않는다.

---

## 4. 구현됨 vs 미구현 — 정직 요약

| 컴포넌트 | 현황 | 비고 |
|----------|------|------|
| **리포트 워커(05) LangGraph** | ✅ 구현됨 | 14노드·3분기 실동작, 상태 전이·하드실패 승격·멱등 저장까지 |
| **RAG(04)** | ✅ 구현됨 | 라우터·trigram 오타보정·tsvector·pgvector·RRF·버전필터·인용역추적. `medical` namespace만 미구현 |
| **core 배관** | ✅ 구현됨 | config·kafka(at-least-once·DLQ·우아한 종료)·db(asyncpg 풀)·ai_client(비스트리밍·1024d 강제)·logging |
| **가드레일(06)** | ⚠️ 부분 | PII 정규식 마스킹·단정금액 치환·고지문·LLM Judge 호출은 동작. **NER PII·인용 강제·환각 섹션 자동삭제는 미구현.** LLM Judge는 관찰 전용 |
| **OCR 워커(02)** | ⚠️ 미구현(스펙) | PaddleOCR·문서분류·엔티티추출·PII 마스킹은 설계만. `ocr_results`를 채우는 실행 코드 없음 |
| **챗봇(12)** | ⚠️ 미구현(스펙) | `app.py`·WS 핸들러·세션·JWT·Redis·PG 연동 없음. WS 계약 모델(`ChatClientMessage`/`ChatServerMessage`)도 `core/contracts.py`에 미정의 |

> 챗봇은 이 생애주기(업로드→리포트)와는 **별개의 실시간 상담 창구**다. 사용자가 "교통사고 후유장해 보상 받을 수 있나요?"를 물으면 입력 가드레일 → RAG 검색 → LLM 생성(완성) → 출력 가드레일의 4단계로 답을 1회 돌려주는 설계다(비스트리밍). 조립 재료(RAG·가드레일·ai_client)는 이미 있으므로, 챗봇 구현은 이들을 FastAPI WebSocket 위에서 순서대로 부르는 "조립" 작업으로 수렴한다. 리포트 워커와 달리 챗봇은 출력 가드레일을 `run_judge=False`로 불러 LLM Judge를 건너뛴다(온라인 대화 지연을 낮추려고).

---

## 5. 자료는 어디서 오나 — RAG namespace ↔ 테이블

리포트 워커가 "근거"를 찾을 때 뒤지는 창고는 RAG 벡터 테이블이다. namespace(검색 대상 이름표)와 실제 테이블의 대응은 이렇다(`src/rag/search.py:40-44`).

| namespace | 테이블 | 담긴 것 | 필터 특성 |
|-----------|--------|---------|-----------|
| `terms` | `policy_chunks` | 신체 관련 보험 약관 | 보험사/상품 메타필터 적용 |
| `case` | `case_chunks` | 판례·금감원 분쟁조정례 | 메타필터 없음(적재기가 안 채워 걸면 recall 0) |
| `level` | `schedule_chunks` | 표준 장해분류표(금감원 별표) | 계약일 버전필터 `[applies_from, applies_to)` |
| `medical` | (테이블 미존재) | HIRA 수가·KCD | **미구현** — 향후 확장 |

> 표 읽는 법: 세 벡터 테이블 모두 `embedding halfvec(1024)`(문장을 1024개 숫자 좌표로 바꾼 것, 반정밀 float16)로 같은 벡터공간을 쓴다. 임베딩(문장을 좌표로 바꾸는 일)은 qwen3:embedding 1024차원을 쓰고 BGE-M3로 폴백한다. `[applies_from, applies_to)`는 "시작일 포함, 종료일 미포함"의 반열림 구간이라, 계약일이 딱 그 판이 유효한 기간 안에 드는 버전만 고른다는 뜻이다.

---

## 6. 데이터 보존·삭제 정책

| 데이터 | 보존 기간 | 삭제 트리거 |
| --- | --- | --- |
| **S3 원본 파일** | 손해사정사 서명일로부터 **3년** | 보험금 청구권 소멸시효(3년) 만료 시 S3 Lifecycle로 자동 삭제 |
| **`ocr_results`** (마스킹 텍스트·엔티티) | 리포트 확정 전까지만 | 손해사정사 **서명 완료 이벤트 발생 시 즉시 삭제** |
| **AI 리포트 초안** (`report_drafts`, JSONB) | 영구 보존 | 사용자 탈퇴 요청 시 별도 검토 |
| **최종 리포트** (`reports` 레코드·PDF) | 영구 보존 | 사용자 탈퇴 요청 시 별도 검토 |

**설계 근거**
- AI 리포트 초안은 손해사정사 검수·서명의 **근거 자료**라 DB에 남긴다.
- 개인정보보호법의 **최소 수집·목적 외 보존 금지** 원칙을 지켜, 목적을 다한 개인정보(원본 서류·마스킹 텍스트)는 빠르게 지운다.

> 쉽게 말하면: **결과물(리포트)** 은 오래 남기되, **개인정보(서류 원본·OCR 텍스트)** 는 법이 허락하는 최소 기간만 붙든다. 서명이 끝난 OCR 텍스트는 곧바로, 서류 원본 사진은 3년 뒤 자동으로 사라진다.

---

## 7. 한 문장으로 되짚기

진단서 사진 한 장은 → S3 금고와 Kafka 주문표를 거쳐 → OCR로 **마스킹 텍스트**가 되고 → 리포트 워커의 14노드를 지나며 **약관·판례·장해분류표 근거를 붙인 리포트 초안**으로 자라난 뒤 → 손해사정사의 **서명**으로 확정되고 → 그 순간 개인정보 원본은 지워진다. 지금 실제로 도는 건 리포트 워커·RAG·가드레일(결정론 파트)·core이고, OCR 워커와 챗봇은 아직 스펙 단계다.
