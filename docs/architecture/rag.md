# 공용 Hybrid RAG 검색 파이프라인 (`src/rag` · report_worker·chatbot 공유)

> 출처: AI 엔진 아키텍처 문서 세트 · 최종 점검일 2026-07-15 · 브랜치 `11-feature-langgraph-멀티에이전트-구현`
> 상위: [README](./README.md) · 원본 코드 정독 + 적대적 교차검증(코드 재대조) 완료

## 🎯 한 문장 요약

이 문서는 사용자의 질문을 받아 **오타를 고치고 → 키워드와 의미(벡터) 두 방식으로 동시에 검색한 뒤 → 두 결과를 하나로 합쳐 → 출처(인용)까지 붙여** 돌려주는 공용 검색 함수 `search()`가 실제 코드에서 어떻게 동작하는지 단계별로 설명한다.

## 🌱 쉽게 말하면

도서관 사서 한 명이 있다고 상상해 보자. 손님이 "후유장애 급수 어떻게 되나요?" 하고 물으면, 사서는 먼저 **말이 조금 틀려도 알아듣고 표준 용어로 바꿔서**(예: "후유장애" → "후유장해") 이해한다. 그다음 이 질문을 **두 가지 방식으로 동시에** 찾는다. 하나는 "질문에 나온 단어가 그대로 들어 있는 책"을 찾는 방식(키워드 검색), 다른 하나는 "단어는 달라도 뜻이 비슷한 책"을 찾는 방식(의미 검색)이다.

두 방식이 각자 후보 목록을 뽑아 오면, 사서는 **양쪽에서 모두 위에 올라온 책일수록 높은 점수**를 주는 규칙(RRF)으로 순위를 정리한다. 마지막으로 손님에게 답을 줄 때 "이건 몇 조 몇 항에서 나온 내용입니다" 하고 **출처까지 콕 집어** 알려준다.

여기서 중요한 점은, 이 사서(=`search()` 함수)는 **혼자 일하는 게 아니라 두 부서(리포트 워커·챗봇)가 똑같이 불러 쓰는 공용 직원**이라는 것이다. 그래서 입력과 출력이 명확하고, 데이터베이스 조회와 임베딩 계산 말고는 딴짓(부수효과)을 하지 않도록 깔끔하게 짜여 있다.

---

## 0. 위치와 진입점, 공유 구조

Hybrid RAG(키워드 검색과 의미 검색을 섞어 쓰는 검색 방식) 검색은 `src/rag` 패키지에 구현된 **순수 함수형 공용 모듈**이다.

> 쉽게 말하면: "순수 함수형"이란 같은 질문을 넣으면 언제나 같은 답이 나오고, 몰래 다른 곳의 값을 바꾸거나 하지 않는 깔끔한 함수라는 뜻이다.

`report_worker`(리포트 생성 워커)와 `chatbot` 두 소비자가 동일한 함수 `search()`를 직접 호출해 공유한다. 패키지가 밖으로 열어 두는 창구(공개 표면)는 아주 좁게 통제된다.

```python
# src/rag/__init__.py:7-9
from rag.search import RagError, search

__all__ = ["RagError", "search"]
```

즉 외부에 노출되는 것은 진입점 함수 `search`와 예외 클래스 `RagError` 둘뿐이다(`src/rag/__init__.py:9`). router·typo·fusion·search는 내부 단계로 분리돼 있고, 결과 모델(`Chunk`·`Citation`·`RagResult`)의 단일 출처는 `core.contracts`다(`src/rag/__init__.py:3-4`).

> 쉽게 말하면: 검색 결과를 담는 그릇(데이터 모양)은 여기저기서 제각각 정의하지 않고, `core.contracts` 한 곳에서만 정의해서 모두가 똑같은 그릇을 쓴다는 뜻이다.

모듈 docstring이 그 설계 의도를 못박는다.

```python
# src/rag/search.py:3-5
report_worker·chatbot이 함수 호출로 공유하는 단일 진입점 `search()`를 제공한다. 순수
함수형 인터페이스(in/out 명확, DB·임베딩 외 부수효과 없음)로, 호출자가 조립하기 쉽다.
```

`report_worker`는 `src/rag`를 직접 쓰지 않고 얇은 어댑터(`src/report_worker/rag/hybrid.py`)를 한 겹 두는데, 이는 pydantic `RagResult`(검색 결과를 담는 파이썬 객체)를 리포트 노드용 dict(딕셔너리)로 바꾸는 형변환 계층일 뿐이다(6절 참조).

> 쉽게 말하면: 어댑터는 콘센트 모양을 바꿔 주는 변환 플러그 같은 것이다. 검색 자체는 손대지 않고, 결과 담는 모양만 리포트 부서가 쓰기 편한 형태로 바꿔 준다.

---

## 1. 파이프라인 개요 — 단계별 데이터 흐름

`search()`는 다음 5단계를 조립한다. `src/rag/search.py:6-12`의 docstring이 공식 요약이다.

```python
# src/rag/search.py:6-12
파이프라인(04):
  1. 라우터 — namespace 결정, 비신체보험 범위 외 빈 결과
  2. trigram 오타보정 — search_terms 정규 용어 치환
  3. tsvector ∥ pgvector 병렬 검색 — namespace별 top_k
  4. RRF 통합 — score = Σ weight/(RRF_K + rank), 키워드·벡터 0.5:0.5
  5. 메타데이터 역추적 — 상위 청크에서 Citation 생성
```

먼저 이 단계들에 나오는 낯선 단어를 한 줄씩 풀어 둔다.

- **namespace(네임스페이스)**: 검색할 자료 창고의 이름표. 여기선 약관/사례/장해분류표처럼 어느 서랍을 뒤질지 고르는 구분값이다.
- **trigram(트라이그램)**: 단어를 세 글자씩 잘라 비슷한 정도를 재는 방법. 오타를 잡을 때 쓴다.
- **tsvector**: PostgreSQL이 본문을 단어별로 쪼개 만들어 둔 검색용 색인. 키워드가 들어 있는지 빠르게 찾는다.
- **pgvector**: 문장을 숫자 좌표(벡터)로 바꿔 저장하고, 좌표가 가까운 것끼리 찾아 주는 확장 기능. 뜻이 비슷한 문장을 찾는다.
- **RRF(Reciprocal Rank Fusion)**: 여러 검색 결과의 순위를 합쳐 하나로 만드는 규칙. 여러 목록에서 두루 상위에 오른 것에 높은 점수를 준다.
- **Citation(인용)**: "이 답은 어느 조항/사례에서 나왔다"는 출처 표시.

### 데이터 흐름 다이어그램

```
                          query, insurance_type, namespaces,
                          top_k, insurer, product, contract_date
                                      │
                                      ▼
              ┌──────────────────────────────────────────┐
   (1) 라우터  │ route(query, insurance_type, namespaces)  │  search.py:307
              │  - 비신체보험 힌트 탐지 → in_scope=False   │
              │  - VALID_NAMESPACES 교집합, 순서 보존      │
              └──────────────────────────────────────────┘
                                      │
                     in_scope=False?  ├──► RagResult(ranked_chunks=[], citations=[])  (search.py:308-310)
                                      │    + log_event_out_of_scope
                                      ▼ in_scope=True
              ┌──────────────────────────────────────────┐
 (2) 오타보정 │ correct_query(pool, query)                │  search.py:313
              │  Kiwi 토큰화(내용어) → search_terms         │
              │  trigram similarity>0.4 정규 용어 치환      │
              │  → keyword_query(키워드용) · embed_text(임베딩용) │
              └──────────────────────────────────────────┘
                                      │
              candidate_k = max(top_k*3, 20)   (search.py:316)
                                      │
        ┌─────────────────────────────┴──────────────────────────────┐
        │                                                             │
        ▼ (병렬 시작)                                                  ▼
 embed_task = create_task(_embed_query(embed_text))          _keyword_search × N namespaces
        │  qwen3:embedding → 실패 시 BGE-M3 → 실패 시 None      │  tsvector @@ plainto_tsquery
        │                                                     │  (search.py:320-327, await gather)
        ▼                                                     │
   embedding (1024d | None)                                  │
        │                                                     │
        ▼ embedding is not None?                              │
 _vector_search × N namespaces                                │
   pgvector  embedding <=> $1  (cosine)                       │
   (search.py:331-336)                                        │
        │  embedding is None → vector_results = [[],...]      │
        │  (키워드만으로 degrade, search.py:337-339)            │
        └─────────────────────────────┬──────────────────────┘
                                      ▼
              ┌──────────────────────────────────────────┐
  (3.5) 인덱싱│ row_index: key(ns+chunk_id) → ChunkRow     │  search.py:342-348
              │ keyword_rankings(weight=0.5)               │
              │ vector_rankings(weight=0.5)                │
              └──────────────────────────────────────────┘
                                      ▼
              ┌──────────────────────────────────────────┐
  (4) RRF통합 │ rrf_fuse([*keyword, *vector])             │  search.py:350
              │ score = Σ weight/(60 + rank)              │  fusion.py:37
              │ 점수 내림차순, 동점 id 오름차순             │
              └──────────────────────────────────────────┘
                                      ▼
                          top = fused[:top_k]   (search.py:351)
                          top_rows = [row_index[key] ...]
                                      │
        ┌─────────────────────────────┴──────────────────────────────┐
        ▼                                                             ▼
 chunks: list[Chunk]                                     (5) build_citations(top_rows)
   text, namespace, score,                                  clause_no/exhibit 역추적
   source_ref=chunk_id, article_number,                     중복 제거   (search.py:366)
   product_name, chunk_type  (search.py:354-365)                       │
        └─────────────────────────────┬──────────────────────────────┘
                                      ▼
                    RagResult(ranked_chunks=chunks, citations=citations)   (search.py:369)
                    + log_event_completed(...)
```

> 다이어그램 읽는 법: 위에서 아래로 흐른다. 질문이 들어오면 (1) 라우터가 "이거 검색해도 되나?"를 판단하고, 안 되면 그 자리에서 빈 결과로 끝난다. 통과하면 (2) 오타를 고치고, (3) 키워드 검색과 의미(벡터) 검색을 동시에 돌려서 후보를 모은 뒤, (4) 둘을 합쳐 순위를 매기고, (5) 출처를 붙여 최종 결과를 만든다.

핵심 특성:

- **부수효과 최소화**: DB 조회(asyncpg 풀)·임베딩 호출(ai_client) 외에는 상태를 바꾸지 않는다. 반환값 외 전역 상태 변경 없음(로깅 제외).
  > 쉽게 말하면: 검색 함수는 결과만 돌려줄 뿐, 몰래 다른 데이터를 고치거나 흔적을 남기지 않는다.
- **병렬성**: 임베딩 생성은 `asyncio.create_task`로 키워드 검색과 동시에 시작되고(`search.py:319`), 키워드 검색과 벡터 검색은 각각 namespace별로 `asyncio.gather`로 병렬 실행된다(`search.py:320-327`, `search.py:331-336`).
  > 쉽게 말하면: 시간이 걸리는 작업들을 한 줄로 세워 순서대로 하지 않고, 여러 개를 동시에 돌려서 전체 시간을 줄인다.
- **우아한 성능 저하(degrade)**: 임베딩이 실패하면 벡터 결과를 빈 리스트로 채워 키워드 검색만으로 진행한다(`search.py:337-339`).
  > 쉽게 말하면: 의미 검색이 고장 나도 검색 전체가 멈추지 않는다. 키워드 검색만으로라도 답을 준다.

---

## 2. `router.py` — namespace 조합 결정

라우터는 "이 질문을 어느 서랍(namespace)에서 찾을지"를 정하는 역할을 한다.

### 2.1 상수 정의

라우터는 **룰 기반**(정해진 규칙표대로 처리, 인공지능 판단이 아님)으로 검색할 namespace 조합을 정한다. 현재 지원 대상은 3종이다.

```python
# src/rag/router.py:11-14
# 현재 검색 가능한 namespace. 소스 테이블로 부여되는 파생값과 동일 체계.
VALID_NAMESPACES: frozenset[str] = frozenset({"terms", "case", "level"})
# 힌트가 없을 때 기본 검색 대상. level(장해분류표)은 계약 체결일 버전 매칭이 필요해
# 명시 요청(namespaces=["level"]) 시에만 검색한다.
DEFAULT_NAMESPACES: tuple[str, ...] = ("terms", "case")
```

- 유효 namespace: `terms`(약관 = `policy_chunks`), `case`(분쟁조정사례 = `case_chunks`), `level`(후유장해분류표 = `schedule_chunks`) — `router.py:2-4`.
- **기본값은 `terms`·`case`만**이다. `level`은 계약 체결일 버전 매칭이 필요하므로 `namespaces=["level"]`로 **명시 요청할 때만** 검색된다(`router.py:12-14`). 즉 기본 라우팅으로는 장해분류표가 절대 섞이지 않는다.
  > 쉽게 말하면: 장해분류표는 계약을 맺은 날짜에 따라 적용되는 판(버전)이 달라서, 아무 때나 섞어 찾으면 엉뚱한 판이 나올 수 있다. 그래서 "이 서랍을 열어라"라고 콕 집어 말할 때만 연다.
- `medical`(수가·KCD)은 테이블 미존재로 향후 확장 대상(`router.py:4-5`). (아직 구현 안 됨)

### 2.2 비신체보험 범위 외 처리

이 검색 엔진은 사람의 몸에 생긴 손해(신체보험)를 다룬다. 자동차·화재 같은 비신체보험 질문이 들어오면 "여긴 그거 담당이 아닙니다" 하고 명확히 안내하고 검색하지 않는다.

```python
# src/rag/router.py:17-31
NON_BODILY_HINTS: tuple[str, ...] = (
    "자동차",
    "자차",
    "차량",
    "화재",
    "재물",
    "배상책임",
    "해상",
    "운송",
    "항공",
    "선박",
)

_OUT_OF_SCOPE_REASON = "비신체보험(자동차·화재 등) 관련 쿼리는 현재 검색 범위 밖입니다."
_NO_NAMESPACE_REASON = "검색 가능한 namespace가 없습니다(terms·case·level만 지원)."
```

탐지 방법은 단순하다. **쿼리 본문 또는 `insurance_type` 힌트** 어느 한쪽에라도 위 키워드가 부분 문자열로 들어 있으면 "비신체보험"으로 본다(단순 substring 매칭, 즉 글자가 그대로 들어 있는지만 확인).

```python
# src/rag/router.py:43-48
def _is_non_bodily(query: str, insurance_type: str | None) -> bool:
    """비신체보험 힌트가 보험 유형 또는 쿼리 본문에 있으면 True."""
    haystacks = [query]
    if insurance_type:
        haystacks.append(insurance_type)
    return any(hint in text for text in haystacks for hint in NON_BODILY_HINTS)
```

### 2.3 라우팅 결정 로직

결과는 `RouteDecision`이라는 데이터 묶음(dataclass)으로 돌려준다. `in_scope=False`(검색 범위 밖)면 호출자가 빈 결과를 반환하기로 약속돼 있다(`router.py:36`).

```python
# src/rag/router.py:34-40
@dataclass(slots=True)
class RouteDecision:
    """라우팅 결과. `in_scope=False`면 호출자는 빈 결과를 반환한다."""

    namespaces: list[str]  # 검색 대상 namespace(범위 밖이면 빈 리스트)
    in_scope: bool  # 신체보험 범위 내 여부
    reason: str | None  # 범위 밖/빈 결과 사유(in_scope=True면 None)
```

`route()`의 3단계 판정:

```python
# src/rag/router.py:66-75
    if _is_non_bodily(query, insurance_type):
        return RouteDecision(namespaces=[], in_scope=False, reason=_OUT_OF_SCOPE_REASON)

    candidates = namespaces if namespaces is not None else list(DEFAULT_NAMESPACES)
    # 유효 namespace만 남기되 입력 순서를 보존한다.
    selected = [ns for ns in candidates if ns in VALID_NAMESPACES]
    if not selected:
        return RouteDecision(namespaces=[], in_scope=False, reason=_NO_NAMESPACE_REASON)

    return RouteDecision(namespaces=selected, in_scope=True, reason=None)
```

판정 순서와 규칙:

1. **비신체보험 우선 차단**(`router.py:66-67`): 힌트가 걸리면 즉시 `in_scope=False`, 사유 = `_OUT_OF_SCOPE_REASON`. 이 경우 `namespaces`를 명시했어도 무시된다(비신체 판정이 최우선).
2. **후보 결정**(`router.py:69`): `namespaces`가 `None`이면 기본값 `("terms","case")`, 아니면 명시값을 쓴다.
3. **유효성 필터 + 순서 보존**(`router.py:71`): `VALID_NAMESPACES`에 속한 것만 남기되 **입력 순서를 지킨다**. 예: `["level","terms"]` → `["level","terms"]`. 잘못된 값(`["medical"]`)은 걸러진다.
4. **빈 결과**(`router.py:72-73`): 유효 namespace가 하나도 없으면 `in_scope=False`, 사유 = `_NO_NAMESPACE_REASON`.
5. **정상**(`router.py:75`): 그 외에는 `in_scope=True`, `reason=None`.

> 주의: 라우터는 **쿼리 내용으로 terms/case/level을 켜고 끄는 지능적 분류를 하지 않는다.** 실제 동작은 "비신체면 차단, 아니면 (명시값 ∩ 유효값) 또는 기본 `terms·case`"가 전부다. namespace 선택의 세밀함은 전적으로 호출자가 넘기는 `namespaces` 인자에 달려 있다.
>
> 쉽게 말하면: 라우터는 똑똑한 분류기가 아니라, "자동차 얘기면 막고, 나머지는 시키는 대로 열어 준다" 정도의 문지기다. 어떤 서랍을 열지 정교하게 고르는 건 부르는 쪽 몫이다.

---

## 3. `typo.py` — trigram 오타 보정

### 3.1 목적과 흐름

Kiwi(한국어 형태소 분석기, 문장을 단어와 품사로 쪼개 주는 도구)로 쿼리에서 **내용어**(명사·동사처럼 뜻을 지닌 단어)만 뽑고, 각 내용어를 `search_terms` 테이블의 pg_trgm 유사도로 조회해 가장 비슷한 표준 용어 1건으로 바꾼다. 이렇게 고친 결과가 키워드 검색과 의미(임베딩) 검색에 함께 쓰인다(`typo.py:1-6`).

> 쉽게 말하면: 손님이 "후유장애"라고 잘못 말해도, 우리 사전에 있는 "후유장해"라는 표준 표기로 바꿔 검색해서 헛걸음을 줄인다.

### 3.2 상수와 SQL (verbatim)

```python
# src/rag/typo.py:14-27
# trigram 유사도 임계값. 이 값 초과인 정규 용어만 치환에 사용한다.
SIMILARITY_THRESHOLD = 0.4

# 내용어로 간주할 Kiwi 품사 태그 접두사(명사·동사·형용사·어근·외국어·숫자).
# 조사·어미·기호 등 기능어는 제외해 핵심 용어만 보정·검색 대상으로 남긴다.
_CONTENT_POS_PREFIXES: tuple[str, ...] = ("NN", "NR", "VV", "VA", "XR", "SL", "SN")

# 가장 유사한 정규 용어 1건 조회. % 연산자가 아닌 명시적 similarity로 임계값을 제어한다.
_TERM_LOOKUP_SQL = (
    "SELECT term FROM search_terms "
    "WHERE similarity(term, $1) > $2 "
    "ORDER BY similarity(term, $1) DESC "
    "LIMIT 1"
)
```

- **임계값 = 0.4**(`typo.py:15`). `similarity(term, 입력) > 0.4`인 용어만 후보다. **엄격한 부등호(`>`)** 이므로 정확히 0.4는 제외된다(`typo.py:24`).
  > 쉽게 말하면: 0(전혀 안 닮음)부터 1(완전히 같음) 사이 점수에서, 0.4보다 확실히 더 닮은 표준 용어만 인정한다는 뜻이다.
- `%` 연산자가 아니라 명시적 `similarity()`를 쓰는 이유는 `pg_trgm.similarity_threshold` GUC(PostgreSQL 실행 중 설정값)와 무관하게 임계값을 코드로 직접 제어하기 위함이다(`typo.py:21`).
- 정렬은 `similarity DESC LIMIT 1` — **가장 비슷한 표준 용어 단 1건**만 뽑는다(`typo.py:25-26`).
- 내용어 판정 품사 접두사: `NN`(일반/고유명사), `NR`(수사), `VV`(동사), `VA`(형용사), `XR`(어근), `SL`(외국어), `SN`(숫자). 조사·어미·기호는 제외(`typo.py:18-19`).

### 3.3 내용어 추출

```python
# src/rag/typo.py:57-69
def extract_content_tokens(query: str) -> list[str]:
    """쿼리에서 내용어 표면형 목록을 추출한다(순수·동기, CPU 바운드).
    ...
    """
    kiwi = _get_kiwi()
    return [
        token.form for token in kiwi.tokenize(query) if token.tag.startswith(_CONTENT_POS_PREFIXES)
    ]
```

- `token.form`(화면에 보이는 그대로의 글자 = 표면형)만 취하고, `token.tag`(품사)가 `_CONTENT_POS_PREFIXES`로 **시작**하면 내용어로 채택한다(`str.startswith`가 튜플을 받아 OR 매칭). 예: `NNG`, `NNP`, `VV`, `SL` 등이 모두 통과.
- Kiwi 인스턴스는 지연 생성 싱글턴이다. 처음 필요할 때 딱 한 번만 만들어 두고 재사용한다(초기화 비용 1회).

```python
# src/rag/typo.py:49-54
def _get_kiwi() -> Kiwi:
    """Kiwi 인스턴스를 지연 생성해 재사용한다(초기화 비용 1회)."""
    global _kiwi
    if _kiwi is None:
        _kiwi = Kiwi()
    return _kiwi
```

### 3.4 정규 용어 치환

```python
# src/rag/typo.py:72-75
async def _lookup_canonical(conn: asyncpg.Pool, token: str) -> str | None:
    """search_terms에서 token과 가장 유사한 정규 용어를 1건 조회한다."""
    row = await conn.fetchrow(_TERM_LOOKUP_SQL, token, SIMILARITY_THRESHOLD)
    return None if row is None else row["term"]
```

SQL 파라미터는 `$1=token`, `$2=SIMILARITY_THRESHOLD(0.4)`. 임계값 미만이면 행이 없어 `None`을 돌려준다(고칠 표준 용어를 못 찾음).

핵심 보정 루프:

```python
# src/rag/typo.py:78-108
async def correct_query(pool: asyncpg.Pool, query: str) -> CorrectedQuery:
    ...
    # 토큰화는 CPU 바운드라 스레드로 격리(async 경로 블로킹 금지).
    tokens = await asyncio.to_thread(extract_content_tokens, query)

    corrected_tokens: list[str] = []
    corrections: list[Correction] = []
    embed_text = query
    for token in tokens:
        canonical = await _lookup_canonical(pool, token)
        if canonical is not None and canonical != token:
            corrected_tokens.append(canonical)
            corrections.append(Correction(original=token, canonical=canonical))
            # 임베딩 텍스트도 동일하게 치환(첫 출현만).
            embed_text = embed_text.replace(token, canonical, 1)
        else:
            corrected_tokens.append(token)

    return CorrectedQuery(
        keyword_query=" ".join(corrected_tokens),
        embed_text=embed_text,
        corrections=corrections,
    )
```

동작 상세:

- **토큰화는 `asyncio.to_thread`로 격리**(`typo.py:89`): Kiwi 토큰화는 계산량이 많은 작업(CPU 바운드)이라, 다른 비동기 작업이 멈추지 않도록 별도 스레드로 빼서 돌린다.
- **치환 조건**은 두 가지를 모두 만족해야 한다(`typo.py:96`): `canonical is not None`(임계값 넘는 후보가 있음) **그리고** `canonical != token`(원래 단어와 다름). 같으면 굳이 바꾸지 않고 원래 단어를 그대로 둔다.
- **서로 다른 두 개의 산출물**을 만든다:
  - `keyword_query`: 보정된 내용어들을 공백으로 이어 붙인 문자열. `plainto_tsquery` 입력용(`typo.py:41-42, 105`). **조사·어미가 빠진 내용어만** 남는다.
  - `embed_text`: **원본 쿼리 문장**에서 바뀐 단어만 `str.replace(token, canonical, 1)`로 **처음 나온 것 1번만** 교체한 것. 의미 검색(임베딩)용이라 자연스러운 문맥을 최대한 살린다(`typo.py:93, 100`).
    > 쉽게 말하면: 키워드용은 핵심 단어만 골라 딱딱하게, 의미 검색용은 원래 문장 느낌을 살려 부드럽게 — 같은 질문을 두 가지 버전으로 준비한다.
- `corrections`는 "무엇을 무엇으로 바꿨다"는 기록(관측·디버깅용)일 뿐, 실제 검색 로직에는 쓰이지 않는다(`typo.py:32-37, 98`).

산출 모델:

```python
# src/rag/typo.py:40-46
@dataclass(slots=True)
class CorrectedQuery:
    """오타 보정 결과. 키워드 검색·임베딩 검색이 각각 다른 표현을 쓴다."""

    keyword_query: str  # 공백 구분 보정 내용어(plainto_tsquery 입력)
    embed_text: str  # 보정된 자연어 쿼리(임베딩 입력)
    corrections: list[Correction] = field(default_factory=list)
```

> 예시: 쿼리 "후유장애 급수 어떻게 되나요"에서 내용어 `["후유장애","급수","되"]`가 추출되고, `search_terms`에 "후유장해"가 있으면 `similarity("후유장해","후유장애")>0.4`일 때 `후유장애→후유장해`로 치환된다. `keyword_query="후유장해 급수 되"`, `embed_text="후유장해 급수 어떻게 되나요"`.

---

## 4. `search.py` — 검색 조립 (가장 상세)

이 절이 문서의 핵심이다. 앞의 부품들을 실제로 엮어 검색을 완성하는 곳이다.

### 4.1 (a) namespace → 테이블·컬럼 매핑

세 종류의 서랍(namespace)은 실제 데이터베이스에서 서로 다른 표(테이블)에 담겨 있고, 컬럼 이름도 제각각이다. 그래서 검색할 때 이름표를 통일해 준다.

**테이블 매핑**:

```python
# src/rag/search.py:39-44
# namespace → 테이블.
_NS_TABLE: dict[str, str] = {
    "terms": "policy_chunks",
    "case": "case_chunks",
    "level": "schedule_chunks",
}
```

**컬럼 매핑(`_NS_COLS`)** — 세 테이블 스키마가 달라 SELECT에서 별칭(AS 이름)으로 맞춰 준다.

```python
# src/rag/search.py:46-54
# namespace별 SELECT 소스식: (clause_no, exhibit, article_number, product_name, chunk_type).
# 세 테이블 스키마가 달라 별칭으로 정합한다. 값은 내부 상수(사용자 입력 아님).
# level: 인용의 조항 자리에는 표 내 section(clause_no·article_number), 별표 자리에는 개정판
#   라벨(version_label)을 실어 어느 버전 분류표인지 역추적한다. product_name은 없음(NULL).
_NS_COLS: dict[str, tuple[str, str, str, str, str]] = {
    "terms": ("article_number", "section", "article_number", "product_name", "chunk_type"),
    "case": ("case_no", "NULL::text", "section", "product_name", "chunk_type"),
    "level": ("section", "version_label", "section", "NULL::text", "chunk_type"),
}
```

튜플 위치는 `(clause_no, exhibit, article_number, product_name, chunk_type)` 순서다. namespace별 매핑을 풀어보면:

| namespace | 테이블 | clause_no ← | exhibit ← | article_number ← | product_name ← | chunk_type ← |
|---|---|---|---|---|---|---|
| `terms` | `policy_chunks` | `article_number` | `section` | `article_number` | `product_name` | `chunk_type` |
| `case` | `case_chunks` | `case_no` | `NULL::text` | `section` | `product_name` | `chunk_type` |
| `level` | `schedule_chunks` | `section` | `version_label` | `section` | `NULL::text` | `chunk_type` |

> 표 읽는 법: 왼쪽 "통일된 이름"(clause_no 등)이, 각 테이블에서는 화살표(←) 오른쪽의 실제 컬럼에서 값을 가져온다는 뜻이다. `NULL::text`는 "그 테이블엔 해당 값이 없으니 빈 값을 채운다"는 의미다.

특기사항:

- `case`의 `exhibit`은 실제 컬럼이 아니라 `NULL::text` 리터럴이다(사례엔 별표 개념이 없어서).
- `level`의 `exhibit`에는 **개정판 라벨 `version_label`**을 실어, 인용할 때 "어느 버전 분류표인지" 되짚을 수 있게 한다(`search.py:48-49`). `level`의 `product_name`은 `NULL::text`(상품 개념 없음).
- 이 값들은 **전부 코드 안에 박힌 내부 상수**이지 사용자 입력이 아니므로, f-string으로 조립해도 SQL 인젝션(악의적 문자열로 DB를 조작하는 공격) 위험이 없다(`search.py:47`).

SELECT 컬럼식 조립:

```python
# src/rag/search.py:57-64
def _select_cols(namespace: str) -> str:
    """namespace의 표준 SELECT 컬럼식(별칭 포함)."""
    clause_no, exhibit, article_number, product_name, chunk_type = _NS_COLS[namespace]
    return (
        f"chunk_id, content, {clause_no} AS clause_no, {exhibit} AS exhibit, "
        f"{article_number} AS article_number, {product_name} AS product_name, "
        f"{chunk_type} AS chunk_type"
    )
```

결과적으로 세 테이블 모두 `chunk_id, content, clause_no, exhibit, article_number, product_name, chunk_type` 7개의 **공통 별칭**으로 정규화되어, `_row_to_chunk_row`(`search.py:145-156`)가 어느 테이블에서 왔든 똑같은 `ChunkRow`를 만든다.

> 쉽게 말하면: 서로 다른 서식의 서류 세 종류를 받아, 위쪽에 항목 이름을 똑같이 붙여 한 가지 표준 양식으로 옮겨 적는 것과 같다. 그러면 이후 처리는 출신 테이블을 신경 쓸 필요가 없다.

### 4.2 (b) tsvector 키워드 검색 SQL

```python
# src/rag/search.py:188-214
async def _keyword_search(
    pool: asyncpg.Pool,
    namespace: str,
    query: str,
    top_k: int,
    insurer: str | None = None,
    product: str | None = None,
    contract_date: datetime.date | None = None,
) -> list[ChunkRow]:
    """namespace의 키워드(BM25) 검색 상위 top_k. content_tokens 함수식으로 매칭."""
    if not query.strip():
        return []
    args: list = [query]
    meta = _meta_filter(namespace, args, insurer, product)
    version = _version_filter(namespace, args, contract_date)
    args.append(top_k)
    sql = (
        f"SELECT {_select_cols(namespace)}, "
        f"ts_rank(to_tsvector('simple', coalesce(content_tokens,'')), "
        f"plainto_tsquery('simple', $1)) AS rank "
        f"FROM {_NS_TABLE[namespace]} "
        f"WHERE to_tsvector('simple', coalesce(content_tokens,'')) "
        f"@@ plainto_tsquery('simple', $1){meta}{version} "
        f"ORDER BY rank DESC LIMIT ${len(args)}"
    )
    rows = await pool.fetch(sql, *args)
    return [_row_to_chunk_row(row, namespace) for row in rows]
```

동작 상세:

- **빈 쿼리 가드**(`search.py:198-199`): `query.strip()`이 비면 즉시 빈 리스트. 오타보정 과정에서 내용어가 전부 사라진 경우를 막는다.
- **매칭·랭킹**: `to_tsvector('simple', ...)`로 만든 본문 색인을 `plainto_tsquery('simple', $1)`(질문을 검색어로 바꾼 것)와 `@@`(들어 있는지 비교하는 연산자)로 맞춰 보고, `ts_rank(...)`로 점수를 매긴다. 정렬은 `ORDER BY rank DESC`(점수 높은 순).
- **`'simple'` 컨피그**: 어간 처리·불용어 제거 없이 단순히 토큰만 맞춰 보는 설정. 형태소 분석은 이미 오타보정 단계(Kiwi)에서 끝냈고, `content_tokens`에는 미리 쪼개 둔 텍스트가 들어 있다는 전제다.
- **`coalesce(content_tokens,'')`**: `content_tokens`가 비어(NULL) 있어도 안전하게 빈 색인으로 처리한다.
- **파라미터 순서**: `$1=query`이고, 그 뒤로 `_meta_filter`·`_version_filter`가 필요한 값을 `args`에 덧붙인 다음, 마지막에 `top_k`가 붙어 `LIMIT ${len(args)}`가 된다(몇 번째 파라미터인지 `$N` 번호를 자동 계산). 참고로 docstring은 "BM25"라 적었지만 실제 구현은 PostgreSQL 기본 `ts_rank`다.

### 4.3 (c) pgvector 벡터 검색 SQL

```python
# src/rag/search.py:217-238
async def _vector_search(
    pool: asyncpg.Pool,
    namespace: str,
    embedding: list[float],
    top_k: int,
    insurer: str | None = None,
    product: str | None = None,
    contract_date: datetime.date | None = None,
) -> list[ChunkRow]:
    """namespace의 pgvector 코사인 유사도 검색 상위 top_k. 캐스트 없음(halfvec/vector 코덱)."""
    args: list = [embedding]
    meta = _meta_filter(namespace, args, insurer, product)
    version = _version_filter(namespace, args, contract_date)
    args.append(top_k)
    sql = (
        f"SELECT {_select_cols(namespace)} "
        f"FROM {_NS_TABLE[namespace]} "
        f"WHERE embedding IS NOT NULL{meta}{version} "
        f"ORDER BY embedding <=> $1 LIMIT ${len(args)}"
    )
    rows = await pool.fetch(sql, *args)
    return [_row_to_chunk_row(row, namespace) for row in rows]
```

> 쉽게 말하면: 임베딩(문장을 숫자 좌표로 바꾼 것)끼리 좌표 거리를 재서, 질문 좌표에 가장 가까운 문장들을 뽑는다. 단어가 달라도 뜻이 가까우면 가까운 좌표에 있다.

동작 상세:

- **연산자 `<=>`**: pgvector의 **코사인 거리**(두 좌표가 이루는 각도로 재는 거리). `ORDER BY embedding <=> $1`은 거리 오름차순 = 유사도 내림차순(가까운 것부터).
- **`WHERE embedding IS NOT NULL`**: 임베딩이 없는 행은 제외한다.
- **명시적 캐스트 없음**(`search.py:226`): asyncpg에 halfvec/vector 코덱이 등록돼 있어 파이썬 `list[float]`을 형변환 없이 그대로 바인딩한다.
- 파라미터: `$1=embedding`(1024차원 리스트), 그 뒤 meta·version 파라미터, 마지막 `top_k`.

임베딩 생성 경로(3중 폴백):

```python
# src/rag/search.py:262-278
async def _embed_query(text: str) -> list[float] | None:
    """쿼리 임베딩. qwen3:embedding 실패 시 BGE-M3 폴백, 둘 다 실패면 None(degrade).
    ...
    """
    try:
        return await ai_client.embed(_QUERY_INSTRUCT + text)
    except ai_client.AiClientError as exc:
        logger.warning("rag.embed.primary_failed", exc_info=exc)

    try:
        return await _bge_embed(text)
    # 폴백 경계: 어떤 실패(ImportError·런타임)든 키워드 검색으로 degrade(원칙 8 예외).
    except Exception as exc:
        logger.warning("rag.embed.fallback_failed", exc_info=exc)
        return None
```

> 쉽게 말하면: 문장을 좌표로 바꿔 주는 도구가 세 겹으로 준비돼 있다. 1순위 도구가 고장 나면 2순위로, 그것도 안 되면 아예 좌표 없이(의미 검색을 건너뛰고) 키워드 검색만 한다. 어떤 경우에도 서비스는 멈추지 않는다.

- **1차: `ai_client.embed`** (기본 = qwen3:embedding, 1024차원). 질문에는 안내 문구(instruction 프리픽스)를 붙인다. 저장된 문서는 그대로 넣고 질문에만 이 문구를 붙이는 **비대칭 방식**이다:

  ```python
  # src/rag/search.py:36-37
  _QUERY_INSTRUCT = "Instruct: 보험 약관에서 사용자 질문에 답할 수 있는 관련 조항을 검색한다\nQuery: "
  ```

  `ai_client.embed`는 OpenAI 호환 `/embeddings` 엔드포인트를 호출하고 좌표 차원 수가 맞는지 검증한다:
  ```python
  # src/core/ai_client.py:148-164
  payload: dict[str, Any] = {"model": settings.embedding_model, "input": text}
  ...
  vector: list[float] = data["data"][0]["embedding"]
  ...
  if len(vector) != settings.embedding_dim:
      raise EmbeddingDimensionError(...)
  return vector
  ```
  `embedding_dim` 기본값은 1024(`core/config.py:16,64` — `DEFAULT_EMBEDDING_DIM = 1024`).

- **2차 폴백: BGE-M3**(`search.py:251-259`). 1차에서 `AiClientError`가 나면 `sentence_transformers`의 `BAAI/bge-m3`(1024차원, `search.py:34`)로 **컴퓨터 내부에서 직접**(로컬) 좌표를 만든다. 차원이 안 맞으면 `RagError`를 던진다. **주의**: 1차 경로에는 안내 문구(프리픽스)를 붙이지만, BGE-M3 경로에는 `_QUERY_INSTRUCT` 없이 원문 `text`만 넣는다(`search.py:274`).

  ```python
  # src/rag/search.py:251-259
  async def _bge_embed(text: str) -> list[float]:
      """BGE-M3로 쿼리를 임베딩한다(폴백 경로). 차원 검증 포함."""
      model = await asyncio.to_thread(_load_bge_model)
      vector: list[float] = await asyncio.to_thread(
          lambda: model.encode(text, normalize_embeddings=True).tolist()  # type: ignore[attr-defined]
      )
      if len(vector) != settings.embedding_dim:
          raise RagError(f"BGE-M3 임베딩 차원 불일치: {len(vector)} != {settings.embedding_dim}")
      return vector
  ```
  BGE 모델은 지연 로드 싱글턴이며(처음 필요할 때 한 번만 불러옴), 시간이 걸리는 로딩을 `to_thread`로 감싼다(`search.py:241-248, 253`).

- **3차: `None`(degrade)**. BGE도 실패(ImportError 포함)하면 `None`을 반환해 **벡터 검색을 통째로 건너뛰고** 키워드 검색만으로 진행한다. 주석이 "원칙 8 예외"(넓게 예외를 잡는 것을 의도적으로 허용)임을 명시(`search.py:275`).

### 4.4 (d) 메타 필터 (insurer / product)

```python
# src/rag/search.py:67-87
# insurer/product 필터가 유효한 namespace. case_chunks의 insurer/product_name은 nullable
# 참고 메타일 뿐 필터 키가 아니라(적재기가 채우지 않음), 걸면 case 결과가 항상 0건이 된다.
_META_FILTER_NS: frozenset[str] = frozenset({"terms"})


def _meta_filter(namespace: str, args: list, insurer: str | None, product: str | None) -> str:
    """insurer/product 메타 필터 절을 만들고 args에 파라미터를 덧붙인다(다음 $N).

    필터는 policy_chunks(terms)에만 적용한다 — case_chunks의 insurer/product_name은
    nullable 참고 메타일 뿐 필터 키가 아니므로 걸면 case recall이 0으로 붕괴한다.
    """
    if namespace not in _META_FILTER_NS:
        return ""
    clause = ""
    if insurer:
        args.append(insurer)
        clause += f" AND insurer = ${len(args)}"
    if product:
        args.append(product)
        clause += f" AND product_name = ${len(args)}"
    return clause
```

- 보험사(insurer)·상품명(product) 필터는 **`terms`(policy_chunks)에만** 적용된다(`_META_FILTER_NS = {"terms"}`). `case`·`level`에는 빈 문자열을 돌려준다(필터 없음).
- 이유(주석 명시): `case_chunks`의 `insurer`/`product_name`은 적재기가 채우지 않는 nullable 참고 메타라, 필터로 걸면 `case` 결과가 **항상 0건**이 되어 recall(찾아 오는 비율)이 무너진다(`search.py:67-69, 75-77`).
  > 쉽게 말하면: 사례 서랍에는 보험사·상품명 칸이 대부분 비어 있어서, 그걸로 거르면 아무것도 안 걸린다. 그래서 약관 서랍에만 이 필터를 건다.
- 파라미터는 `args`에 덧붙이며 `$N` 번호를 `len(args)`로 자동 계산한다 → **파라미터 바인딩**이라 인젝션 위험 없음.
- `insurer`, `product`는 각각 값이 있을 때만 조건이 추가된다(둘 다 선택 사항).

### 4.5 (e) `_version_filter` — 반열림 구간 [applies_from, applies_to)

이 절이 `level`(후유장해분류표) 버전 매칭의 핵심이다. verbatim:

```python
# src/rag/search.py:90-118
# 버전(개정판) 필터가 유효한 namespace. 후유장해분류표(schedule_chunks)는 시행세칙 개정마다
# 판이 갈려, 계약 체결일이 속하는 버전만 검색해야 한다.
_VERSION_FILTER_NS: frozenset[str] = frozenset({"level"})


def _version_filter(namespace: str, args: list, contract_date: datetime.date | None) -> str:
    """level namespace의 개정 버전(적용기간) 필터 절을 만들고 args에 파라미터를 덧붙인다.

    후유장해분류표는 개정 버전별로 적용되므로, 계약 체결일이 속하는 판만 남긴다.
    `contract_date=None`이면 현행판(`applies_to IS NULL`)만 검색한다. level 외 namespace에는
    적용하지 않는다(terms 버전매칭은 별도 작업).

    Args:
        namespace: 검색 namespace.
        args: SQL 파라미터 리스트(제자리 수정 — contract_date를 덧붙일 수 있다).
        contract_date: 계약 체결일. None이면 현행판만.

    Returns:
        WHERE 뒤에 이어붙일 필터 절(해당 없으면 빈 문자열).
    """
    if namespace not in _VERSION_FILTER_NS:
        return ""
    if contract_date is None:
        # 현행판만: 종료일이 없는(적용 중인) 버전.
        return " AND applies_to IS NULL"
    # 파라미터 1개를 바인딩해 시작·종료 경계 두 곳에서 재사용($N)한다(SQL 인젝션 없음).
    args.append(contract_date)
    idx = len(args)
    return f" AND applies_from <= ${idx} AND (applies_to IS NULL OR ${idx} < applies_to)"
```

> 쉽게 말하면: 후유장해분류표는 법 개정 때마다 새 판으로 바뀐다. 계약을 2020년에 맺었으면 그때 유효하던 판으로 봐야 하므로, "계약일이 어느 판의 적용 기간 안에 드는가"를 따져 딱 그 판만 검색한다.

로직:

- **`level`에만** 적용(`_VERSION_FILTER_NS = {"level"}`). `terms`·`case`엔 빈 문자열 → 버전 필터 없음(`terms` 버전 매칭은 향후 별도 작업으로 명시).
- **`contract_date=None`** → `AND applies_to IS NULL`. 종료일이 없는 = 지금 적용 중인 최신판만.
- **`contract_date` 지정** → **반열림 구간** `applies_from <= date AND (applies_to IS NULL OR date < applies_to)`.
  - 하한은 **포함**(`applies_from <= date`), 상한은 **배제**(`date < applies_to`). 즉 `[applies_from, applies_to)`.
    > 표기 읽는 법: `[`는 "그 날짜 포함", `)`는 "그 날짜는 안 포함"을 뜻한다. 시작일 당일은 포함, 종료일 당일은 다음 판으로 넘어간다는 의미다.
  - `applies_to IS NULL`(현행판)도 상한 조건을 통과시켜, 계약일이 최신판 시행 이후면 현행판이 매칭된다.
  - **경계 안전성**: 개정 경계 날짜에서 이전 판(`applies_to = 경계일`)과 새 판(`applies_from = 경계일`)이 겹치지 않는다(이전 판은 `date < applies_to` 위반, 새 판은 `applies_from <= date` 만족). 즉 경계일 하루에 두 판이 동시에 걸리는 일이 없다.
- **파라미터 재사용**: `contract_date` 하나만 덧붙이고, 같은 `$idx`를 시작·종료 두 조건에 재사용한다(`search.py:116-118`). 바인딩 파라미터라 인젝션 없음.

### 4.6 (f) top_k와 후보 풀 확장

```python
# src/rag/search.py:315-316
# RRF 통합 전 후보 풀은 넉넉히 긁는다(top_k만 긁으면 메타필터 좁은 집합에서 recall 손실).
candidate_k = max(top_k * 3, 20)
```

각 namespace의 키워드/벡터 검색은 `top_k`가 아니라 `candidate_k = max(top_k*3, 20)`로 넉넉하게 후보를 긁어 온 뒤(`search.py:316, 323, 333`), RRF 통합이 끝난 다음 최종적으로 `fused[:top_k]`로 자른다(`search.py:351`). 좁은 필터 조건 때문에 후보가 부족해 좋은 결과를 놓치는 걸 막기 위함이다. 기본 `top_k`는 8이다(`search.py:31` — `DEFAULT_TOP_K = 8`).

> 쉽게 말하면: 최종 8개만 필요해도 처음엔 넉넉히(최소 20개, 또는 8×3=24개) 뽑아 놓는다. 넓게 그물을 던져야 좋은 후보를 놓치지 않고, 합쳐서 순위를 다시 매긴 뒤 상위 8개만 남긴다.

### 4.7 인용(citation) 역추적

```python
# src/rag/search.py:164-185
def build_citations(rows: list[ChunkRow]) -> list[Citation]:
    """상위 청크 행에서 인용 근거를 역추적한다(순수 함수).

    조항/사례 번호·별표가 모두 없는 행은 건너뛰고, 동일 인용은 한 번만 담는다.
    ...
    """
    seen: set[tuple[str | None, str | None]] = set()
    citations: list[Citation] = []
    for row in rows:
        if row.clause_no is None and row.exhibit is None:
            continue
        key = (row.clause_no, row.exhibit)
        if key in seen:
            continue
        seen.add(key)
        citations.append(Citation(clause_no=row.clause_no, exhibit=row.exhibit))
    return citations
```

- 입력은 **최종 top_k의 상위 청크 행**(`top_rows`, `search.py:353,366`)이며 순서를 그대로 지킨다.
- `clause_no`·`exhibit`이 **둘 다 None**이면 건너뛴다(출처를 표시할 수 없어서).
- `(clause_no, exhibit)` 짝으로 **중복 제거**(같은 조항/별표는 한 번만).
- 순수 함수(부수효과 없음)라 테스트하기 쉽다.

> 쉽게 말하면: 최종 결과 각각에 "이건 몇 조에서 나왔다"는 꼬리표를 붙이되, 같은 꼬리표가 여러 번 나오면 한 번만 적는다.

### 4.8 조립 본체 — 상태 전이

`search()`의 본체(`search.py:281-369`)가 위 조각들을 엮는다. 상태 전이 순서:

1. **라우팅**(`search.py:307-310`): `route()` 호출. `in_scope=False`면 `log_event_out_of_scope`를 남기고 즉시 `RagResult(ranked_chunks=[], citations=[])`.
2. **풀 획득 + 오타보정**(`search.py:312-313`): `get_pool()`로 asyncpg 풀(DB 연결 묶음)을 얻고, `correct_query`로 `corrected`를 만든다.
3. **병렬 시작**(`search.py:319-320`): `embed_task = create_task(_embed_query(corrected.embed_text))`를 먼저 걸어 두고, 키워드 검색을 `gather`로 돌린다.
   ```python
   # src/rag/search.py:319-328
   embed_task = asyncio.create_task(_embed_query(corrected.embed_text))
   keyword_results = await asyncio.gather(
       *(
           _keyword_search(
               pool, ns, corrected.keyword_query, candidate_k, insurer, product, contract_date
           )
           for ns in decision.namespaces
       )
   )
   embedding = await embed_task
   ```
   키워드 검색은 `corrected.keyword_query`를, 임베딩은 `corrected.embed_text`를 쓴다(3.4에서 만든 서로 다른 두 표현).
4. **벡터 검색 또는 degrade**(`search.py:330-339`): `embedding is not None`이면 namespace별 `_vector_search`를 gather, 아니면 `vector_results = [[] for _ in decision.namespaces]`(빈 결과로 채우고 키워드만).
5. **행 인덱싱·랭킹 구성**(`search.py:341-348`): `row_index[key]=ChunkRow`를 채우면서 각 namespace 결과를 `(키리스트, weight)` 형태로 만든다. 키워드는 `KEYWORD_WEIGHT`, 벡터는 `VECTOR_WEIGHT`.
   ```python
   # src/rag/search.py:342-348
   row_index: dict[str, ChunkRow] = {}
   keyword_rankings: list[tuple[list[str], float]] = []
   vector_rankings: list[tuple[list[str], float]] = []
   for rows in keyword_results:
       keyword_rankings.append(([_index_row(row_index, row) for row in rows], KEYWORD_WEIGHT))
   for rows in vector_results:
       vector_rankings.append(([_index_row(row_index, row) for row in rows], VECTOR_WEIGHT))
   ```
   키는 `_key(namespace, chunk_id)` = `f"{namespace}\x00{chunk_id}"`. namespace마다 chunk_id가 겹쳐도 NUL 구분자(눈에 안 보이는 특수 문자)로 충돌을 막는다(`search.py:121-122, 159-161`). `_index_row`는 `setdefault`로 **같은 키가 또 오면 먼저 넣은 행을 유지**한다(`search.py:372-376`).
6. **RRF 통합·절단**(`search.py:350-351`): `fused = rrf_fuse([*keyword_rankings, *vector_rankings])`, `top = fused[:top_k]`.
7. **Chunk 구성**(`search.py:353-365`): `top`의 각 `(key, score)`에 대해 `row_index[key]`로 원래 행을 찾아 `Chunk`를 만든다. `score`는 RRF 점수, `source_ref`는 `chunk_id`.
   ```python
   # src/rag/search.py:354-365
   chunks = [
       Chunk(
           text=row.content,
           namespace=row.namespace,
           score=score,
           source_ref=row.chunk_id,
           article_number=row.article_number,
           product_name=row.product_name,
           chunk_type=row.chunk_type,
       )
       for (_, score), row in zip(top, top_rows, strict=True)
   ]
   ```
   `zip(..., strict=True)`로 두 리스트 길이가 어긋나면 곧바로 오류를 내 방어한다.
8. **인용 생성·로깅·반환**(`search.py:366-369`): `build_citations(top_rows)`로 출처를 만들고, `log_event_completed(...)`로 기록한 뒤, `RagResult(ranked_chunks=chunks, citations=citations)`를 돌려준다.

**로깅의 PII 방어**: 두 로그 이벤트 모두 쿼리 본문을 남기지 않는다(`search.py:379-393`). `out_of_scope`는 사유만, `completed`는 namespaces·top_k·n_chunks·degraded 플래그만 기록한다.

> 쉽게 말하면: PII(개인을 식별할 수 있는 정보)가 새지 않도록, 로그에는 사용자가 뭘 물었는지(질문 원문)를 아예 남기지 않고 "몇 개 찾았다" 같은 통계만 적는다.

```python
# src/rag/search.py:384-393
def log_event_completed(namespaces: list[str], top_k: int, n_chunks: int, degraded: bool) -> None:
    """검색 완료 이벤트 로깅(쿼리 본문 제외)."""
    logger.info(
        "rag.search.completed",
        event_type="rag.search.completed",
        namespaces=namespaces,
        top_k=top_k,
        n_chunks=n_chunks,
        degraded=degraded,
    )
```

---

## 5. `fusion.py` — RRF(Reciprocal Rank Fusion)

RRF는 여러 검색 결과의 순위를 하나로 합쳐 최종 순위를 만드는 규칙이다.

> 쉽게 말하면: 두 심사위원(키워드 검색·의미 검색)이 각각 매긴 순위를 합산해 종합 순위를 내는 것과 같다. 두 심사위원 모두에게 상위로 뽑힌 후보가 종합 1등이 된다.

### 5.1 상수 (verbatim)

```python
# src/rag/fusion.py:9-15
# RRF 상수. score = Σ weight_i / (RRF_K + rank_i). 60은 RRF 원논문 권장값으로,
# 상위권 순위 차이는 살리되 하위 노이즈의 영향을 완만하게 누른다.
RRF_K = 60

# tsvector·벡터 기본 가중치(0.5:0.5). namespace별로 호출자가 조정할 수 있다.
KEYWORD_WEIGHT = 0.5
VECTOR_WEIGHT = 0.5
```

- **`RRF_K = 60`**: RRF 원논문 권장값. 완충 상수로, 상위권 순위 차이는 살리되 하위 노이즈 영향을 완만하게 누른다.
  > 쉽게 말하면: 60은 순위 점수를 부드럽게 눌러 주는 완충 값이다. 1등과 2등의 차이는 살아 있게 하면서, 저 아래 순위들끼리는 점수 차이를 거의 안 나게 해서 잡음의 영향을 줄인다.
- **키워드:벡터 = 0.5:0.5** 기본 가중치. `search.py`가 이 상수를 그대로 각 랭킹의 weight로 넘긴다(`search.py:346-348`). 즉 두 심사위원의 표를 반반으로 본다.

### 5.2 RRF 공식 (verbatim)

```python
# src/rag/fusion.py:18-39
def rrf_fuse(
    rankings: Sequence[tuple[Sequence[str], float]],
    *,
    k: int = RRF_K,
) -> list[tuple[str, float]]:
    """여러 순위 리스트를 RRF로 통합해 점수 내림차순 결과를 반환한다.
    ...
    """
    scores: dict[str, float] = {}
    for ids, weight in rankings:
        for position, doc_id in enumerate(ids):
            rank = position + 1  # 1-based
            scores[doc_id] = scores.get(doc_id, 0.0) + weight / (k + rank)
    # 점수 내림차순, 동점은 id 오름차순(결정적 출력).
    return sorted(scores.items(), key=lambda item: (-item[1], item[0]))
```

동작:

- **입력**: `(정렬된_id_리스트, 가중치)` 튜플들의 시퀀스. 각 리스트는 **점수 높은 순으로 정렬돼 있다**고 전제한다(rank = 0-based 위치 + 1).
- **점수 누적**: 같은 id가 여러 랭킹에 나타나면 `weight/(k+rank)`를 **합산**한다. 즉 키워드·벡터 양쪽에서 상위에 오른 문서가 가장 높은 점수를 받는다.
- **공식**: `score(doc) = Σ_i  weight_i / (RRF_K + rank_i)` = `Σ weight_i / (60 + rank_i)`.
- **정렬**: `key=lambda item: (-item[1], item[0])` — **점수 내림차순, 동점이면 id 오름차순**. 언제 돌려도 같은 순서가 나오도록(결정적) 보장한다.
- **부수효과 없음**: 순수 함수라 단위 테스트가 쉽다(`fusion.py:3-4`).

`search.py`에서의 조합: 각 namespace의 키워드 결과와 벡터 결과가 **모두 별개의 랭킹 리스트**로 들어온다. 예를 들어 namespace가 `terms`·`case` 2개면 총 4개 랭킹(키워드 2 + 벡터 2)이 `rrf_fuse`에 넘어가고(`search.py:350`), 같은 `(namespace,chunk_id)` 키가 여러 랭킹에 등장하면 점수가 합산되어 하나의 통합 순위가 된다.

> 예: `terms` 키워드에서 1위, `terms` 벡터에서 2위인 청크의 점수 = `0.5/(60+1) + 0.5/(60+2)` = `0.008197 + 0.008065` = `0.016262`.

---

## 6. `hybrid.py` 어댑터 (report_worker)

이 파일은 **얇은 형변환 계층**이다. 과거엔 자체 하이브리드 검색 구현이었으나 `src/rag`로 단일화했고, 지금은 pydantic `RagResult`를 리포트 노드용 dict로만 바꾼다(`hybrid.py:1-6`). 검색 로직·필터·임베딩은 전부 `src/rag`가 담당한다.

> 쉽게 말하면: 예전엔 리포트 부서가 자기만의 검색기를 따로 갖고 있었지만, 지금은 공용 검색기 하나로 합쳤다. 이 파일은 공용 검색 결과를 리포트 부서가 쓰기 편한 모양으로 갈아 끼워 주는 어댑터일 뿐이다.

```python
# src/report_worker/rag/hybrid.py:14-15
from core.contracts import Chunk
from rag import search as _rag_search
```

### 6.1 인용 포맷터

```python
# src/report_worker/rag/hybrid.py:18-21
def _fmt_citation(c: Chunk) -> str:
    """`[조항/사례번호, 상품명]` 형식의 사람이 읽는 인용. 메타 없으면 chunk_id로 폴백."""
    inside = c.article_number or c.source_ref
    return f"[{inside}, {c.product_name or ''}]"
```

- 형식: `[조항/사례번호, 상품명]`. 앞부분은 `article_number`, 없으면 `source_ref`(chunk_id)로 대체. 뒷부분은 `product_name`, 없으면 빈 문자열.

### 6.2 어댑터 본체

```python
# src/report_worker/rag/hybrid.py:24-63
async def search(
    query: str,
    insurance_type: str | None = None,
    namespaces: list[str] | None = None,
    top_k: int = 8,
    *,
    insurer: str | None = None,
    product: str | None = None,
    contract_date: datetime.date | None = None,
) -> dict[str, Any]:
    """canonical `rag.search` 호출 후 리포트 노드용 dict로 매핑.
    ...
    """
    res = await _rag_search(
        query,
        insurance_type,
        namespaces or ["terms"],
        top_k,
        insurer=insurer,
        product=product,
        contract_date=contract_date,
    )
    chunks = [
        {
            "text": c.text,
            "namespace": c.namespace,
            "score": c.score,
            "source_ref": _fmt_citation(c),
            "article_number": c.article_number,
            "product_name": c.product_name,
            "chunk_type": c.chunk_type,
        }
        for c in res.ranked_chunks
    ]
    return {"ranked_chunks": chunks, "citations": [c["source_ref"] for c in chunks]}
```

핵심 차이점(공용 함수와의 계약 차이):

- **namespace 기본값이 다르다**: 공용 `search`는 `None`을 넘기면 라우터가 `["terms","case"]`를 쓰지만, 어댑터는 `namespaces or ["terms"]`로 **`["terms"]`만** 기본값으로 넘긴다(`hybrid.py:45`). 즉 리포트 노드는 기본적으로 약관만 검색한다.
- **`source_ref` 의미가 뒤바뀐다**: 공용 `Chunk.source_ref`는 원문 위치 `chunk_id`이지만(`search.py:359`), 어댑터 dict의 `"source_ref"`는 `_fmt_citation(c)`로 만든 **사람이 읽는 인용 문자열**이다(`hybrid.py:57`). 원래 chunk_id는 `_fmt_citation` 안에서 대체값으로만 남는다.
- **citations 필드도 다시 정의된다**: 공용 `RagResult.citations`는 `Citation` 객체 리스트지만, 어댑터의 `"citations"`는 각 청크의 포맷된 `source_ref` 문자열 리스트다(`hybrid.py:63`). 즉 어댑터는 공용의 `res.citations`를 **버리고** 청크에서 새로 만든다.
- `contract_date`는 그대로 공용 함수로 넘어가(패스스루) `namespaces=["level"]` 검색 시 버전 매칭에 쓰인다(`hybrid.py:39-41, 49`).

> 주의: 어댑터를 거치면 `source_ref`와 `citations`의 뜻이 공용 함수와 달라진다. 리포트 노드가 받는 것은 `Citation` 객체가 아니라 사람이 읽는 문자열 인용이다. 이 차이를 모르고 공용 계약을 그대로 기대하면 어긋난다.

### 6.3 레거시 별칭과 no-op

```python
# src/report_worker/rag/hybrid.py:66-72
# 하위호환 별칭
hybrid_search = search


async def close_pool() -> None:
    """진입점/scripts의 `close_rag_pool` 호환. 풀은 core.db가 소유하므로 no-op."""
    return None
```

- `hybrid_search`는 `search`의 다른 이름(옛 임포트 호환용).
- `close_pool`은 **아무 일도 하지 않는(no-op)** 함수다 — DB 연결 풀의 소유권이 `core.db`에 있어 어댑터가 닫을 게 없다. 진입점 호환을 위한 빈 코루틴이다.
- 모듈명 `hybrid`도 옛 이름(레거시)이며 노드 임포트 호환을 위해 그대로 둔다(`hybrid.py:6`).

> 프롬프트가 언급한 `_select_schedule` 함수는 이 파일에 **존재하지 않는다**. `hybrid.py` 어댑터가 노출하는 것은 `_fmt_citation`, `search`(=`hybrid_search`), `close_pool` 뿐이다. schedule 선별 관련 로직(`chunk_type == "schedule"` 등)은 이 어댑터가 아니라 소비 측 리포트 노드에 있을 것으로 보이며, 본 조사 범위 파일에는 구현이 없다(과장 없이 명시).

---

## 7. 입출력 계약

### 7.1 `search()` 입력 인자

공용 진입점 시그니처(`search.py:281-289`):

```python
async def search(
    query: str,
    insurance_type: str | None = None,
    namespaces: list[str] | None = None,
    top_k: int = DEFAULT_TOP_K,   # = 8
    *,
    insurer: str | None = None,
    product: str | None = None,
    contract_date: datetime.date | None = None,
) -> RagResult:
```

| 인자 | 타입 | 기본값 | 의미 | 근거 |
|---|---|---|---|---|
| `query` | `str` | (필수) | 사용자 쿼리 텍스트 | `search.py:282,294` |
| `insurance_type` | `str \| None` | `None` | 신체보험 유형 힌트. 비신체면 범위 외 빈 결과 | `search.py:283,295` |
| `namespaces` | `list[str] \| None` | `None` | 검색 namespace. `None`이면 라우터가 `terms·case` | `search.py:284,296` |
| `top_k` | `int` | `8`(`DEFAULT_TOP_K`) | 반환 청크 최대 개수 | `search.py:31,285,297` |
| `insurer` | `str \| None` (키워드 전용) | `None` | 보험사 메타 필터. **terms에만 적용** | `search.py:287,298` |
| `product` | `str \| None` (키워드 전용) | `None` | 상품명 메타 필터. **terms에만 적용** | `search.py:288,299` |
| `contract_date` | `datetime.date \| None` (키워드 전용) | `None` | 계약 체결일. **level 버전 매칭** `applies_from <= date < applies_to`. None이면 현행판 | `search.py:289,300-302` |

> 표 읽는 법: "키워드 전용"이라고 적힌 인자는 반드시 이름을 붙여(`insurer=...`) 넘겨야 한다는 뜻이다. 코드의 `*` 뒤에 있는 인자들이 그렇다.

`insurer`·`product`·`contract_date`는 `*` 뒤의 **키워드 전용 인자**다(`search.py:286`). `contract_date`는 `terms`·`case`에는 영향이 없다(`search.py:302`).

### 7.2 반환 계약 — `RagResult`

```python
# src/core/contracts.py:119-123
class RagResult(BaseModel):
    """`rag.search`의 반환 계약. 비신체보험 쿼리는 빈 결과로 반환된다."""

    ranked_chunks: list[Chunk]
    citations: list[Citation]
```

- `ranked_chunks`: RRF 통합 순위 상위 `top_k` 청크.
- `citations`: 상위 청크에서 되짚어 만든 중복 제거 인용 근거.
- **범위 밖/빈 결과**: 비신체보험이거나 유효 namespace가 없으면 `RagResult(ranked_chunks=[], citations=[])`(`search.py:310`).

### 7.3 `Chunk` 필드

```python
# src/core/contracts.py:97-109
class Chunk(BaseModel):
    """Hybrid RAG 검색 결과 청크. 임베딩 차원은 1024 고정(config.EMBEDDING_DIM)."""

    text: str  # 청크 원문 (POLICY_CHUNKS.content / CASE_CHUNKS.content)
    # 검색한 소스 테이블로 부여되는 파생값. 현재 유효값: "terms"(POLICY_CHUNKS) ·
    # "case"(CASE_CHUNKS). "level"(장해분류)·"medical"(수가·KCD)은 테이블 미존재 → 향후 확장.
    namespace: str
    score: float  # RRF 통합 점수
    source_ref: str  # 원문 위치 참조 (chunk_id)
    # 소비자(리포트 노드)가 인용 포맷·schedule 선별에 쓰는 파생 메타(없으면 None).
    article_number: str | None = None  # 조항/사례 번호 (terms=article_number, case=section)
    product_name: str | None = None
    chunk_type: str | None = None  # 예: schedule(장해분류표)·clause·termination
```

| 필드 | 타입 | 소스 | 근거 |
|---|---|---|---|
| `text` | `str` | `row.content` (해당 테이블 content) | `search.py:355`, `contracts.py:100` |
| `namespace` | `str` | `terms`/`case`/`level` | `search.py:356`, `contracts.py:103` |
| `score` | `float` | RRF 통합 점수 | `search.py:357`, `contracts.py:104` |
| `source_ref` | `str` | `row.chunk_id`(원문 위치) | `search.py:359`, `contracts.py:105` |
| `article_number` | `str \| None` | terms=article_number, case=section, level=section | `search.py:361`, `_NS_COLS` |
| `product_name` | `str \| None` | terms/case=product_name, level=NULL | `search.py:362` |
| `chunk_type` | `str \| None` | schedule·clause·termination 등 | `search.py:363`, `contracts.py:109` |

> 주의: `contracts.py:101-102`의 주석은 `level`·`medical`을 "테이블 미존재 → 향후 확장"이라 적었지만, 이는 계약 문서가 최신화되기 전의 서술로 보인다. 실제 `search.py`의 `_NS_TABLE`/`_NS_COLS`(`search.py:40-54`)와 라우터의 `VALID_NAMESPACES`(`router.py:11`)는 `level`(schedule_chunks)을 **이미 지원**한다. `medical`만 여전히 미구현이다(정직하게 표기: contracts 주석과 실제 코드 사이에 문서 불일치 존재).

### 7.4 `Citation` 필드

```python
# src/core/contracts.py:112-116
class Citation(BaseModel):
    """인용 근거(메타데이터 역추적). 출처 URL 컬럼은 보유 테이블에 없어 계약에서 제외."""

    clause_no: str | None = None  # 조항/사례 번호 (article_number / case_number)
    exhibit: str | None = None  # 별표·항목 (POLICY_CHUNKS.section, 있으면)
```

| 필드 | 타입 | 소스(`_NS_COLS` 기준) | 근거 |
|---|---|---|---|
| `clause_no` | `str \| None` | terms=article_number, case=case_no, level=section | `search.py:184`, `contracts.py:115` |
| `exhibit` | `str \| None` | terms=section, case=NULL, level=version_label | `search.py:184`, `contracts.py:116` |

두 필드가 모두 `None`인 행은 인용에서 제외되고, `(clause_no, exhibit)`로 중복 제거된다(`search.py:178-184`). 출처 URL 컬럼은 보유 테이블에 없어 계약에서 아예 제외됐다(`contracts.py:113`).

---

## 8. 미구현·불일치·주의사항 정리 (정직 표기)

> 이 절은 "아직 안 된 것, 코드와 문서가 어긋나는 것, 헷갈리기 쉬운 것"을 숨기지 않고 정직하게 모아 둔 것이다.

1. **`medical` namespace 미구현**: 라우터·search 어디에도 테이블 매핑이 없다. `router.py:4-5`가 "테이블 미존재로 향후 확장"이라 명시.
2. **contracts.py 주석 노후**: `Chunk.namespace` 주석(`contracts.py:101-102`)이 `level`을 미구현으로 적었으나 실제 코드는 지원한다. 계약 문서와 구현 사이의 문서 드리프트(문서가 코드를 못 따라간 상태).
3. **`_select_schedule` 부재**: 프롬프트가 언급한 함수는 `hybrid.py`에 없다. schedule 선별 로직은 이 파일 범위 밖(소비 노드에 있을 것으로 추정, 본 조사 파일엔 없음).
4. **라우터의 namespace 지능 부재**: 라우터는 쿼리 내용으로 terms/case/level을 지능적으로 켜지 않는다. "비신체 차단 + (명시값 ∩ 유효값) or 기본값"이 전부다. 세밀한 namespace 선택은 호출자 책임.
5. **BGE 폴백의 프리픽스 비대칭**: 1차 경로는 `_QUERY_INSTRUCT` 프리픽스를 붙이지만(`search.py:269`), BGE 폴백은 원문만 임베딩(`search.py:274`)한다. 두 임베딩 모델의 쿼리 표현이 달라 폴백 시 검색 품질 특성이 미묘하게 바뀔 수 있다(버그는 아니나 알아 둘 필요 있음).
6. **BM25 표기**: `_keyword_search` docstring이 "BM25"라 하지만 실제는 PostgreSQL `ts_rank`다(`search.py:197, 206`). 알고리즘 명칭이 부정확.
7. **어댑터 계약 재정의**: `hybrid.py`의 `source_ref`/`citations`는 공용 `RagResult`와 의미가 다르다(7.2/6.2 참조). 리포트 노드는 공용의 `Citation` 객체를 받지 않고, 청크에서 새로 만든 문자열 인용을 받는다. 소비 측 코드가 공용 계약을 그대로 기대하면 안 된다.

### 8-보강. 교차검증에서 추가된 정밀 주석

아래는 코드 대조 검증 단계에서 보강된 4가지 미세 정정으로, 위 서술의 정확도를 높인다.

8. **`RagError`는 실무상 호출자에게 전파되지 않는다**: `RagError`는 공개 API(`__all__`)이고 파이프라인에서 실제로 `raise`되는 유일한 지점은 `_bge_embed`의 차원 불일치(`search.py:258`)뿐이다. 그런데 이 호출은 `_embed_query`의 넓은 `except Exception`(`search.py:273-278`)에 **즉시 잡혀 `None`(degrade)으로 흡수**된다. 따라서 §4.3의 "BGE 차원 불일치면 `RagError`를 던진다"는 함수 내부 사실일 뿐, `search()`가 `RagError`를 밖으로 전파하는 경로는 실질적으로 없다(키워드 검색만으로 degrade).
   > 쉽게 말하면: `RagError`는 이론상 밖으로 나올 수 있는 예외지만, 실제로는 바로 위에서 붙잡혀 "키워드만으로 계속 진행"으로 바뀌기 때문에 호출자에게까지 튀어 나오지 않는다.
9. **`EmbeddingDimensionError`는 `AiClientError`의 서브클래스**(`ai_client.py:23`)다. 그래서 1차 임베딩(qwen3)이 **차원 불일치**로 `EmbeddingDimensionError`를 던지면 `_embed_query`의 `except ai_client.AiClientError`(`search.py:270`)에 걸려 곧바로 **BGE-M3 폴백**으로 넘어간다. 즉 "차원 불일치"도 폴백을 트리거하는 조건에 포함된다.
10. **`embedding_model` pydantic 기본값은 빈 문자열**(`config.py:63`, `embedding_model: str = ""`)이며 env(환경변수) 주입이 전제다(주석 "예: qwen3:embedding"). §4.3의 "기본 = qwen3:embedding"은 하드코딩된 기본값이 아니라 **코드 주석이 상정하는 표준 모델**을 가리키는 표현으로, 엄밀히는 pydantic 필드 기본값과 구분된다(→ [core 섹션]의 "빈 문자열 강제 주입" 설계와 같은 맥락).
11. **§1의 "반환값 외 전역 상태 변경 없음"은 엄밀히는 과한 서술**이다. 첫 호출 시 `_kiwi`(`typo.py:52-53`), `_bge_model`(`search.py:243-247`), `ai_client`의 임베딩 클라이언트 등 **모듈 전역 싱글턴이 지연 초기화**되어 전역 상태가 변한다. 관측 불가한 메모이제이션 캐시(한 번 만들어 재사용하려고 저장해 두는 것)라 실질적 부작용은 아니지만(멱등 — 여러 번 해도 결과가 한 번 한 것과 같음), "부수효과 없음"은 "DB·임베딩 호출 외 관측 가능한 부수효과 없음"으로 이해해야 한다(코드 docstring도 "DB·임베딩 외 부수효과 없음"으로 한정).
    > 쉽게 말하면: 처음 한 번은 도구들을 준비해 서랍에 넣어 두느라 내부 상태가 살짝 바뀌지만, 그건 밖에서 보이지도 않고 결과에 영향도 없다. 그래서 사실상 "깨끗한 함수"로 봐도 무방하다.
