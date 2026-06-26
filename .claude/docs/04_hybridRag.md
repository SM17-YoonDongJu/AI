## 노션 링크: https://app.notion.com/p/04-Hybrid-RAG-37530798f08f81c0b591c210c3f46212

## 참여 컴포넌트

- **FastAPI** (RAG 모듈): 쿼리 라우팅, 오타 보정, tsvector·벡터 검색, RRF 통합, 메타데이터 역추적
- **qwen3:embedding** (Ollama, 기본값) / **BGE-M3** (sentence_transformers 폴백): 쿼리 임베딩 생성 (1024d)
- **PostgreSQL + pgvector**:
    - `search_terms` 테이블: 보험 도메인 정규 용어 목록, trigram 인덱스 (오타 보정)
    - tsvector 인덱스: 키워드 검색
    - `embedding` 컬럼 + HNSW 인덱스: 벡터 유사도 검색

Frontend와 Spring Boot는 직접 관여하지 않는다. FastAPI 내부 모듈로서 LangGraph 에이전트(05번)와 챗봇(12번)에서 함수 호출 방식으로 사용한다.

---

## 소프트웨어 레이어 구조

**[쿼리 라우터]**

LangGraph 에이전트 또는 챗봇으로부터 쿼리 텍스트와 보험 유형 정보를 받아 검색할 namespace 조합과 각 namespace의 top-k를 결정한다. 현재 구현 대상은 신체 관련 보험 약관(terms, `POLICY_CHUNKS`)·분쟁조정사례(case, `CASE_CHUNKS`) 2개 namespace다. 후유장해 분류표(level)·HIRA 수가·KCD(medical)는 테이블 미존재로 향후 확장한다. `namespace`는 물리 컬럼이 아니라 검색한 소스 테이블로 부여하는 파생값이다. 비신체 보험 관련 쿼리(자동차·화재 등)는 범위 외로 안내한다.

**[trigram 오타 보정]**

사용자 입력 쿼리를 `search_terms` 테이블의 pg_trgm trigram 인덱스로 조회한다. `similarity(input, term) > 0.4` 조건으로 가장 유사한 정규 용어를 반환하며 오타·약어·구어체 표현을 보험 도메인 정규 용어로 치환한다. 보정된 쿼리가 이후 tsvector 검색과 임베딩 생성에 사용된다.

**[tsvector 키워드 검색]**

보정된 쿼리 텍스트를 tsvector 인덱스에서 키워드 기반 top-k 청크를 검색한다. 보험 도메인 특화 용어("상해후유장해", "외모추상" 등) 정밀 매칭에 강점이 있다.

**[pgvector 벡터 검색]**

보정된 쿼리로 qwen3:embedding(Ollama, 기본값) 임베딩(1024d)을 생성하고 pgvector HNSW 인덱스에서 코사인 유사도 기반 top-k 청크를 검색한다. 의미론적 유사성에 강점이 있다.

**[RRF 통합]**

tsvector와 벡터 검색 결과를 RRF(Reciprocal Rank Fusion) 알고리즘으로 통합한다. `score = Σ 1/(60 + rank_i)` 공식으로 최종 순위를 산출한다. 두 검색의 기본 가중치는 0.5:0.5이며 namespace별로 조정 가능하다.

**[메타데이터 역추적]**

RRF 통합 후 상위 청크에서 조항 번호·별표·출처 URL을 자동 역추적하여 인용 근거를 생성한다.

---

## 데이터 흐름 (순서)

1. LangGraph 에이전트 또는 챗봇이 쿼리 텍스트와 namespace 힌트를 전달
2. 쿼리 라우터가 검색할 namespace 조합과 top-k 결정
3. `search_terms` 테이블 pg_trgm 인덱스로 오타 보정 → 정규 용어로 쿼리 치환
4. 보정된 쿼리로 tsvector 키워드 검색과 pgvector 벡터 검색을 병렬 실행
5. RRF로 두 결과 통합 및 재순위화
6. 메타데이터 역추적으로 인용 근거 생성
7. 최종 ranked chunks + citations 반환

---

## 컴포넌트 간 통신 방식

| 구간 | 방식 |
| --- | --- |
| LangGraph / 챗봇 → RAG 모듈 | Python 함수 내부 호출 |
| RAG 모듈 → search_terms (trigram) | asyncpg (SQL, `similarity()`  • pg_trgm 인덱스) |
| RAG 모듈 → tsvector 인덱스 | asyncpg (SQL) |
| RAG 모듈 → Ollama (qwen3:embedding) | HTTP localhost (기본값) |
| RAG 모듈 → pgvector | asyncpg (SQL) |

!04_Hybrid_RAG_검색_파이프라인.png