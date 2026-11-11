## 🔗 관련 이슈
Closes #8

## ✅ 작업 내용

### 배경 — RAG가 의존할 토대부터 검색 본체까지 한 번에 세웠습니다

보험·법률 AI 엔진의 Hybrid RAG 검색(04번)을 구현하려 했는데, 시작 시점엔 `src/core`도 `src/rag`도 README만 있는 골격 상태였습니다. RAG의 `search()`는 혼자 못 돌고 **공용 인프라(설정·DB 풀·임베딩 클라이언트·계약 모델·로깅)** 와 **DB 스키마**에 의존합니다. 그래서 토대(core) → 마이그레이션 → 검색 본체 순으로 의존 사슬을 따라 쌓았습니다.

또한 계약 문서(`contracts.md`)와 ERD가 어긋나 있어(예: 존재하지 않는 `level`·`medical` namespace, 보유 컬럼 없는 `Citation.source_url`), 구현 전에 **실제 스키마(POLICY_CHUNKS·CASE_CHUNKS) 기준으로 계약을 정합**시켰습니다.

<br/>

### 1. core 공용 인프라 구축 (`src/core`)

모든 워커·공용 모듈이 의존하는 토대 계층입니다. `contracts.md`를 코드로 옮겨 단일 출처를 만들고, 외부 자원 접근을 async 핸들로 추상화했습니다.

#### 세부 항목
- `core/contracts.py` → Kafka(`OcrJob`·`ReportJob`)·RAG(`Chunk`·`Citation`·`RagResult`)·가드레일 결과 모델. pydantic v2로 진입점 검증.
- `core/config.py` → pydantic-settings 환경설정. 모델·엔드포인트는 **주입(하드코딩 금지)**, 시크릿 비노출.
- `core/db.py` → asyncpg 풀 lifecycle + `pgvector` 타입 등록(`vector(1024)` ↔ `list[float]`).
- `core/ai_client.py` → OpenAI 호환 `chat`/`embed`. 임베딩은 **1024차원 보장**(불일치 시 예외).
- `core/logging.py` → structlog 구조적 로깅. OTel 필드 네이밍, `trace_id` contextvars 전파, **PII 자동 마스킹 방어선**.

```python
# core/contracts.py — RAG 반환 계약
class Chunk(BaseModel):
    text: str           # 청크 원문
    namespace: str      # terms(POLICY_CHUNKS) | case(CASE_CHUNKS) — 소스 테이블 파생값
    score: float        # RRF 통합 점수
    source_ref: str     # chunk_id

class Citation(BaseModel):
    clause_no: str | None = None   # article_number / case_number
    exhibit: str | None = None     # section
```

<br/>

### 2. RAG DB 마이그레이션 + 한국어 토큰화 의존성 (`migrations/`)

마이그레이션 프레임워크 없이 **순서 있는 평문 SQL**(멱등)로 구성했습니다.

#### 세부 항목
- `vector`·`pg_trgm` 확장
- `policy_chunks`(terms)·`case_chunks`(case) — `content`·`content_tokens`·`embedding vector(1024)`·`content_tsv`(STORED generated, `to_tsvector('simple', content_tokens)`)
- `search_terms` — 오타보정 사전(**ERD 제외, 마이그레이션 only** — 비즈니스 엔터티 아님)
- 인덱스: **HNSW**(`vector_cosine_ops`)·tsvector **GIN**·`search_terms` **pg_trgm GIN**
- `kiwipiepy`(한국어 형태소 토큰화) 의존성 추가

<br/>

### 3. Hybrid RAG 검색 구현 (`src/rag`)

`report_worker`·`chatbot`이 함수 호출로 쓰는 순수 함수형 검색 모듈입니다.

```python
from rag import search, RagError
async def search(query, insurance_type=None, namespaces=None, top_k=8) -> RagResult
```

#### 세부 항목
- `rag/router.py` → namespace 결정. **비신체보험(자동차·화재)은 빈 결과**(범위 외, 예외 아님)
- `rag/typo.py` → Kiwi 토큰화 + `search_terms` trigram 보정(`similarity > 0.4`)
- `rag/fusion.py` → **RRF 순수 함수**(`score = Σ 1/(60+rank)`, 키워드:벡터 0.5:0.5)
- `rag/search.py` → tsvector ∥ pgvector **병렬**(`asyncio.gather`) 조립 → RRF → 인용 역추적

<br/>

### 4. 테스트 (총 46 passed)

외부 의존(PG·Ollama) 없이 단위·계약 검증으로 격리했습니다.
- 계약 모델(JSON 라운드트립·필드 검증), config·db·ai_client(mock/monkeypatch), logging(PII 마스킹)
- RAG: RRF 순수함수·라우터 범위외·인용 역추적·Kiwi 토큰화·`search()` e2e(fake pool)

## 💬 고민했던 부분

### 한국어 tsvector 전략 — 앱단 형태소 토큰화 + `to_tsvector('simple')`

PostgreSQL 기본 tsvector는 한국어 형태소를 못 나눕니다. 후보는 mecab 기반 PG 확장·PGroonga·pg_bigm·앱단 토큰화였는데, **인프라 제약(AWS RDS)** 이 결정적이었습니다. RDS는 커스텀 mecab 확장·PGroonga를 못 깔지만 앱단 토큰화는 확장 없이 동작하고 로컬↔RDS 환경차도 없습니다. 마침 ERD의 `content_tokens` 컬럼이 정확히 이 용도라, **적재 시 Kiwi로 토큰화 → `content_tokens` 저장 → `to_tsvector('simple', …)` STORED generated 컬럼 + GIN**으로 일관 적용했습니다. (짧은 키워드 recall이 부족하면 RDS 지원되는 pg_bigm을 보강으로 추가 가능)

### 계약을 실제 스키마에 정합 — namespace는 파생값, source_url 제거

`contracts.md`는 4개 namespace(terms·level·case·medical)와 `Citation.source_url`을 가정했지만, 실제 ERD엔 `POLICY_CHUNKS`(terms)·`CASE_CHUNKS`(case)만 있고 출처 URL 컬럼이 없습니다. PG에 `namespace` 컬럼을 두지 않고 **검색한 소스 테이블로 부여하는 파생값**으로 정의해, `level`·`medical` 추가 시 backward-compatible하도록 했습니다. `source_url`은 보유 컬럼이 없어 계약에서 제외했습니다.

### 임베딩 실패 시 degrade

검색은 가용성이 중요해서, 임베딩 실패가 검색 전체를 막지 않도록 **qwen3 → BGE-M3 폴백 → 키워드 전용 degrade**로 단계적 강등합니다(경고 로깅 동반). 폴백 경계의 광범위 `except`는 이 복원력을 위한 의도적 선택입니다(컨벤션 §8 예외, 주석 명시).

---

> **참고**: 이 브랜치엔 RAG 외에 `docs: RAG 계약 정합`·`chore: git-committer 스킬` 커밋도 포함됩니다(같은 브랜치 작업분).
>
> **검증 한계**: 외부 PG·Ollama 미기동 + 청크/`search_terms` 데이터 미적재로 **런타임 통합·랭킹 정성 평가는 미수행**입니다. docker-compose 기동 + 시드 적재 후 통합 테스트가 후속으로 필요합니다.
