# 데이터베이스 구조 전체

> 출처: AI 엔진 아키텍처 문서 세트 · 최종 점검일 2026-07-15 · 브랜치 `11-feature-langgraph-멀티에이전트-구현`
> 상위: [README](./README.md) · 원본 코드 정독 + 적대적 교차검증(코드 재대조) 완료

### 🎯 한 문장 요약
보험·법률 AI 엔진이 쓰는 PostgreSQL 데이터베이스의 모든 테이블을, 누가 만들고 소유하는지·어떤 컬럼과 인덱스가 있는지·어느 코드가 언제 읽고 쓰는지까지 코드 근거(`파일:라인`)와 함께 정리한 설명서다.

### 🌱 쉽게 말하면
데이터베이스는 여러 개의 큰 엑셀 표(테이블)를 모아 둔 창고라고 생각하면 된다. 이 문서는 그 창고 안에 어떤 표들이 있고, 각 표에 무슨 칸(컬럼)이 있으며, 누가 그 표를 만들고 누가 내용을 채우는지를 하나하나 안내하는 지도다.

창고 안 표는 크게 두 종류다. 하나는 AI가 "비슷한 문장 찾기"에 쓰는 검색용 표(약관·판례·장해분류표), 다른 하나는 실제 업무 데이터가 담기는 표(청구·리포트 같은 것)다. 그리고 이 표들을 누가 만드느냐도 갈린다. 검색용 표는 이 Python 엔진이 직접 만들고, 업무 표는 옆 동네인 Spring Boot 앱이 만들어 둔 것을 Python이 빌려 쓴다. 이 문서는 그 경계선과 실제 데이터가 흘러가는 길을 모두 그려 놓은 것이다.

이 섹션은 보험·법률 AI Python 엔진이 사용하는 PostgreSQL 스키마 전부를 소유권 경계·컬럼 단위·인덱스·읽기/쓰기 경로까지 정독해 정리한다. 근거는 모두 `파일경로:라인` 형식으로 붙였고, 핵심 SQL은 원문 그대로 인용한다.

---

## 1. DB 소유권 경계 — 누가 어떤 표를 만드나

DB 객체는 **세 갈래**로 나뉜다. 물리적으로는 하나의 PostgreSQL 인스턴스(데이터베이스 서버 한 대)지만, "누가 스키마(표의 설계도)를 만들고 소유하는가"가 서로 다르다.

> 쉽게 말하면: 건물은 한 채인데, 그 안의 방(표)마다 주인이 다른 셈이다. 어떤 방은 Python 엔진이 짓고, 어떤 방은 옆 앱(Spring Boot)이 짓는다.

### 1-A. Python 마이그레이션(`migrations/`)이 소유 — RAG 벡터 인프라

`migrations/`는 마이그레이션 프레임워크(DB 표 변경을 자동으로 관리해 주는 도구) 없이, 숫자 접두사 순서대로 하나씩 적용하는 평문 SQL 모음이다. 전부 멱등(같은 걸 여러 번 실행해도 결과가 한 번 한 것과 같음)하게 작성됐다 — `IF NOT EXISTS`(이미 있으면 건너뛰기)와 `DO $$`(조건부 실행 블록)를 써서다(`migrations/README.md:3-5`, `:62`). 이 폴더가 소유하는 것은 **Hybrid RAG(04번 검색 파이프라인)용 4개 청크/사전 테이블 + 2개 확장**뿐이다.

> 여기서 "청크(chunk)"는 긴 문서를 검색하기 좋게 잘게 자른 조각을 뜻하고, "확장(extension)"은 PostgreSQL에 새 기능을 끼워 넣는 플러그인 같은 것이다.

| 순서 | 파일 | 소유 객체 |
|------|------|-----------|
| 001 | `001_extensions.sql` | `vector`(pgvector), `pg_trgm` 확장 (`001_extensions.sql:7-8`) |
| 002 | `002_policy_chunks.sql` | `policy_chunks` (약관, namespace=terms) + 5개 인덱스 |
| 003 | `003_case_chunks.sql` | `case_chunks` (판례·금감원 분쟁조정례, namespace=case) + 5개 인덱스 |
| 004 | `004_search_terms.sql` | `search_terms` (정규 용어 사전) + trigram GIN |
| 005 | `005_schedule_chunks.sql` | `schedule_chunks` (후유장해분류표, namespace=level) + 5개 인덱스 |

> 표 읽는 법: `pgvector`는 "문장을 좌표(숫자 벡터)로 바꿔 비슷한 문장을 찾게 해 주는" 확장이고, `pg_trgm`은 "글자 세 개씩 쪼개 오타를 견디는 검색"을 해 주는 확장이다. namespace는 뒤(2장)에서 자세히 설명한다.

출처가 "Hybrid RAG(04)용 PostgreSQL 스키마"로 못박혀 있다(`migrations/README.md:3`). 즉 **업무/앱 테이블(reports 등)은 migrations/가 만들지 않는다.**

### 1-B. Spring Boot 메인 앱이 소유 — 업무/앱 테이블

`ocr_results`·`user_claims`·`user_insurances`·`reports`·`report_drafts`·`report_issues`·`insurance_products`는 Spring Boot 게이트웨이/메인 앱이 소유한다(즉 Python이 아니라 옆 앱이 만든다). 근거는 코드 주석에 직접 박혀 있다.

- `policy_chunks.product_id` 컬럼 주석: `-- 상품 FK (nullable). REFERENCES insurance_products(id)는 메인 앱 마이그레이션에서 관리.`(`002_policy_chunks.sql:39-40`, 동일하게 `tempVectorDB/init/01_schema.sql:38-39`). 즉 `insurance_products`는 **참조만** 하고 Python은 만들지 않는다. (FK = 외래 키, 다른 표의 행을 가리키는 연결 고리.)
- `policy_chunks.table_id` 주석: `-- S3 key → policy-tables/{table_id}.md (FK 없음)`(`002_policy_chunks.sql:35`) — Python 쪽에는 FK 제약을 걸지 않는다.
- CLAUDE.md에 "Spring Boot는 게이트웨이(업로드/JWT/S3/Kafka 발행) — 별도 범위"로 명시.

`report_worker`는 이 앱 테이블들을 **읽고(SELECT) / 갱신(UPDATE·UPSERT·INSERT)**하지만, 표 자체를 새로 만들지는 않는다. `reports` 행은 이미 존재한다고 가정하고 `UPDATE`(기존 행 수정)만 한다(§3, §5 참조).

> 쉽게 말하면: report_worker는 남이 지어 둔 방에 들어가 내용물만 채우고 갈아 끼운다. 방 자체를 새로 짓지는 않는다.

### 1-C. `tempVectorDB/init/` — 실험용 로컬 복제본(정본 겸용)

`tempVectorDB/init/`는 개발자 PC에서 docker(로컬에 서버 환경을 그대로 흉내 내는 컨테이너 도구)를 띄울 때 쓰는 초기화 스크립트다. 성격이 두 겹인데, **RAG 테이블에 대해서는 정본(canonical, 기준이 되는 원본)이고, 앱 테이블에 대해서는 실험용 최소 복제본**이다.

- `01_schema.sql`: `policy_chunks` + `search_terms`. 주석 "Policy-Chunker(insurance-chunker) db/schema.sql 와 정합 유지"(`tempVectorDB/init/01_schema.sql:2`).
- `02_app_tables.sql`: 앱 테이블 6종. 파일 헤더가 성격을 규정한다 — `-- 앱 테이블 (Notion ERD 기준 최소 집합) — report_worker 실험용. / -- 전체 ERD 아님. report_worker가 읽고/쓰는 테이블만.`(`tempVectorDB/init/02_app_tables.sql:1-2`). 즉 이 파일은 Spring 소유 테이블을 **로컬에서 report_worker를 돌려보려고 흉내 낸 복제본**이지 진실의 원천이 아니다.
- `03_case_chunks.sql`: `case_chunks` 정본.
- `04_schedule_chunks.sql`: `schedule_chunks` 정본.

**정본 관계**: `migrations/002`·`003`·`005`는 `tempVectorDB/init/01`·`03`·`04`를 "정본으로 삼아 동일 스키마를 재현"한다(`migrations/README.md:19-28`; 각 마이그레이션 상단 주석 `002:6`, `003:6`, `005:6`). 적재기(`tempVectorDB/load_cases.py`·`load_schedule.py`)와 검색기(`src/rag/search.py`)가 이 스키마에 맞춰 있으므로 두 소스는 항상 일치해야 한다(`migrations/README.md:22-23`).

> 쉽게 말하면: `tempVectorDB/init`의 검색용 표가 "원본 설계도"이고, `migrations/`는 그 설계도를 서버용으로 그대로 베껴 그린 사본이다. 원본과 사본이 어긋나면 검색이 깨지므로 둘은 늘 똑같아야 한다.

> **정직한 불일치 표기 — `search_terms` 스키마 드리프트**: `migrations/004`의 `search_terms`는 `(term PK, namespace, source)` 3컬럼이고 인덱스명이 `idx_search_terms_term_trgm`이다(`004_search_terms.sql:7-15`). 반면 `tempVectorDB/init/01_schema.sql`의 `search_terms`는 `term` 단일 컬럼이고 인덱스명이 `idx_search_terms_trgm`이다(`tempVectorDB/init/01_schema.sql:66-71`). 두 소스가 정본이라면서도 이 테이블만 컬럼·인덱스명이 어긋나 있다(이런 어긋남을 "드리프트"라고 부른다). 로컬 init에는 `namespace`/`source` 컬럼이 없다.

---

## 2. RAG 벡터 테이블 4종 — AI가 검색에 쓰는 표들

RAG(문서를 검색해 그 근거로 답을 만드는 방식)는 **4개 테이블**을 쓴다. 이 중 3개(`policy_chunks`·`case_chunks`·`schedule_chunks`)가 임베딩(문장을 숫자 좌표로 바꾼 값)을 담는 벡터 테이블이고, `search_terms`는 벡터 없는 trigram 사전이다.

> 쉽게 말하면: 임베딩은 문장을 지도 위의 좌표로 바꾸는 것이다. 뜻이 비슷한 문장은 좌표도 가까이 찍히므로, "이 질문과 가까운 좌표의 문장들"을 뽑으면 관련 있는 근거를 찾을 수 있다. 벡터 테이블은 그 좌표들을 저장해 두는 표다.

### namespace ↔ 테이블 매핑 (파생값)

`namespace`(검색 대상 구역을 가리키는 이름표)는 **표에 실제로 저장된 컬럼이 아니라, 어느 소스 테이블에서 검색했는지에 따라 붙여 주는 파생값**이다(`002:2-3`, `003:2-3`, `005:2-3`, `migrations/README.md:16-17`). 검색기 코드가 이 매핑을 상수로 고정한다:

```python
# src/rag/search.py:40-44
_NS_TABLE: dict[str, str] = {
    "terms": "policy_chunks",
    "case": "case_chunks",
    "level": "schedule_chunks",
}
```

> 위 코드 설명: namespace 이름표("terms"·"case"·"level")를 실제 표 이름으로 바꿔 주는 대응표다. 예를 들어 "terms"라고 하면 `policy_chunks` 표를 뒤진다.

| namespace | 소스 테이블 | 내용 | 필터 특성 |
|-----------|-------------|------|-----------|
| `terms` | `policy_chunks` | 신체 관련 보험 약관 | insurer/product 메타필터 유효 (`search.py:69`) |
| `case` | `case_chunks` | 판례·금감원 분쟁조정례 | 메타필터 안 검 — insurer/product는 nullable 참고 메타라 걸면 recall 0 붕괴 (`search.py:67-69`, `:75-77`) |
| `level` | `schedule_chunks` | 후유장해분류표(금감원 별표) | `contract_date` 버전필터 유효 (`search.py:92`) |
| `medical` | (테이블 미존재) | HIRA 수가·KCD | **미구현** — 향후 확장 (`migrations/README.md:83`, `contracts.py:102`) |

> 표 읽는 법: "메타필터"는 검색할 때 "이 보험사 것만" 식으로 조건을 좁히는 필터다. `case`는 그런 조건 칸이 비어 있는 경우가 많아, 조건을 걸면 검색 결과(recall = 실제로 찾아오는 비율)가 0으로 무너진다. 그래서 case는 필터를 안 건다. `medical`은 아직 표조차 없는 **미구현** 항목이다.

`search_terms`는 namespace가 아니다. 입력 쿼리를 trigram 유사도(`similarity(input, term) > 0.4`)로 조회해 오타·약어·구어체를 정규 용어(표준 표현)로 치환하는 **04 파이프라인의 오타 보정 단계 사전**이다(`004_search_terms.sql:1-5`).

> 쉽게 말하면: 사용자가 "디스크"라고 쳐도 표준 용어 "추간판탈출증"으로 바로잡아 주는 맞춤법 교정 사전 같은 역할이다. trigram은 글자를 세 개씩 겹쳐 쪼개 비교하기 때문에 오타가 조금 있어도 비슷한 단어를 찾아낸다.

### 2-1. `policy_chunks` (약관, namespace=terms)

정본 `tempVectorDB/init/01_schema.sql:9-40` / 재현 `migrations/002_policy_chunks.sql:10-41`.

| 컬럼 | 타입 | 의미 (원문 주석) |
|------|------|------------------|
| `chunk_id` | `TEXT` **PK** | 청크 식별자 (`002:11`) |
| `content` | `TEXT NOT NULL` | 임베딩 원문 (`002:12`) |
| `content_tokens` | `TEXT` | Kiwi 형태소 결과(공백 구분) → tsvector 전문검색 (`002:13`) |
| `embedding` | `halfvec(1024)` | qwen3:embedding 1024d / BGE-M3 1024d (float16) (`002:14`) |
| `token_count` | `INT` | 토큰 수 (`002:15`) |
| `chunk_type` | `TEXT NOT NULL` | `coverage\|exclusion\|definition\|special_clause\|duty\|claim\|termination\|schedule\|general` (`002:16`) |
| `doc_hash` | `TEXT NOT NULL` | PDF sha256, 중복 ingest 방지 (`002:17`) |
| `page_number` | `INT` | 페이지 번호 (`002:18`) |
| `ingested_at` | `TIMESTAMPTZ DEFAULT now()` | 적재 시각 (`002:19`) |
| `insurer` | `TEXT NOT NULL` | 보험사(검색 필터) (`002:22`) |
| `product_name` | `TEXT NOT NULL` | 상품명(검색 필터) (`002:23`) |
| `product_code` | `TEXT` | 상품 코드 (`002:24`) |
| `effective_date` | `DATE` | 시행일 (`002:25`) |
| `article_number` | `TEXT` | "제12조" (`002:28`) |
| `article_title` | `TEXT` | "보험금을 지급하지 않는 사유" (`002:29`) |
| `generation` | `TEXT` | 세대(예: "4세대") (`002:30`) |
| `section` | `TEXT` | 경계 라벨 또는 편/장 경로 (`002:31`) |
| `chunk_index` | `INT` | 문서 전체 순서(조항 복원 시 ORDER BY) (`002:32`) |
| `table_id` | `UUID` | 표 row 청크 전용. S3 key → `policy-tables/{table_id}.md` (FK 없음) (`002:35`) |
| `row_start` | `SMALLINT` | 표 row 시작 (`002:36`) |
| `row_end` | `SMALLINT` | 표 row 끝 (`002:37`) |
| `product_id` | `UUID` | 상품 FK(nullable). `REFERENCES insurance_products(id)`는 메인 앱 관리 (`002:39-40`) |

> 표 읽는 법 (자주 나오는 용어): **PK**(기본 키, 각 행을 유일하게 구분하는 값), **content_tokens**는 Kiwi(한국어 형태소 분석기)로 문장을 단어별로 쪼갠 결과인데 이걸로 tsvector(PostgreSQL이 본문을 빠르게 검색하려고 단어별로 쪼개 만든 색인) 전문검색을 한다. **embedding**의 `halfvec(1024)`는 1024개 숫자로 된 좌표를 절반 정밀도(float16)로 저장한다는 뜻이다. **doc_hash**는 같은 PDF를 두 번 넣지 않으려고 문서 지문(sha256)을 저장해 두는 칸이다.

**인덱스 (`002:44-63`)**:
```sql
CREATE INDEX IF NOT EXISTS idx_policy_hnsw
    ON policy_chunks USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 64);
CREATE INDEX IF NOT EXISTS idx_policy_fts
    ON policy_chunks
    USING gin (to_tsvector('simple', coalesce(content_tokens, '')));
CREATE INDEX IF NOT EXISTS idx_policy_meta
    ON policy_chunks (insurer, chunk_type, effective_date);
CREATE INDEX IF NOT EXISTS idx_policy_doc_hash ON policy_chunks (doc_hash);
CREATE INDEX IF NOT EXISTS idx_policy_table_id ON policy_chunks (table_id);
```

> 인덱스는 책 뒤의 "찾아보기(색인)"와 같다. 없으면 표를 처음부터 끝까지 훑어야 하지만, 있으면 원하는 행으로 바로 점프한다.

- `idx_policy_hnsw`: 벡터로 "비슷한 문장 빠르게 찾기"(ANN, 근사 최근접 이웃 검색) — HNSW 방식, `halfvec_cosine_ops`(코사인 거리로 비교), m=16/ef_construction=64.
- `idx_policy_fts`: 키워드 전문검색 — **함수식 GIN**(계산식 결과에 거는 색인). 검색기가 동일 표현식 `to_tsvector('simple', coalesce(content_tokens,''))`로 `@@` 매칭하므로 인덱스 표현식과 정확히 일치한다(`migrations/README.md:64-71`).
- `idx_policy_meta`: 메타 필터 (보험사·청크타입·시행일). `terms` namespace가 insurer/product 필터를 걸 때 활용(`search.py:69`).
- `idx_policy_doc_hash`: 중복 ingest 방지 조회.
- `idx_policy_table_id`: 표 row 청크 조회.

> 쉽게 말하면: 이 표에는 색인이 두 종류 걸려 있다. 하나는 "뜻이 비슷한 문장"을 찾는 벡터 색인(hnsw), 다른 하나는 "그 단어가 든 문장"을 찾는 키워드 색인(fts)이다. 나머지 셋은 조건 좁히기·중복 방지용이다.

### 2-2. `case_chunks` (판례·금감원 분쟁조정례, namespace=case)

정본 `tempVectorDB/init/03_case_chunks.sql:8-36` / 재현 `migrations/003_case_chunks.sql:19-47`.

| 컬럼 | 타입 | 의미 |
|------|------|------|
| `chunk_id` | `TEXT` **PK** | 청크 식별자 (`003:20`) |
| `content` | `TEXT NOT NULL` | 임베딩 원문 (`003:21`) |
| `content_tokens` | `TEXT` | Kiwi 형태소(공백 구분) → tsvector (`003:22`) |
| `embedding` | `halfvec(1024)` | qwen3-embedding 1024d (약관과 동일) (`003:23`) |
| `token_count` | `INT` | 토큰 수 (`003:24`) |
| `chunk_type` | `TEXT NOT NULL` | `holding\|summary\|reasoning\|order\|decision\|fact\|general` (`003:25`; init 03:14 상세 라벨) |
| `doc_hash` | `TEXT NOT NULL` | 원문 sha256, 중복 방지 (`003:26`) |
| `ingested_at` | `TIMESTAMPTZ DEFAULT now()` | 적재 시각 (`003:27`) |
| `source_type` | `TEXT NOT NULL` | `court_precedent`(판례) \| `fss_mediation`(금감원 분쟁조정례) (`003:30`) |
| `institution` | `TEXT` | 대법원\|서울중앙지법\|금융감독원 … (`003:31`) |
| `case_no` | `TEXT` | 사건번호 "2021다1234" / 조정번호 "제2022-15호" (`003:32`) |
| `case_title` | `TEXT` | 사건명 (`003:33`) |
| `holding` | `TEXT` | 판시사항 요약(조항 복원용 헤더) (`003:34`) |
| `decision_date` | `DATE` | 선고일 / 결정일 (`003:35`) |
| `source_url` | `TEXT` | 출처 URL (`003:36`) |
| `insurer` | `TEXT` | 관련 보험사 (nullable 참고 메타) (`003:39`) |
| `product_name` | `TEXT` | 관련 상품 (nullable 참고 메타) (`003:40`) |
| `accident_type` | `TEXT` | ERD accident_type (nullable) (`003:41`) |
| `tags` | `TEXT[]` | 쟁점 태그 (후유장해, 면책, 고지의무 …) (`003:42`) |
| `section` | `TEXT` | 편/장/항 경로 또는 경계 라벨 (`003:45`) |
| `chunk_index` | `INT` | 문서 내 순서(복원 시 ORDER BY) (`003:46`) |

> 표 읽는 법: `source_type`은 이 조각이 법원 판례인지 금감원 분쟁조정례인지 구분한다. `insurer`·`product_name`은 "참고용"일 뿐이고 비어 있을 수 있어(nullable) 검색 조건으로는 쓰지 않는다(아래 "필터 특이사항" 참고). `tags`는 `TEXT[]`, 즉 여러 개의 문자열을 한 칸에 배열로 담는 타입이다.

**인덱스 (`003:50-69`)**: `idx_case_hnsw`(HNSW halfvec_cosine_ops m=16/64), `idx_case_fts`(함수식 GIN tsvector), `idx_case_meta (source_type, accident_type, decision_date)`, `idx_case_tags USING gin (tags)`(쟁점 태그), `idx_case_doc_hash (doc_hash)`.

> **주의(마이그레이션 이력)**: 구버전(`case_number`/`block_type` enum/`court_level`) 스키마로 이미 `case_chunks`를 만든 DB는 `CREATE TABLE IF NOT EXISTS`가 스킵돼(이미 표가 있으니 새로 안 만들고 건너뛰어) 낡은 컬럼이 남고, case 검색·적재가 `UndefinedColumnError`(없는 컬럼을 찾는 오류)로 전면 실패한다. `DROP TABLE case_chunks; DROP TYPE case_outcome; DROP TYPE case_block_type;`(표와 옛 타입을 지운 뒤) 후 재적용해야 한다(`003_case_chunks.sql:10-17`, `migrations/README.md:26-28`).

> 쉽게 말하면: 옛날 설계로 만든 표가 남아 있으면, "이미 있으니 건너뛴다" 규칙 때문에 새 설계가 적용되지 않는다. 이럴 때는 옛 표를 지우고 처음부터 다시 만들어야 한다.

> **필터 특이사항**: `insurer`/`product_name`은 nullable 참고 메타일 뿐 필터 키가 아니다. 적재기가 채우지 않으므로 걸면 case recall이 0으로 붕괴한다 → 검색기는 case namespace에 메타필터를 걸지 않는다(`search.py:67-69`, `:75-77`).

### 2-3. `schedule_chunks` (후유장해분류표, namespace=level)

정본 `tempVectorDB/init/04_schedule_chunks.sql:12-38` / 재현 `migrations/005_schedule_chunks.sql:15-41`.

| 컬럼 | 타입 | 의미 |
|------|------|------|
| `chunk_id` | `TEXT` **PK** | 청크 식별자 (`005:16`) |
| `content` | `TEXT NOT NULL` | 임베딩 원문 (`005:17`) |
| `content_tokens` | `TEXT` | Kiwi 형태소 → tsvector (`005:18`) |
| `embedding` | `halfvec(1024)` | qwen3-embedding 1024d (약관·판례와 동일) (`005:19`) |
| `token_count` | `INT` | 토큰 수 (`005:20`) |
| `chunk_type` | `TEXT NOT NULL` | `schedule`(장해분류표) **고정** (약관 chunk_type 체계와 정합) (`005:21`) |
| `doc_hash` | `TEXT NOT NULL` | 원문 sha256, 중복 방지 (`005:22`) |
| `ingested_at` | `TIMESTAMPTZ DEFAULT now()` | 적재 시각 (`005:23`) |
| `body_part` | `TEXT NOT NULL` | 신체부위 분류: 눈\|귀\|코\|씹어먹거나말하기\|외모\|척추\|체간골\|팔\|다리\|손가락\|발가락\|흉복부장기및비뇨생식기\|신경계정신행동 등 (`005:26-28`) |
| `disability_grade` | `TEXT` | 장해등급/지급률 항목 라벨 (nullable) (`005:29`) |
| `rate` | `NUMERIC` | 항목 단일 지급률(%) — 표 행이 단일 지급률일 때 (nullable) (`005:30`) |
| `version_label` | `TEXT NOT NULL` | 개정판 라벨 (예: "2018.4 개정", "2005 표준약관") (`005:33`) |
| `applies_from` | `DATE NOT NULL` | 이 버전 적용 시작일 (계약 체결일 >= applies_from) (`005:34`) |
| `applies_to` | `DATE` | 적용 종료일(exclusive). **NULL이면 현행판** (`005:35`) |
| `source_url` | `TEXT` | 출처 URL (`005:36`) |
| `section` | `TEXT` | 표 내 절/항목 경로 또는 경계 라벨 (`005:39`) |
| `chunk_index` | `INT` | 문서 내 순서 (`005:40`) |

> 표 읽는 법: 후유장해분류표는 "어느 신체부위에 어떤 장해가 남으면 지급률 몇 %"를 규정한 표다. `body_part`가 신체부위, `rate`가 지급률(%)이다. `applies_from`/`applies_to`는 이 버전이 언제부터 언제까지 유효한지를 나타내며, `applies_to`가 비어 있으면(NULL) 지금 쓰는 최신판이라는 뜻이다. (exclusive = 종료일 당일은 포함 안 함.)

**인덱스 (`005:44-63`)**: `idx_schedule_hnsw`(HNSW halfvec_cosine_ops), `idx_schedule_fts`(함수식 GIN tsvector), `idx_schedule_version (applies_from, applies_to)`(버전/적용기간 필터), `idx_schedule_body_part (body_part)`(신체부위 필터), `idx_schedule_doc_hash (doc_hash)`.

**버전 매칭**: 후유장해분류표는 2018.4 대개정 등 시행세칙 개정마다 판이 갈린다. 검색기는 `level` namespace에 한해 `contract_date`(계약 체결일) 인자로 유효 버전만 필터한다(`search.py:92`, `:95-118`, `005:10-13`):
```
AND applies_from <= $idx AND (applies_to IS NULL OR $idx < applies_to)
```
`contract_date=None`이면 `applies_to IS NULL`(현행판)만 검색한다(`search.py:112`; 정책 근거 `migrations/README.md:78-79` — 005 SQL은 64줄뿐이라 해당 라인 없음).

> 쉽게 말하면: 장해분류표는 시대마다 개정판이 있어서, "이 사람이 계약한 날짜에 유효하던 판"을 골라 써야 공정하다. 위 조건은 "계약일이 이 버전의 유효 기간 안에 드는가"를 걸러내는 것이고, 계약일 정보가 없으면 그냥 현행판만 본다.

### 2-4. `search_terms` (정규 용어 사전, 벡터 없음)

`migrations/004_search_terms.sql:7-15` 기준(정본은 로컬 init과 컬럼이 다름 — §1-C 드리프트 참조).

| 컬럼 | 타입 | 의미 |
|------|------|------|
| `term` | `text` **PK** | 정규 용어 (`004:8`) |
| `namespace` | `text` | `terms \| case` (적용 대상 힌트, 파생값과 동일 체계) (`004:9`) |
| `source` | `text` | 용어 출처(약관/사례 등) (`004:10`) |

**인덱스**: `idx_search_terms_term_trgm ON search_terms USING gin (term gin_trgm_ops)` — trigram 오타 보정용(similarity / `%` 연산자) (`004:14-15`).

> 쉽게 말하면: 이 표만 벡터(좌표)가 없다. 대신 글자를 세 개씩 쪼개 비교하는 trigram 색인으로, 오타가 섞인 입력도 표준 용어로 바로잡는 데 쓴다.

### 벡터공간 통일 정리

세 벡터 테이블 모두 `embedding halfvec(1024)`이며 HNSW는 `halfvec_cosine_ops`(m=16, ef_construction=64)를 쓴다 — **동일 임베딩 모델(qwen3:embedding 1024d, BGE-M3 폴백)로 벡터공간이 일치**해야 교차 검색이 성립한다(`002:14`, `003:8`·`:23`, `005:8`·`:19`, `contracts.py:98`의 "임베딩 차원은 1024 고정"). 임베딩 코덱은 `db.py`가 커넥션마다 `register_vector`로 등록하고(`db.py:39-42`, `:18`), 벡터 검색 시 캐스트 없이 halfvec/vector 코덱으로 주고받는다(`search.py:226`).

> 쉽게 말하면: 세 표가 같은 "지도(임베딩 모델)"로 좌표를 찍어야, 약관·판례·장해표를 한꺼번에 놓고 "가까운 것 찾기"가 성립한다. 지도가 서로 다르면 좌표를 나란히 비교할 수 없다. (BGE-M3 폴백 = 기본 모델이 안 되면 대신 쓰는 예비 모델. 둘 다 1024차원이라 호환된다.)

---

## 3. 업무/앱 테이블 — 실제 업무 데이터가 담기는 표

전부 `tempVectorDB/init/02_app_tables.sql`의 실험용 복제본 정의를 기준으로 하되(진실의 원천은 Spring), `report_worker`의 실제 접근 방식을 함께 표기한다. `report_worker`가 각 테이블을 **읽는지/쓰는지, 어느 노드(작업 단계)에서** 하는지가 핵심이다.

> 쉽게 말하면: 여기부터는 검색용 표가 아니라, 사용자의 청구·가입정보·리포트 같은 진짜 업무 데이터를 담는 표들이다. report_worker가 각 표를 "읽기만" 하는지 "쓰기도" 하는지 눈여겨보면 된다.

### 3-1. `ocr_results` — OCR·PII 마스킹 결과 (report_worker 입력, **읽기 전용**)

정의 `02_app_tables.sql:5-12`.

> OCR = 이미지·PDF 속 글자를 텍스트로 뽑아내는 것. PII = 이름·주민번호 같은 개인식별정보. 마스킹 = 그 개인정보를 가리는 것.

| 컬럼 | 타입 | 의미 |
|------|------|------|
| `id` | `UUID` **PK** | OCR 결과 식별자. `ReportJob.ocr_result_id`가 참조 (`02:6`, `contracts.py:85`) |
| `job_id` | `UUID` | 원 OCR 작업 (`02:7`) |
| `doc_type` | `TEXT` | `diagnosis\|policy\|payout_notice\|claim\|other` (`02:8`, `contracts.py:47-54`) |
| `masked_text` | `TEXT` | PII 마스킹된 OCR 텍스트 (downstream 입력) (`02:9`) |
| `entities` | `JSONB` | 추출 엔티티 (`02:10`) |
| `created_at` | `TIMESTAMPTZ DEFAULT now()` | 생성 시각 (`02:11`) |

**접근**: `load_context` 노드가 `masked_text`, `entities`만 SELECT(읽기)한다. 쓰기는 없다(`agents.py:69-72`).

> 쉽게 말하면: OCR 워커가 미리 개인정보를 가려 둔 텍스트를 report_worker가 가져다 읽는 입력용 표다. 여기에는 아무것도 쓰지 않는다. (JSONB = JSON 형태의 데이터를 통째로 담는 유연한 칸. entities는 뽑아낸 정보 항목들이다.)

### 3-2. `user_claims` — 사용자 청구 (**읽기 전용**)

정의 `02_app_tables.sql:15-27`.

| 컬럼 | 타입 | 의미 |
|------|------|------|
| `id` | `UUID` **PK** | 청구 식별자. `reports.claim_id`/`ReportJob.claim_id`가 참조 (`02:16`) |
| `user_id` | `UUID` | 사용자 (`02:17`) |
| `product_id` | `UUID` | 상품 (`02:18`) |
| `offered_amount` | `BIGINT` | 제안 받은 보험금 (`02:19`) |
| `accident_date` | `DATE` | 사고일 (`02:20`) |
| `hospitalization` | `JSONB` | `[{hospitalStart, hospitalEnd, hospitalReason}]` (`02:21`) |
| `diagnosis` | `TEXT` | 진단명 (`02:22`) |
| `description` | `TEXT` | 설명 (`02:23`) |
| `additional_information` | `TEXT` | 추가 정보 (`02:24`) |
| `accident_type` | `TEXT` | ERD accident_type enum 값 (`02:25`) |
| `created_at` | `TIMESTAMPTZ DEFAULT now()` | 생성 시각 (`02:26`) |

**접근**: `load_context`가 `reports.claim_id`가 있을 때만 조건부로 `diagnosis, accident_date, accident_type, offered_amount, description, hospitalization`을 SELECT한다(`agents.py:78-84`).

> 쉽게 말하면: 사용자가 낸 보험금 청구 내용(사고일·진단명·제안받은 금액 등)이 담긴 표다. 리포트에 연결된 청구 건이 있을 때만 읽어 온다.

### 3-3. `user_insurances` — 사용자 가입보험 (**읽기 전용**)

정의 `02_app_tables.sql:30-43`.

| 컬럼 | 타입 | 의미 |
|------|------|------|
| `id` | `UUID` **PK** | 가입보험 식별자 (`02:31`) |
| `user_id` | `UUID` | 사용자 (`02:32`) |
| `insurer_name` | `TEXT` | 보험사 원문 — **`policy_chunks.insurer`와 매칭 키** (`02:33`) |
| `product_name` | `TEXT` | 상품명 원문 — **`policy_chunks.product_name`과 매칭 키** (`02:34`) |
| `product_id` | `UUID` | INSURANCE_PRODUCTS 매칭 (nullable) (`02:35`) |
| `match_status` | `TEXT` | `MATCHED\|UNMATCHED\|PENDING` (`02:36`) |
| `policy_no` | `TEXT` | 증권번호 (`02:37`) |
| `enrolled_at` | `DATE` | 가입일 — **표준 장해분류표 버전 매칭용**(`case_info.enrolled_at` → `contract_date`) (`02:38`, `agents.py:109-111`·`:401`) |
| `coverages` | `TEXT[]` | 가입 특약 → `subscribed_coverages` (`02:39`) |
| `policy_file_url` | `TEXT` | 약관 파일 URL (`02:40`) |
| `ocr_result_id` | `UUID` | 연관 OCR 결과 (`02:41`) |
| `created_at` | `TIMESTAMPTZ DEFAULT now()` | 생성 시각 (`02:42`) |

**접근**: `load_context`가 `insurer_name, product_name, coverages, enrolled_at`을 `reports.user_id` 기준 서브쿼리로 1건 SELECT한다(`agents.py:85-89`). 또 `policy_in_db` 분기가 `insurer`/`product_name`으로 `policy_chunks` 매칭 카운트를 조회한다(`agents.py:180-188`).

> 쉽게 말하면: 사용자가 어떤 보험사의 어떤 상품에 가입했는지, 어떤 특약을 들었는지, 언제 가입했는지가 담긴 표다. 여기 있는 `insurer_name`/`product_name`이 약관 표(`policy_chunks`)를 찾는 열쇠(매칭 키)가 되고, `enrolled_at`(가입일)은 어느 시기의 장해분류표를 쓸지 고르는 기준이 된다.

### 3-4. `reports` — 보상분석 리포트 (**UPDATE만**, 행은 Spring이 선생성)

정의 `02_app_tables.sql:46-65`.

| 컬럼 | 타입 | 의미 / report_worker 쓰기 여부 |
|------|------|------|
| `id` | `UUID` **PK** | 리포트 식별자. `ReportJob.report_id`(멱등키) (`02:47`, `contracts.py:83`) |
| `user_id` | `UUID` | 사용자 — `load_context`가 `user_insurances` 조인에 사용 (`02:48`) |
| `adjuster_id` | `UUID` | 손해사정사 (`02:49`) |
| `product_id` | `UUID` | 상품 (`02:50`) |
| `claim_id` | `UUID` | 청구 — `load_context`가 읽어 `user_claims` 조회 (`02:51`, `agents.py:74`·`:79`) |
| `accident_type` | `TEXT` | 사고유형 — `load_context` 읽음 (`02:52`, `agents.py:74`) |
| `treatment` | `TEXT` | 질병명 — `load_context`가 diagnosis 폴백으로 읽음 (`02:53`, `agents.py:103`) |
| `claimed_min_amount` | `BIGINT` | **persist가 UPDATE** ← `estimated_range.min` (`02:54`, SQL `agents.py:608` · 값바인딩 `:615`) |
| `claimed_max_amount` | `BIGINT` | **persist가 UPDATE** ← `estimated_range.max` (`02:55`, SQL `agents.py:608` · 값바인딩 `:616`) |
| `offered_amount` | `BIGINT` | 제안 보험금 — `load_context` 읽음 (`02:56`, `agents.py:74`) |
| `applicable_guarantees` | `TEXT[]` | **persist가 UPDATE** ← `applicable_coverages` (`02:57`, `agents.py:607`·`612`) |
| `omitted_special_contract` | `TEXT[]` | **persist가 UPDATE** ← `missing_coverages` (`02:58`, `agents.py:607`·`613`) |
| `basis_terms_precedents` | `TEXT[]` | **persist가 UPDATE** ← `basis`(약관+판례+장해 인용) (`02:59`, `agents.py:608`·`614`) |
| `question` | `TEXT` | 사용자 질문 — `load_context` 읽음 (`02:60`, `agents.py:74`) |
| `case_no` | `VARCHAR` | 사건번호 (`02:61`) |
| `status` | `TEXT` | `AWAITING_INSPECTION\|AWAITING_ADOPTION\|...` — **persist가 `'AWAITING_ADOPTION'`, persist_blocked가 `'BLOCKED'`로 UPDATE** (`02:62`, `agents.py:609`·`655`) |
| `created_at` | `TIMESTAMPTZ DEFAULT now()` | 생성 시각 (`02:63`) |
| `updated_at` | `TIMESTAMPTZ DEFAULT now()` | 갱신 시각 — persist/persist_blocked가 `now()`로 갱신 (`02:64`, `agents.py:609`·`655`) |

> 표 읽는 법: "persist가 UPDATE"라고 적힌 칸만 report_worker가 값을 채운다(마지막 `persist` 단계에서). 나머지는 읽기만 한다. 화살표 `←`는 "왼쪽 컬럼에 오른쪽 state 값을 넣는다"는 뜻이다. **멱등키**란 같은 report_id로 여러 번 처리해도 결과가 한 번과 같도록 하는 식별자다.

**쓰기 방식**: `persist`가 단일 `UPDATE ... WHERE id = $1`을 한다(`agents.py:605-617`). 행을 새로 만들지 않는다 → **Spring이 리포트 행을 미리 만들어 두고, report_worker는 결과 컬럼만 채운다.**

> 쉽게 말하면: 리포트라는 빈 서류 양식은 Spring이 먼저 만들어 둔다. AI 워커는 그 양식의 분석 결과 칸(추정 금액·적용 담보·근거·상태 등)만 채워 넣는다.

### 3-5. `report_drafts` — 리포트 초안 (**UPSERT, ON CONFLICT**)

정의 `02_app_tables.sql:68-73`.

> UPSERT = "있으면 수정(update), 없으면 새로 넣기(insert)"를 한 번에 하는 방식. ON CONFLICT = 같은 키가 이미 있어 충돌하면 어떻게 할지 정하는 구문.

| 컬럼 | 타입 | 의미 |
|------|------|------|
| `report_id` | `UUID` **PK** | `reports.id` 참조. UPSERT 충돌 키 (`02:69`) |
| `draft` | `JSONB` | sections·estimated_range·disclaimer·judge_failures·issues 등 (`02:70`) |
| `status` | `TEXT` | `draft\|signed\|rejected` (`02:71`) |
| `created_at` | `TIMESTAMPTZ DEFAULT now()` | 생성 시각 (`02:72`) |

**쓰기 방식**: `persist`가 `ON CONFLICT (report_id) DO UPDATE`로 멱등 UPSERT를 한다(`agents.py:598-604`, §5 참조). status는 항상 `'draft'`.

> 쉽게 말하면: 리포트의 상세 초안(본문 섹션·추정 금액·고지문 등)을 통째로 JSON 하나에 담아 둔다. 같은 리포트를 다시 처리하면 새 행을 늘리지 않고 기존 것을 덮어써서, 몇 번을 돌려도 초안은 한 개만 남는다.

### 3-6. `report_issues` — 리포트 쟁점 (**DELETE 후 INSERT**)

정의 `02_app_tables.sql:76-91`.

| 컬럼 | 타입 | 의미 / persist 쓰기 |
|------|------|------|
| `id` | `UUID` **PK** | 쟁점 식별자 — persist가 `uuid.uuid4()` 생성 (`02:77`, `agents.py:623`) |
| `report_id` | `UUID` | `reports.id` 참조 (`02:78`, `agents.py:624`) |
| `title` | `VARCHAR` | 제목 — persist가 `str(...)[:200]`로 절단 삽입 (`02:79`, `agents.py:625`) |
| `description` | `TEXT` | AI 의견/설명 (`02:80`, `agents.py:626`) |
| `ai_status` | `TEXT` | `CONFIRMED\|TRUSTED\|INFO` — 없으면 `'INFO'` (`02:81`, `agents.py:627`) |
| `review_status` | `TEXT DEFAULT 'PENDING'` | `PENDING\|ACCEPTED\|MODIFIED\|EXCLUDED` — persist는 미지정(기본값) (`02:82`) |
| `tags` | `TEXT[]` | 참조(예: 약관 제5조) — `[str(t) for t in ...]` (`02:83`, `agents.py:628`) |
| `impact_amount` | `BIGINT` | 영향 금액 — persist 미기입 (`02:84`) |
| `adjuster_opinion` | `TEXT` | 손해사정사 의견 — persist 미기입 (`02:85`) |
| `modified_reason` | `TEXT` | 수정 사유 (`02:86`) |
| `excluded_reason` | `TEXT` | 제외 사유 (`02:87`) |
| `created_at` | `TIMESTAMPTZ DEFAULT now()` | 생성 시각 (`02:88`) |

> 표 읽는 법: "persist가 …"라고 적힌 칸만 워커가 채우고, "미기입"으로 표시된 칸은 손해사정사가 나중에 손대는 몫이라 워커는 비워 둔다. `title`의 `str(...)[:200]`은 제목이 200자를 넘으면 잘라 넣는다는 뜻이다.

**인덱스**: `idx_report_issues_report ON report_issues (report_id)` (`02_app_tables.sql:91`).

**쓰기 방식**: persist가 **먼저 `DELETE FROM report_issues WHERE report_id = $1`로 기존 쟁점을 지운 뒤**, `state.issues`를 하나씩 돌며 INSERT한다(`agents.py:618-629`). 즉 단순 INSERT가 아니라 **지우고 다시 넣기(delete-then-insert) 재작성**이라, 다시 실행해도 쟁점이 중복되지 않는다. INSERT는 `(id, report_id, title, description, ai_status, tags)` 6컬럼만 채우고 나머지는 기본값/NULL로 둔다.

> 쉽게 말하면: 같은 리포트를 다시 돌리면 예전 쟁점을 통째로 지운 뒤 새로 써 넣는다. 그래서 같은 쟁점이 두 번 쌓이는 일이 없다.

### 3-7. `insurance_products` — 상품 마스터 (**참조만**, Python 미소유)

`report_worker`가 직접 SELECT/INSERT하지 않는다. `policy_chunks.product_id`·`user_claims.product_id`·`user_insurances.product_id`·`reports.product_id`가 논리적으로 이 테이블을 가리키지만, **FK 제약과 스키마는 메인 앱 마이그레이션이 관리**한다(`002_policy_chunks.sql:39-40`). `tempVectorDB/init`에도 이 테이블 정의는 없다.

> 쉽게 말하면: 여러 표가 "상품 마스터"를 가리키지만, 그 표 자체는 Spring 앱의 것이라 Python은 만들지도 직접 읽지도 않는다. 이름표(ID)만 공유할 뿐이다.

---

## 4. `load_context`가 조합하는 컨텍스트 — 흩어진 정보 한데 모으기

`load_context` 노드(`agents.py:66-119`)는 **테이블 4개(ocr_results, reports, user_claims, user_insurances)**를 한 커넥션(DB 연결 하나)에서 순서대로 조회해, 뒤에 오는 노드들이 쓸 재료(컨텍스트)를 조립한다. SQL 요지:

> 쉽게 말하면: 리포트를 쓰려면 OCR 텍스트, 사고 정보, 청구 내용, 가입보험을 각기 다른 표에서 모아 와야 한다. `load_context`가 이 네 곳을 차례로 훑어 하나의 작업 재료로 합쳐 준다.

1. **ocr_results** — `masked_text`, `entities` 로드:
   ```python
   ocr = await c.fetchrow(
       "SELECT masked_text, entities FROM ocr_results WHERE id = $1",
       uuid.UUID(state["ocr_result_id"]),
   )  # agents.py:69-72
   ```
2. **reports** — 사고 기본정보 + `claim_id` 로드:
   ```python
   rep = await c.fetchrow(
       "SELECT accident_type, treatment, offered_amount, question, claim_id "
       "FROM reports WHERE id = $1",
       uuid.UUID(state["report_id"]),
   )  # agents.py:73-77
   ```
3. **user_claims** — `rep["claim_id"]`가 있을 때만 조건부 조회(`agents.py:78-84`):
   ```python
   if rep and rep["claim_id"]:
       claim = await c.fetchrow(
           "SELECT diagnosis, accident_date, accident_type, offered_amount, description, "
           "hospitalization FROM user_claims WHERE id = $1", rep["claim_id"])
   ```
   (여기서 `rep["claim_id"]`는 DB에서 온 UUID라 별도 캐스팅 없이 그대로 파라미터로 넘긴다.)
4. **user_insurances** — `reports.user_id`를 서브쿼리로 뽑아 매칭되는 가입보험 1건(`agents.py:85-89`):
   ```python
   ins = await c.fetchrow(
       "SELECT insurer_name, product_name, coverages, enrolled_at FROM user_insurances "
       "WHERE user_id = (SELECT user_id FROM reports WHERE id = $1) LIMIT 1",
       uuid.UUID(state["report_id"]))
   ```

> 위 SQL 설명: `fetchrow`는 조건에 맞는 한 행을 가져온다. `$1`은 값을 안전하게 끼워 넣는 자리표시자다. 4번은 "이 리포트의 사용자"를 서브쿼리(괄호 안의 내부 조회)로 먼저 찾은 뒤, 그 사용자의 가입보험 1건을 가져온다.

**조립 결과** — 4개 조회를 병합해 아래 state 키를 만든다(`agents.py:100-119`):

| state 키 | 조립 규칙 (출처 테이블·컬럼) |
|----------|------------------------------|
| `case_info.accident_type` | `reports.accident_type` ?? `user_claims.accident_type` (`agents.py:101-102`) |
| `case_info.diagnosis` | `user_claims.diagnosis` ?? `reports.treatment` (`agents.py:103`) |
| `case_info.offered_amount` | `reports.offered_amount` (`agents.py:104`) |
| `case_info.question` | `reports.question` (`agents.py:105`) |
| `case_info.description` | `user_claims.description` (`agents.py:106`) |
| `case_info.insurer` | `user_insurances.insurer_name` (`agents.py:107`) |
| `case_info.product_name` | `user_insurances.product_name` (`agents.py:108`) |
| `case_info.enrolled_at` | `user_insurances.enrolled_at`(date\|None) — 표준표 버전 매칭용 (`agents.py:109-111`) |
| `masked_text` | `ocr_results.masked_text` (없으면 `""`) (`agents.py:115`) |
| `entities` | `ocr_results.entities` — dict면 그대로, 아니면 `json.loads` (`agents.py:94-98`·`116`) |
| `subscribed_coverages` | `list(user_insurances.coverages)` (없으면 `[]`) (`agents.py:117`) |

> 표 읽는 법: `??`는 "앞이 비어 있으면 뒤 값을 쓴다"는 뜻이다. 예를 들어 `case_info.diagnosis`는 청구서의 진단명을 먼저 쓰되, 없으면 리포트의 질병명(`treatment`)으로 대체한다.

`ocr`가 없으면 `errors`에 `"ocr_result_missing"`을 추가하고도 부분결과로 진행한다(`agents.py:92-93`, `safe_node` 방침 `:30-44`).

> 쉽게 말하면: 재료 하나가 빠져도 통째로 멈추지 않고, "이건 없었다"고 오류 목록에 적어 두고 가능한 부분까지는 계속 진행한다.

---

## 5. persist 매핑 (state → 테이블/컬럼) — 결과를 DB에 저장하기

`persist` 노드(`agents.py:570-630`)는 한 트랜잭션(`async with c.transaction()`, `agents.py:597`) 안에서 `report_drafts` UPSERT → `reports` UPDATE → `report_issues` DELETE+INSERT를 차례로 한다.

> 트랜잭션 = 여러 쓰기를 "전부 성공 아니면 전부 취소"로 묶는 것. 은행 이체처럼 중간에 실패하면 앞선 것도 없던 일로 되돌린다.
>
> 쉽게 말하면: AI가 만든 분석 결과(state)를 실제 DB 표들에 옮겨 담는 마지막 단계다. 세 표에 나눠 쓰되, 한 묶음으로 처리해서 중간에 문제가 생기면 셋 다 취소된다.

### 5-A. `basis` 조립 (`agents.py:573-578`)

```python
terms_cites = state.get("coverage_analysis", {}).get("citations", [])
case_refs = [c.get("source_ref") for c in state.get("legal_references", []) if c.get("source_ref")]
da_cites = (state.get("disability_analysis") or {}).get("citations", [])
basis = terms_cites + case_refs + da_cites
```
= 약관 인용 + 판례 source_ref + 장해분류표 인용을 이어붙인 것이다. `reports.basis_terms_precedents`와 `draft.basis_terms_precedents` 양쪽에 쓰인다.

> 쉽게 말하면: 리포트의 "근거 목록"을 만드는 부분이다. 약관에서 뽑은 인용, 판례 출처, 장해분류표 인용 세 뭉치를 하나로 이어 붙인다.

### 5-B. `report_drafts` UPSERT

```sql
-- agents.py:598-604
INSERT INTO report_drafts (report_id, draft, status)
   VALUES ($1, $2::jsonb, 'draft')
   ON CONFLICT (report_id) DO UPDATE SET draft = EXCLUDED.draft, status = 'draft'
```
`draft`는 `json.dumps(draft, ensure_ascii=False)`로 직렬화한다(`agents.py:603`).

> 위 SQL 설명: 초안을 새로 넣되, 같은 `report_id`가 이미 있으면(ON CONFLICT) 덮어쓴다. `$2::jsonb`는 문자열을 JSONB 타입으로 바꿔 넣는다는 뜻이고, `ensure_ascii=False`는 한글을 깨진 코드가 아니라 한글 그대로 저장한다는 뜻이다.

### 5-C. `draft` JSONB 내용물 (`agents.py:580-592`)

| draft JSONB 키 | state 소스 |
|----------------|-----------|
| `sections` | `state.sections` (8섹션 본문) (`:581`) |
| `estimated_range` | `state.estimated_range` `{min,max}` (`:582`) |
| `disclaimer` | `guardrail.DISCLAIMER` (상수 고지문) (`:583`) |
| `judge_failures` | `state.judge_failures` (출력 가드레일 인용검증 실패) (`:584`) |
| `issues` | `state.issues` (`:585`) |
| `applicable_guarantees` | `state.applicable_coverages` (`:586`) |
| `omitted_special_contract` | `state.missing_coverages` (`:587`) |
| `basis_terms_precedents` | `basis` (`:588`) |
| `legal_references` | `case_refs` (판례·분쟁조정 근거 별도 보존) (`:589`) |
| `disability` | `state.disability_analysis` (장해지급률·근거, P1) (`:590`) |
| `errors` | `state.errors` (`:591`) |

> 표 읽는 법: 이 JSON 한 칸(`draft`) 안에 리포트 산출물 전체가 통째로 보존된다. 본문 8섹션, 추정 금액 범위, 고지문, 인용검증 실패 기록, 쟁점, 근거 등이 모두 들어간다.

### 5-D. `reports` UPDATE 매핑

```sql
-- agents.py:605-617
UPDATE reports SET
   applicable_guarantees = $2, omitted_special_contract = $3,
   basis_terms_precedents = $4, claimed_min_amount = $5, claimed_max_amount = $6,
   status = 'AWAITING_ADOPTION', updated_at = now()
 WHERE id = $1
```

| 파라미터 | reports 컬럼 | state 소스 |
|----------|--------------|-----------|
| `$1` | `id` (WHERE) | `uuid.UUID(state.report_id)` |
| `$2` | `applicable_guarantees` | `state.applicable_coverages` |
| `$3` | `omitted_special_contract` | `state.missing_coverages` |
| `$4` | `basis_terms_precedents` | `basis` |
| `$5` | `claimed_min_amount` | `estimated_range.min` |
| `$6` | `claimed_max_amount` | `estimated_range.max` |
| (고정) | `status` | `'AWAITING_ADOPTION'` |
| (고정) | `updated_at` | `now()` |

> 표 읽는 법: `$1`~`$6`은 위 SQL의 자리표시자 순서다. 즉 이 표는 "SQL의 몇 번째 자리에 어떤 state 값이 들어가는지"를 풀어 놓은 것이다. `status`는 항상 `'AWAITING_ADOPTION'`(채택 대기)로 고정 설정된다.

### 5-E. `report_issues` DELETE + INSERT (`agents.py:618-629`)

`DELETE FROM report_issues WHERE report_id = $1`로 지운 뒤 `state.issues`를 하나씩 돌며:
```sql
INSERT INTO report_issues (id, report_id, title, description, ai_status, tags)
   VALUES ($1,$2,$3,$4,$5,$6)
```

| INSERT 파라미터 | report_issues 컬럼 | state 소스 |
|-----------------|--------------------|-----------|
| `$1` | `id` | `uuid.uuid4()` |
| `$2` | `report_id` | `rid` |
| `$3` | `title` | `str(it["title"])[:200]` |
| `$4` | `description` | `str(it["description"])` |
| `$5` | `ai_status` | `it["ai_status"]` ?? `'INFO'` |
| `$6` | `tags` | `[str(t) for t in it["tags"]]` |

> 표 읽는 법: 쟁점마다 새 UUID를 만들어(`uuid.uuid4()`) 한 줄씩 넣는다. `ai_status`가 비어 있으면 기본값 `'INFO'`를 쓴다.

### 5-F. persist_blocked (차단 경로, `agents.py:634-658`)

입력 가드레일(부적절·도메인 밖 입력을 막는 안전장치)이 요청을 차단하면 **초안을 만들지 않고 `reports.status`만 갱신**한다:
```sql
-- agents.py:654-657
UPDATE reports SET status = 'BLOCKED', updated_at = now() WHERE id = $1
```
차단 사유(`input_blocked:...`)는 초안이 아니라 로그로만 남긴다(`agents.py:645-649`).

> 쉽게 말하면: 애초에 처리하면 안 되는 요청이 들어오면, 리포트 초안을 만들지 않고 상태만 "차단됨(BLOCKED)"으로 바꿔 둔다.

> **정직한 계약 갭 표기**: `BLOCKED`는 이 노드가 새로 만든 status 값이다. 원래 계약 enum(`AWAITING_INSPECTION|AWAITING_ADOPTION|...`)에는 없으며 `TODO(spring-contract)`로 "reports.status enum에 'BLOCKED'를 추가해 정렬해야 한다"고 명시돼 있다(`agents.py:642-643`). 즉 Spring 쪽 enum과 아직 미정렬 상태다. (enum = 미리 정해 둔 값들의 목록.)

---

## 6. 관계 (텍스트 ERD) — 표들이 어떻게 이어지나

> ERD = 표들 사이의 연결 관계를 그린 지도. 아래 그림은 어느 표가 어느 표를 가리키고, report_worker가 어디를 읽고(◄──) 어디에 쓰는지(◄─ persist)를 보여준다.

```
                        [Kafka: report-job]
                               │  ReportJob{report_id, ocr_result_id, claim_id, ...}
                               ▼
   ┌──────────────┐   load_context가 읽는 4테이블   ┌──────────────────┐
   │ ocr_results  │◄── ocr_result_id ──────────────│    reports        │  (Spring 선생성)
   │  id (PK)     │                                 │  id (PK)          │
   │  masked_text │                                 │  user_id ─────────┼───┐
   │  entities    │                                 │  claim_id ───┐    │   │
   └──────────────┘                                 │  status      │    │   │
                                                    │  applicable_guarantees   │  ◄─ persist UPDATE
                                                    │  omitted_special_contract│  ◄─ persist UPDATE
                                                    │  basis_terms_precedents  │  ◄─ persist UPDATE
                                                    │  claimed_min/max_amount  │  ◄─ persist UPDATE
                                                    └──────┬────────────┼───┼──┘
                                                           │ id(PK)     │   │
                        ┌──────────────┐                   │            │   │
                        │ user_claims  │◄── claim_id ──────┘            │   │
                        │  id (PK)     │                                │   │
                        │  diagnosis   │                                │   │
                        │  accident_*  │                                │   │
                        └──────────────┘                                │   │
                                                                        │   │
                        ┌────────────────┐    user_id 서브쿼리 매칭      │   │
                        │ user_insurances│◄──────────────────────────────┘  │
                        │  id (PK)       │                                   │
                        │  insurer_name ─┼──┐  매칭 키                        │
                        │  product_name ─┼──┤                                │
                        │  coverages     │  │                                │
                        │  enrolled_at ──┼─┐│                                │
                        └────────────────┘ ││                                │
                                           ││                                │
   report_drafts (report_id PK) ◄── UPSERT ── persist ──────────────────────┤
   report_issues (report_id FK, idx) ◄── DELETE+INSERT ── persist ──────────┘
                                           ││
   ┌─────────────────────────────┐        ││
   │ policy_chunks (namespace=terms)       ││  coverage_analysis / policy_in_db 필터
   │  insurer      ◄──────────────┼────────┘│  (insurer_name/product_name == insurer/product_name)
   │  product_name ◄──────────────┼─────────┘
   │  embedding halfvec(1024)     │
   └─────────────────────────────┘
   ┌─────────────────────────────┐
   │ schedule_chunks(namespace=level)       enrolled_at → contract_date 로
   │  applies_from / applies_to  │◄─── disability_rag 폴백 버전필터
   │  embedding halfvec(1024)     │        (applies_from <= enrolled_at < applies_to)
   └─────────────────────────────┘
   ┌─────────────────────────────┐
   │ case_chunks (namespace=case) │◄─── case_search (메타필터 없이 검색)
   └─────────────────────────────┘
   search_terms (trigram 사전) ── 04 오타보정 단계에서 쿼리 정규화 (검색 전처리)
```

> Kafka는 우체통 같은 것이다. Spring이 "이 리포트 만들어 줘"라는 편지(report-job)를 우체통에 넣으면, report_worker가 그걸 꺼내 처리를 시작한다.

관계 요약:
- **ocr_results ← report_worker 입력**: `ReportJob.ocr_result_id` → `ocr_results.id` (`contracts.py:85`, `agents.py:71`).
- **reports.claim_id → user_claims.id**: 조건부 조회 (`agents.py:79-84`).
- **reports.user_id → user_insurances.user_id**: 서브쿼리 매칭 (`agents.py:87`).
- **reports.id ↔ report_drafts.report_id(PK) ↔ report_issues.report_id(논리 관계, idx)**: persist가 세 테이블을 report_id로 묶어 쓴다 (`agents.py:599`·`610`·`618-624`, 인덱스 `02:91`). **주의**: `tempVectorDB/init/02_app_tables.sql`에는 물리적 `FOREIGN KEY` 제약이 하나도 선언돼 있지 않다 — `reports.claim_id`·`report_drafts.report_id`(PK)·`report_issues.report_id`는 전부 `REFERENCES` 없는 순수 UUID 컬럼/PK이고, `report_issues.report_id`는 인덱스(`idx_report_issues_report`, `02:91`)만 있다. 관계는 애플리케이션이 지키는 논리적 관계이며, 실제 FK 강제는 Spring 소유 스키마 소관이다.

  > 쉽게 말하면: 표들이 서로 연결돼 있긴 하지만, DB가 강제로 "짝이 없으면 못 넣게" 막는 물리적 자물쇠(FK)는 이 로컬 복제본에 걸려 있지 않다. 연결을 지키는 책임은 코드(그리고 Spring 쪽 진짜 스키마)에 있다.

- **user_insurances.insurer_name/product_name ↔ policy_chunks.insurer/product_name**: `policy_in_db` 분기(`agents.py:180-188`)와 `coverage_analysis`/`disability_rag` RAG 필터(`agents.py:227-229`·`391-395`)의 매칭 키.
- **user_insurances.enrolled_at ↔ schedule_chunks.(applies_from, applies_to)**: `case_info.enrolled_at`이 `contract_date`로 흘러가 표준 장해분류표 버전을 고른다 (`agents.py:401`·`404`, `search.py:95-118`).

---

## 7. 멱등성·타입 규약 — 안전하게 다시 실행하기 & 데이터 형식

### 식별자·시각 타입
- **UUID**(전 세계에서 겹치지 않게 만든 긴 식별 번호): 앱 테이블 PK(`id`)와 참조키가 전부 UUID다(`02_app_tables.sql` 전반). 계약상 "식별자는 UUIDv4 문자열"(`contracts.py:8`). state의 문자열 id는 SQL 직전에 `uuid.UUID(state["report_id"])`로 파싱한다(`agents.py:76`·`88`·`593`·`651`; 참고로 `:72`는 `state["ocr_result_id"]` 파싱). `report_issues.id`는 워커가 `uuid.uuid4()`로 새로 만든다(`agents.py:623`).
- **TIMESTAMPTZ**(시간대 정보를 포함한 시각): 모든 앱 테이블·청크 테이블의 `created_at`/`ingested_at`/`updated_at`이 `TIMESTAMPTZ DEFAULT now()`다(예: `02:11`·`26`·`42`, `002:19`). 계약은 "시각은 ISO-8601(UTC)"(`contracts.py:8`).
- **DATE**(날짜만, 시각 없음): 사고일/시행일/버전 경계가 date다 — `user_claims.accident_date`, `policy_chunks.effective_date`, `schedule_chunks.applies_from`/`applies_to`(`02:20`, `002:25`, `005:34-35`).

### 멱등 쓰기 패턴 (재실행 안전)
`report-job`은 `report_id` 멱등 처리 계약(`contracts.py:80`)이라 같은 report_id로 다시 소비될 수 있다. persist는 세 가지 방식으로 재실행 안전을 확보한다:
- **report_drafts**: `INSERT ... ON CONFLICT (report_id) DO UPDATE`(`agents.py:601`) — PK 충돌 시 덮어쓰기라 다시 실행해도 1행만 유지된다.
- **report_issues**: `DELETE ... WHERE report_id = $1` 후 재삽입(`agents.py:618-629`) — 다시 실행해도 쟁점이 쌓여 중복되지 않는다.
- **reports**: `UPDATE ... WHERE id = $1`(`agents.py:606-617`) — 행을 만들지 않고 결과 컬럼만 덮어쓴다(멱등).
- 세 쓰기는 한 트랜잭션으로 묶여 원자적으로(전부 성공 아니면 전부 취소) 커밋된다(`agents.py:597`). persist_blocked의 단일 UPDATE는 트랜잭션 래핑 없이 단독으로 실행된다(`agents.py:653-657`).
- 마이그레이션 SQL 자체도 전부 `IF NOT EXISTS`로 멱등하다(`migrations/README.md:62`, 각 파일).

> 쉽게 말하면: 같은 작업 메시지가 실수로 두 번 도착해도, 결과가 두 배로 늘어나거나 꼬이지 않도록 세 표 모두 "덮어쓰기" 또는 "지우고 다시 넣기" 방식으로 처리한다.

### halfvec (벡터 타입)
- 세 벡터 테이블의 `embedding`은 전부 `halfvec(1024)`다 — float16(반정밀), 1024차원(`002:14`, `003:23`, `005:19`). HNSW는 halfvec 전용 연산자 클래스 `halfvec_cosine_ops`(m=16, ef_construction=64)를 쓴다. 이는 저장 공간을 절반으로 줄이면서 코사인 거리 기반 근사 검색(ANN)을 지원하려는 것이다.
- 벡터공간 통일: 세 테이블이 모두 같은 임베딩 모델·차원이라야 교차 검색이 성립한다(`003:8`, `005:8`). 차원은 `EMBEDDING_DIM`으로 1024에 고정돼 있다(`contracts.py:98`).
- 커넥션 코덱: `db.py`가 커넥션을 초기화할 때 `register_vector(conn)`으로 pgvector 타입을 등록해 `list[float]` ↔ 벡터를 주고받는다(`db.py:39-42`, `:18`). 검색기는 캐스트 없이 halfvec/vector 코덱으로 파라미터를 넘긴다(`search.py:226`).

> 쉽게 말하면: 좌표 하나를 절반 크기로 저장(float16)해 공간을 아끼면서도, "가까운 것 찾기"에는 충분한 정밀도를 유지하는 방식이다. 대신 세 표가 같은 규격(1024차원, 같은 모델)이라야 서로 비교할 수 있다.

### JSONB 용도
- `ocr_results.entities`(추출 엔티티, `02:10`), `user_claims.hospitalization`(입원 이력 배열, `02:21`), `report_drafts.draft`(리포트 초안 전체 구조, `02:70`)가 JSONB다.
- 읽을 때 dict가 아니면 `json.loads`로 방어적으로 변환한다(`agents.py:96-98`). 쓸 때는 `json.dumps(..., ensure_ascii=False)`로 문자열화한 뒤 `$2::jsonb`로 캐스트한다(`agents.py:603`·`600`). `draft` JSONB는 sections·estimated_range·disclaimer·judge_failures·issues·disability·errors 등 리포트 산출물 전체를 **영구 보존**하는 그릇이다(§5-C).

> 쉽게 말하면: JSONB는 "형태가 자유로운 데이터"를 한 칸에 통째로 담는 타입이다. 입원 이력처럼 항목 수가 들쭉날쭉한 정보나, 리포트 초안 전체 구조를 한꺼번에 넣기에 편하다.

### tsvector / trigram (키워드·오타보정 규약)
- 키워드 검색은 앱단(kiwipiepy)에서 토큰화 → `content_tokens`에 저장 → `to_tsvector('simple', coalesce(content_tokens,''))` **함수식 GIN**으로 매칭한다. 인덱스 표현식과 검색기 쿼리식이 정확히 일치해야 한다(`migrations/README.md:64-71`, 각 `*_fts` 인덱스).
- `search_terms.term`은 `gin_trgm_ops` trigram GIN으로 색인해 `similarity(input, term) > 0.4` 오타 보정에 쓴다(`004:14-15`, `:2-4`).

> 쉽게 말하면: 키워드 검색용 색인(tsvector)을 만들 때 쓴 계산식과, 검색할 때 쓰는 계산식이 글자 하나까지 똑같아야 색인이 작동한다. trigram(글자 세 개씩 쪼개기)은 오타를 견디는 용도다.

### 컬럼 배열 타입 (TEXT[])
- `reports.applicable_guarantees`/`omitted_special_contract`/`basis_terms_precedents`, `user_insurances.coverages`, `case_chunks.tags`, `report_issues.tags`가 PostgreSQL `TEXT[]`(문자열 여러 개를 한 칸에 배열로 담는 타입)다. persist는 Python `list[str]`을 그대로 바인딩하며, tags는 `[str(t) for t in ...]`로 문자열 정규화 후 삽입한다(`agents.py:612-614`·`628`).

> 쉽게 말하면: 담보 목록·근거 목록·태그처럼 "여러 개"가 자연스러운 값은 한 칸에 목록째 담는다. Python의 리스트를 그대로 넣을 수 있다.

---

### 부록: 관련 소스 경로

- 마이그레이션: `C:\Users\wkdrn\project\Ai\migrations\001_extensions.sql` ~ `005_schedule_chunks.sql`, `migrations\README.md`
- 로컬 정본/복제본: `C:\Users\wkdrn\project\Ai\tempVectorDB\init\01_schema.sql`·`02_app_tables.sql`·`03_case_chunks.sql`·`04_schedule_chunks.sql`
- 노드 SQL: `C:\Users\wkdrn\project\Ai\src\report_worker\nodes\agents.py`
- 풀 lifecycle: `C:\Users\wkdrn\project\Ai\src\core\db.py`
- 계약 모델: `C:\Users\wkdrn\project\Ai\src\core\contracts.py`
- 상태 정의: `C:\Users\wkdrn\project\Ai\src\report_worker\state.py`
- namespace↔테이블 매핑: `C:\Users\wkdrn\project\Ai\src\rag\search.py` (`_NS_TABLE:40-44`, `_NS_COLS:50-54`, `_META_FILTER_NS:69`, `_VERSION_FILTER_NS:92`)
