-- 003_case_chunks.sql
-- CASE_CHUNKS (판례·금감원 분쟁조정례 청크, namespace=case).
-- namespace는 물리 컬럼이 아니라 소스 테이블로 부여하는 파생값(case_chunks -> case).
-- 선행: 001_extensions.sql (vector·pg_trgm 확장).
--
-- 정본(canonical): tempVectorDB/init/03_case_chunks.sql 과 동일 스키마.
--   적재기 tempVectorDB/load_cases.py 와 src/rag/search.py(_NS_COLS["case"])가 이 스키마를 쓴다.
--   embedding 은 policy_chunks 와 동일 halfvec(1024) — 벡터공간 일치.
--
-- ⚠ 마이그레이션 이력 주의:
--   구버전(case_number/block_type enum/court_level 등) 스키마로 이미 case_chunks 테이블을
--   만든 DB에서는 CREATE TABLE IF NOT EXISTS 가 스킵되어 낡은 컬럼이 남는다 →
--   case 검색·적재가 UndefinedColumnError 로 전면 실패한다.
--   그런 DB는 아래를 먼저 실행해 DROP 후 재적용해야 한다:
--       DROP TABLE IF EXISTS case_chunks;
--       DROP TYPE  IF EXISTS case_outcome;
--       DROP TYPE  IF EXISTS case_block_type;

CREATE TABLE IF NOT EXISTS case_chunks (
    chunk_id        TEXT PRIMARY KEY,
    content         TEXT        NOT NULL,   -- 임베딩 원문
    content_tokens  TEXT,                   -- Kiwi 형태소 (공백 구분) → tsvector 전문검색
    embedding       halfvec(1024),          -- qwen3-embedding 1024d (약관과 동일)
    token_count     INT,
    chunk_type      TEXT        NOT NULL,   -- holding|summary|reasoning|order|decision|fact|general
    doc_hash        TEXT        NOT NULL,   -- 원문 sha256, 중복 ingest 방지
    ingested_at     TIMESTAMPTZ DEFAULT now(),

    -- 출처·사건 메타
    source_type     TEXT        NOT NULL,   -- court_precedent(판례)|fss_mediation(금감원 분쟁조정례)
    institution     TEXT,                   -- 대법원|서울중앙지법|금융감독원 ...
    case_no         TEXT,                   -- 사건번호 "2021다1234" / 조정번호 "제2022-15호"
    case_title      TEXT,                   -- 사건명
    holding         TEXT,                   -- 판시사항 요약(조항 복원용 헤더)
    decision_date   DATE,                   -- 선고일 / 결정일
    source_url      TEXT,

    -- 보험 연관 메타 (필터·약관 교차참조). insurer/product_name 은 nullable 참고 메타.
    insurer         TEXT,                   -- 관련 보험사 (nullable)
    product_name    TEXT,                   -- 관련 상품 (nullable)
    accident_type   TEXT,                   -- ERD accident_type (nullable)
    tags            TEXT[],                 -- 쟁점 태그 (후유장해, 면책, 고지의무 ...)

    -- 구조 메타
    section         TEXT,                   -- 편/장/항 경로 또는 경계 라벨
    chunk_index     INT                     -- 문서 내 순서 (복원 시 ORDER BY)
);

-- 벡터 검색 (ANN, cosine) — HNSW, halfvec 전용 연산자 클래스.
CREATE INDEX IF NOT EXISTS idx_case_hnsw
    ON case_chunks USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 키워드 검색 — tsvector 전문검색 (GIN, 함수식). 앱단 토큰 → content_tokens → 'simple' tsvector.
CREATE INDEX IF NOT EXISTS idx_case_fts
    ON case_chunks
    USING gin (to_tsvector('simple', coalesce(content_tokens, '')));

-- 메타 필터 (출처·사고유형·결정일).
CREATE INDEX IF NOT EXISTS idx_case_meta
    ON case_chunks (source_type, accident_type, decision_date);

-- 쟁점 태그 검색.
CREATE INDEX IF NOT EXISTS idx_case_tags
    ON case_chunks USING gin (tags);

-- doc_hash 중복 방지 조회.
CREATE INDEX IF NOT EXISTS idx_case_doc_hash
    ON case_chunks (doc_hash);
