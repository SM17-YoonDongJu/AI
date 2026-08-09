-- case_chunks: 판례·금감원 분쟁조정례 청크 (RAG namespace "case")
-- policy_chunks(01_schema.sql) 미러 구조 + 판례 전용 메타.
-- 임베딩은 약관과 동일 모델·1024d(qwen3-embedding) 이어야 벡터공간이 일치.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS case_chunks (
    chunk_id        TEXT PRIMARY KEY,
    content         TEXT        NOT NULL,   -- 임베딩 원문
    content_tokens  TEXT,                   -- Kiwi 형태소 (공백 구분) → tsvector 전문검색
    embedding       halfvec(1024),          -- qwen3-embedding 1024d (약관과 동일)
    token_count     INT,
    chunk_type      TEXT        NOT NULL,   -- holding(판시사항)|summary(판결요지)|reasoning(이유)|order(주문)|decision(분쟁조정 결정)|fact(사실관계)|general
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

    -- 보험 연관 메타 (필터·약관 교차참조)
    insurer         TEXT,                   -- 관련 보험사 (nullable)
    product_name    TEXT,                   -- 관련 상품 (nullable)
    accident_type   TEXT,                   -- ERD accident_type (nullable)
    tags            TEXT[],                 -- 쟁점 태그 (후유장해, 면책, 고지의무 ...)

    -- 구조 메타
    section         TEXT,                   -- 편/장/항 경로 또는 경계 라벨
    chunk_index     INT                     -- 문서 내 순서 (복원 시 ORDER BY)
);

-- 벡터 검색 (ANN, cosine) — HNSW, halfvec 전용 연산자 클래스
CREATE INDEX IF NOT EXISTS idx_case_hnsw
    ON case_chunks USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 키워드 검색 — tsvector 전문검색 (GIN)
CREATE INDEX IF NOT EXISTS idx_case_fts
    ON case_chunks
    USING gin (to_tsvector('simple', coalesce(content_tokens, '')));

-- 메타 필터
CREATE INDEX IF NOT EXISTS idx_case_meta
    ON case_chunks (source_type, accident_type, decision_date);

-- 쟁점 태그 검색
CREATE INDEX IF NOT EXISTS idx_case_tags
    ON case_chunks USING gin (tags);

-- doc_hash 중복 방지 조회
CREATE INDEX IF NOT EXISTS idx_case_doc_hash
    ON case_chunks (doc_hash);
