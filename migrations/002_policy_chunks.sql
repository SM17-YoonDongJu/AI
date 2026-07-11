-- 002_policy_chunks.sql
-- POLICY_CHUNKS (약관, namespace=terms) — 신체 관련 보험 약관 청크.
-- namespace는 물리 컬럼이 아니라 소스 테이블로 부여하는 파생값(policy_chunks -> terms).
-- 선행: 001_extensions.sql (vector·pg_trgm 확장).
--
-- 정본(canonical): tempVectorDB/init/01_schema.sql 의 policy_chunks 와 동일 스키마.
--   embedding 은 halfvec(1024)(qwen3:embedding / BGE-M3 폴백, float16), HNSW 는 halfvec_cosine_ops.
--   키워드 검색은 content_tokens 함수식 GIN 인덱스(src/rag/search.py 의 쿼리식과 일치).

CREATE TABLE IF NOT EXISTS policy_chunks (
    chunk_id        TEXT PRIMARY KEY,
    content         TEXT        NOT NULL,   -- 임베딩 원문
    content_tokens  TEXT,                   -- Kiwi 형태소 결과 (공백 구분) → tsvector 전문검색
    embedding       halfvec(1024),          -- qwen3:embedding 1024d / BGE-M3 1024d (float16)
    token_count     INT,
    chunk_type      TEXT        NOT NULL,   -- coverage|exclusion|definition|special_clause|duty|claim|termination|schedule|general
    doc_hash        TEXT        NOT NULL,   -- PDF sha256, 중복 ingest 방지
    page_number     INT,
    ingested_at     TIMESTAMPTZ DEFAULT now(),

    -- 보험사·상품 메타 (검색 필터)
    insurer         TEXT        NOT NULL,
    product_name    TEXT        NOT NULL,
    product_code    TEXT,
    effective_date  DATE,

    -- 약관 구조 메타
    article_number  TEXT,                   -- "제12조"
    article_title   TEXT,                   -- "보험금을 지급하지 않는 사유"
    generation      TEXT,                   -- 세대 (예: "4세대")
    section         TEXT,                   -- 경계 라벨 또는 편/장 경로
    chunk_index     INT,                    -- 문서 전체 순서 (조항 복원 시 ORDER BY)

    -- 표 row 청크 전용 (텍스트 청크는 NULL)
    table_id        UUID,                   -- S3 key → policy-tables/{table_id}.md (FK 없음)
    row_start       SMALLINT,
    row_end         SMALLINT,

    -- 상품 FK (nullable). REFERENCES insurance_products(id)는 메인 앱 마이그레이션에서 관리.
    product_id      UUID
);

-- 벡터 검색 (ANN, cosine) — HNSW, halfvec 전용 연산자 클래스.
CREATE INDEX IF NOT EXISTS idx_policy_hnsw
    ON policy_chunks USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 키워드 검색 — tsvector 전문검색 (GIN, 함수식). 앱단 토큰 → content_tokens → 'simple' tsvector.
CREATE INDEX IF NOT EXISTS idx_policy_fts
    ON policy_chunks
    USING gin (to_tsvector('simple', coalesce(content_tokens, '')));

-- 메타 필터 (보험사·청크타입·시행일).
CREATE INDEX IF NOT EXISTS idx_policy_meta
    ON policy_chunks (insurer, chunk_type, effective_date);

-- doc_hash 중복 방지 조회.
CREATE INDEX IF NOT EXISTS idx_policy_doc_hash
    ON policy_chunks (doc_hash);

-- 표 row 청크 조회.
CREATE INDEX IF NOT EXISTS idx_policy_table_id
    ON policy_chunks (table_id);
