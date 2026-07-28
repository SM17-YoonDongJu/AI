-- 002_policy_chunks.sql
-- POLICY_CHUNKS (약관, namespace=terms) — 신체 관련 보험 약관 청크.
-- namespace는 물리 컬럼이 아니라 소스 테이블로 부여하는 파생값(POLICY_CHUNKS -> terms).
-- 선행: 001_extensions.sql (vector 확장).

CREATE TABLE IF NOT EXISTS policy_chunks (
    chunk_id        text PRIMARY KEY,
    product_id      uuid,
    content         text NOT NULL,
    content_tokens  text,                       -- 앱단 형태소 토큰화 결과(공백 구분). tsvector 입력.
    embedding       vector(1024),               -- qwen3:embedding / BGE-M3 폴백 (1024d 고정)
    token_count     integer,
    chunk_type      text,
    doc_hash        text,
    page_number     integer,
    insurer         text,
    product_name    text,
    product_code    text,
    effective_date  date,
    article_number  text,
    article_title   text,
    generation      text,
    section         text,
    chunk_index     integer,
    table_id        uuid,
    row_start       smallint,
    row_end         smallint,
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    -- 키워드 검색용 tsvector (STORED generated). 04 전략: 앱단 토큰 -> content_tokens -> simple tsvector.
    -- to_tsvector(regconfig, text)는 IMMUTABLE이라 생성 컬럼에 사용 가능('simple'로 명시 고정).
    content_tsv     tsvector GENERATED ALWAYS AS (to_tsvector('simple', coalesce(content_tokens, ''))) STORED
);

-- 벡터 유사도 검색: HNSW + cosine.
CREATE INDEX IF NOT EXISTS idx_policy_chunks_embedding_hnsw
    ON policy_chunks USING hnsw (embedding vector_cosine_ops);

-- 키워드 검색: content_tsv GIN.
CREATE INDEX IF NOT EXISTS idx_policy_chunks_content_tsv
    ON policy_chunks USING gin (content_tsv);
