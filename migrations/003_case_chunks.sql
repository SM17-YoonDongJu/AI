-- 003_case_chunks.sql
-- CASE_CHUNKS (분쟁조정, namespace=case) — 분쟁조정사례 청크.
-- namespace는 소스 테이블로 부여하는 파생값(CASE_CHUNKS -> case).
-- 선행: 001_extensions.sql (vector 확장).

-- enum 타입(멱등 생성).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'case_outcome') THEN
        CREATE TYPE case_outcome AS ENUM ('plaintiff_win', 'defendant_win', 'partial', 'settled');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'case_block_type') THEN
        CREATE TYPE case_block_type AS ENUM (
            'holding', 'issue', 'facts', 'arguments', 'reasoning', 'conclusion'
        );
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS case_chunks (
    chunk_id        text PRIMARY KEY,
    source_id       text,
    content         text NOT NULL,
    content_tokens  text,                       -- 앱단 형태소 토큰화 결과(공백 구분). tsvector 입력.
    embedding       vector(1024),               -- 1024d 고정
    token_count     integer,
    doc_hash        text,
    chunk_index     integer,
    case_type       text,
    court_level     text,
    case_number     text,
    decision_date   date,
    outcome         case_outcome,
    insurance_type  text,
    block_type      case_block_type,
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    content_tsv     tsvector GENERATED ALWAYS AS (to_tsvector('simple', coalesce(content_tokens, ''))) STORED
);

-- 벡터 유사도 검색: HNSW + cosine.
CREATE INDEX IF NOT EXISTS idx_case_chunks_embedding_hnsw
    ON case_chunks USING hnsw (embedding vector_cosine_ops);

-- 키워드 검색: content_tsv GIN.
CREATE INDEX IF NOT EXISTS idx_case_chunks_content_tsv
    ON case_chunks USING gin (content_tsv);

-- 메타데이터 필터(쿼리 라우터의 보험유형/출처 필터).
CREATE INDEX IF NOT EXISTS idx_case_chunks_insurance_type
    ON case_chunks (insurance_type);
CREATE INDEX IF NOT EXISTS idx_case_chunks_source_id
    ON case_chunks (source_id);
