-- schedule_chunks: 후유장해분류표(금감원 보험업감독업무시행세칙 별표) 청크 (RAG namespace "level")
-- policy_chunks(01_schema.sql)·case_chunks(03_case_chunks.sql) 미러 구조 + 장해분류·개정버전 메타.
-- 임베딩은 약관·판례와 동일 모델·1024d(qwen3-embedding) 이어야 벡터공간이 일치.
--
-- 개정 버전별 적용: 후유장해분류표는 2018.4 대개정 등 시행세칙 개정마다 판이 갈린다.
-- 계약 체결일이 속하는 버전을 골라야 하므로 (version_label, applies_from, applies_to)로
-- 유효기간을 표현한다. applies_to IS NULL 이면 현행판이다.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS schedule_chunks (
    chunk_id        TEXT PRIMARY KEY,
    content         TEXT        NOT NULL,   -- 임베딩 원문
    content_tokens  TEXT,                   -- Kiwi 형태소 (공백 구분) → tsvector 전문검색
    embedding       halfvec(1024),          -- qwen3-embedding 1024d (약관·판례와 동일)
    token_count     INT,
    chunk_type      TEXT        NOT NULL,   -- schedule(장해분류표) 고정 (약관 chunk_type 체계와 정합)
    doc_hash        TEXT        NOT NULL,   -- 원문 sha256, 중복 ingest 방지
    ingested_at     TIMESTAMPTZ DEFAULT now(),

    -- 장해분류 메타
    body_part       TEXT        NOT NULL,   -- 신체부위 분류: 눈|귀|코|씹어먹거나말하기|외모|척추|
                                            --   체간골|팔|다리|손가락|발가락|흉복부장기및비뇨생식기|
                                            --   신경계정신행동 등 원문 분류
    disability_grade TEXT,                  -- 장해등급/지급률 항목 라벨 (nullable)
    rate            NUMERIC,                -- 항목 단일 지급률(%) — 표 행이 단일 지급률일 때 (nullable)

    -- 개정 버전 메타 (계약 체결일 기준 버전 매칭)
    version_label   TEXT        NOT NULL,   -- 개정판 라벨 (예: "2018.4 개정", "2005 표준약관")
    applies_from    DATE        NOT NULL,   -- 이 버전 적용 시작일 (계약 체결일 >= applies_from)
    applies_to      DATE,                   -- 이 버전 적용 종료일 (exclusive). NULL 이면 현행판
    source_url      TEXT,

    -- 구조 메타
    section         TEXT,                   -- 표 내 절/항목 경로 또는 경계 라벨
    chunk_index     INT                     -- 문서 내 순서 (복원 시 ORDER BY)
);

-- 벡터 검색 (ANN, cosine) — HNSW, halfvec 전용 연산자 클래스.
CREATE INDEX IF NOT EXISTS idx_schedule_hnsw
    ON schedule_chunks USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 키워드 검색 — tsvector 전문검색 (GIN, 함수식). 앱단 토큰 → content_tokens → 'simple' tsvector.
CREATE INDEX IF NOT EXISTS idx_schedule_fts
    ON schedule_chunks
    USING gin (to_tsvector('simple', coalesce(content_tokens, '')));

-- 버전(적용기간) 필터 — 계약 체결일이 속하는 개정판 조회.
CREATE INDEX IF NOT EXISTS idx_schedule_version
    ON schedule_chunks (applies_from, applies_to);

-- 신체부위 필터.
CREATE INDEX IF NOT EXISTS idx_schedule_body_part
    ON schedule_chunks (body_part);

-- doc_hash 중복 방지 조회.
CREATE INDEX IF NOT EXISTS idx_schedule_doc_hash
    ON schedule_chunks (doc_hash);
