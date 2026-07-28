-- 004_search_terms.sql
-- search_terms (RAG 인프라) — 보험 도메인 정규 용어 사전.
-- 입력 쿼리를 trigram 유사도(similarity(input, term) > 0.4)로 조회해 오타·약어·구어체를
-- 정규 용어로 치환한다(04 trigram 오타 보정 단계).
-- 선행: 001_extensions.sql (pg_trgm 확장).

CREATE TABLE IF NOT EXISTS search_terms (
    term       text PRIMARY KEY,   -- 정규 용어
    namespace  text,               -- terms | case (적용 대상 힌트, 파생값과 동일 체계)
    source     text                -- 용어 출처(약관/사례 등)
);

-- 오타 보정: trigram GIN (similarity / % 연산자).
CREATE INDEX IF NOT EXISTS idx_search_terms_term_trgm
    ON search_terms USING gin (term gin_trgm_ops);
