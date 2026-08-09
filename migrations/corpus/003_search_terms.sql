-- 010_search_terms.sql
-- search_terms (RAG 인프라) — 보험 도메인 정규 용어 사전.
-- 입력 쿼리를 trigram 유사도(similarity(input, term) > 0.4)로 조회해 오타·약어·구어체를
-- 정규 용어로 치환한다(04 trigram 오타 보정 단계).
-- 선행: 007_extensions.sql (pg_trgm 확장).

CREATE TABLE IF NOT EXISTS corpus.search_terms (
    term       text PRIMARY KEY,   -- 정규 용어
    namespace  text,               -- terms | case (적용 대상 힌트, 파생값과 동일 체계)
    source     text                -- 용어 출처(약관/사례 등)
);

-- 오타 보정: trigram GIN (similarity / % 연산자).
CREATE INDEX IF NOT EXISTS idx_search_terms_term_trgm
    ON corpus.search_terms USING gin (term gin_trgm_ops);

-- report_worker·chatbot이 ai_owner로 오타보정 시 이 테이블을 읽어야 한다(스키마 분리,
-- deploy/schema_split.sql 참고). role이 없는 로컬 PG에서는 조용히 건너뛴다.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ai_owner') THEN
        EXECUTE 'GRANT SELECT ON corpus.search_terms TO ai_owner';
    END IF;
END
$$;
