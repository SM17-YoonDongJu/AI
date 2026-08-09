-- 000_extensions.sql (corpus) — Hybrid RAG에 필요한 PostgreSQL 확장 + corpus 스키마 선행 보장.
--
-- vector/pg_trgm 확장 생성은 ../ai/000_extensions.sql과 중복이지만 둘 다 IF NOT EXISTS라
-- 어느 순서로 실행돼도 안전하다(DB 전역 오브젝트라 스키마와 무관).
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- corpus 스키마 선행 보장 — **자기 스키마(corpus)만** 만든다(ai/000_extensions.sql과 동일 이유·
-- 동일 패턴 — pg_namespace로 존재를 먼저 확인해 CREATE 문 자체를 실행하지 않으면 권한 에러를
-- 피할 수 있다. IF NOT EXISTS는 권한 체크를 건너뛰어주지 않는다).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'corpus') THEN
        CREATE SCHEMA corpus;
    END IF;
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'corpus 스키마 생성 건너뜀(권한 부족 — 이미 존재하거나 별도 관리): %', SQLERRM;
END
$$;
