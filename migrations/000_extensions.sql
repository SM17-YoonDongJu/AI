-- 000_extensions.sql — 공용 PG 확장(로컬 PG·RDS 동일 적용).
--
-- core.db 풀은 커넥션마다 pgvector 타입을 등록하므로(RAG 벡터 검색), `vector` 확장이
-- DB에 먼저 존재해야 어떤 워커든 풀 생성이 성공한다. OCR 워커도 이 공용 풀을 쓰므로
-- 여기서 선행 보장한다. 반복 적용 안전(IF NOT EXISTS).
CREATE EXTENSION IF NOT EXISTS vector;

-- pg_trgm — trigram 유사도/오타보정 인덱스(gin_trgm_ops)용. corpus_file.product_name의
-- 수요 매칭(상품명 근사 검색)과 RAG trigram 오타보정이 이 확장에 의존한다. `vector`와
-- 같은 이유로 인덱스(002_corpus_catalog.sql)보다 먼저 존재해야 CREATE INDEX가 성공한다.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ai/corpus 스키마 선행 보장 — 이 폴더의 마이그레이션이 001부터 스키마를 명시(ai./corpus.)
-- 하므로, 신규 환경(로컬 PG 등)에서 스키마 자체가 없으면 바로 실패한다. 이미 스키마 분리가
-- 끝난 dev/RDS/prod에서는 IF NOT EXISTS라 조용히 스킵된다.
CREATE SCHEMA IF NOT EXISTS ai;
CREATE SCHEMA IF NOT EXISTS corpus;

-- pgaudit — 쿼리 감사(민감 컬럼 READ/WRITE 로그, CloudWatch Logs 연동). RDS 파라미터
-- 그룹에서 shared_preload_libraries에 pgaudit을 넣고 재부팅한 뒤에만 성공한다 — 그
-- 전에 이 마이그레이션이 먼저 돌면(예: 로컬 PG, 또는 재부팅 전 RDS) 여기서 실패해
-- 워커 기동 전체가 막힌다. 로컬 PG는 이 확장이 아예 불필요하므로, 있으면 켜고
-- 없으면(재부팅 전이거나 로컬) 조용히 건너뛴다 — 다른 확장과 달리 워커 기능이 이
-- 확장에 의존하지 않기 때문에 이렇게 관대하게 처리해도 안전하다.
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS pgaudit;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'pgaudit 확장 생성 건너뜀(shared_preload_libraries 미적용 또는 권한 부족): %', SQLERRM;
END
$$;
