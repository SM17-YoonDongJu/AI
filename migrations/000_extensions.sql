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
