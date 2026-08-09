-- 007_extensions.sql
-- Hybrid RAG에 필요한 PostgreSQL 확장 설치.
-- 번호는 dev의 000~006(OCR·corpus 계열)과의 충돌을 피해 007부터 시작.
-- dev 000_extensions.sql 과 중복 적용돼도 안전(IF NOT EXISTS).
--   vector  : pgvector — embedding(vector(1024)) 컬럼·HNSW 벡터 검색
--   pg_trgm : trigram  — search_terms.term 오타 보정(similarity / GIN)
-- 멱등(IF NOT EXISTS). superuser 또는 CREATE 권한 필요.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
