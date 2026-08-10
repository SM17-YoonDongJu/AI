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

-- ai 스키마 선행 보장 — 이 디렉터리의 마이그레이션이 001부터 스키마를 명시(ai.)하므로,
-- 신규 환경(로컬 PG 등)에서 스키마 자체가 없으면 바로 실패한다. **자기 스키마(ai)만** 만든다 —
-- corpus는 corpus/000_extensions.sql이 corpus_owner로 따로 만든다(이 디렉터리는 ai_owner
-- 전용이라 corpus에 대한 권한 자체가 없다 — 아래 참고).
--
-- 주의: `CREATE SCHEMA IF NOT EXISTS`는 스키마가 이미 있어도 CREATE 권한을 먼저 검사한다
-- (IF NOT EXISTS가 권한 체크까지 건너뛰어주진 않는다) — dev/RDS에서 ai_owner는 DATABASE에
-- CREATE 권한이 없어 스키마가 이미 있는데도 permission denied로 워커 기동이 통째로 막혔다
-- (실측, #48 배포 직후). pg_namespace로 존재를 먼저 확인해 이미 있으면 CREATE 문 자체를
-- 실행하지 않는다 — 이미 스키마 분리가 끝난 환경(dev/RDS/prod)은 여기서 끝나고, 스키마가
-- 정말 없는 신규 환경(로컬 PG 등, 보통 superuser로 접속)만 CREATE를 탄다.
--
-- 더 근본적인 배경: migrations/ 전체를 워커 하나가 통째로 실행하던 구조에서, ai_owner가
-- corpus 스키마 오브젝트를(001~002가 아니라 예전 008~011을), corpus_owner가 ai 스키마
-- 오브젝트를 서로 건드리다 CREATE TABLE/INDEX 권한 에러로 두 워커가 동시에 다운됐다
-- (실측). 그래서 migrations/를 ai_owner 전용(이 디렉터리)·corpus_owner 전용(../corpus)으로
-- 분리했다 — ocr_worker/corpus_worker의 _MIGRATIONS_DIR도 각자 이 디렉터리만 가리키도록
-- 바뀐다(src/ocr_worker/__main__.py, src/corpus_worker/__main__.py).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'ai') THEN
        CREATE SCHEMA ai;
    END IF;
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'ai 스키마 생성 건너뜀(권한 부족 — 이미 존재하거나 별도 관리): %', SQLERRM;
END
$$;

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
