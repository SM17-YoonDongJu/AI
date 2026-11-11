-- schema_split.prod.sql — schema_split.sql의 prod(brbs_prod)판. 롤은 *_owner_prod를 쓰고, 서비스 간
-- 권한은 dev(brbs_dev)에 실제로 걸린 것과 같게 맞춘다.
--
-- =============================================================================================
-- 실행 순서 (prod DB 작업 전체)
-- =============================================================================================
--   1) backend 첫 기동      — Flyway가 core 테이블을 만든다(V40이 public → core로 옮김, 소유자 app_owner_prod)
--   2) AI 마이그레이션       — OCR 또는 report가 ai_owner_prod로 한 번 떠서 ai 테이블을 만든다
--   3) 이 스크립트           — corpus_owner_prod 롤·corpus 스키마를 만들고 서비스 간 권한을 건다
--   4) corpus 적재           — corpus_owner_prod로 migrations/corpus(DDL) → 데이터(아래 "corpus 적재" 참고)
--   5) 이 스크립트 다시 실행 — 4)에서 생긴 corpus 테이블에 ai_owner_prod SELECT를 건다
-- 아직 없는 테이블은 그 GRANT만 건너뛰고 NOTICE를 남긴다. 전부 멱등이라 몇 번 돌려도 결과가 같다.
--
-- =============================================================================================
-- schema_split.sql을 이름만 바꿔 돌리지 않는 이유
-- =============================================================================================
-- schema_split.sql은 public 스키마 하나를 함께 쓰던 dev를 나중에 core·ai·corpus로 쪼갠 부트스트랩이다
-- (롤 생성·테이블 이동·소유자 변경·권한). brbs_prod는 처음부터 나뉜 상태로 만들어졌다(2026-11-11 조회).
--   - app_owner_prod·ai_owner_prod 롤이 있다(비밀번호는 Secrets Manager prod/rds-admin).
--   - brbs_prod 소유자는 app_owner_prod이고 PUBLIC의 CONNECT가 없다 — 같은 인스턴스의 dev 롤은 못 붙는다.
--   - core는 app_owner_prod, ai는 ai_owner_prod 소유 스키마다.
-- 백엔드 Flyway(V40)와 AI 마이그레이션이 처음부터 제 스키마에 제 소유로 테이블을 만들므로, 원본의 테이블
-- 이동·소유자 변경 목록(V32 기준이라 이미 낡았다)은 필요 없다. 롤별 search_path도 dev와 같게
-- corpus_owner_prod에만 건다(backend는 JDBC currentSchema, AI 워커는 DATABASE_URL options로 정한다).
--
-- =============================================================================================
-- 거는 권한 — dev에 걸린 서비스 간 권한(2026-11-11 조회)을 롤 이름만 바꾼 것
-- =============================================================================================
--   ai_owner_prod     → core  : USAGE, reports SELECT + 7개 컬럼 UPDATE, user_claims·user_insurances
--                               SELECT, report_issues INSERT·DELETE + report_id SELECT   (report_worker)
--   ai_owner_prod     → corpus: USAGE, policy_chunks·case_chunks·schedule_chunks·search_terms SELECT (RAG)
--   app_owner_prod    → ai    : USAGE, report_drafts·ocr_job_failures·report_job_failures SELECT,
--                               ocr_results 7개 컬럼 SELECT + REFERENCES                 (backend 읽기 전용)
--   corpus_owner_prod → ai    : USAGE, corpus_source·corpus_file·corpus_file_part 읽기·쓰기 (corpus_worker)
-- prod에만 있는 권한(dev에 없음):
--   ai_owner_prod     → core  : encryption_keys 4개 컬럼 SELECT — report의 PII 복호화(core.crypto)가 prod에서
--                               KMS로 풀 wrapped DEK를 읽는다. dev·local은 env 키(PII_ENC_KEY)를 써서 이 테이블을
--                               읽지 않는다. 2026-11-11 prod DB에 직접 GRANT됐던 것을 여기로 옮겼다(원본 유지).
-- ai → app_owner_prod 권한은 AI 마이그레이션(007·008·011·012)도 기동마다 다시 건다(접속 롤에서 받는 롤을
-- 끌어낸다). 여기서도 거는 건 실행 시점과 상관없이 결과를 같게 하려는 것이다.
--
-- =============================================================================================
-- corpus 적재 (4단계)
-- =============================================================================================
-- 약관 코퍼스는 사용자 데이터가 아니라 dev와 prod가 같은 참조 데이터다. GPU로 다시 청킹하지 않고 dev에서
-- 검증된 것을 옮긴다: corpus_owner_prod로 corpus_worker 마이그레이션(migrations/corpus, DDL)을 적용한 뒤
-- 데이터만 복사한다. dev corpus는 policy_chunks만 약 227만 행·13GB다. RDS는 20GB 중 4GB만 남아 있어
-- 2026-11-11에 50GB로 늘리고 스토리지 자동 확장(최대 100GB)을 켰다 — 복사본이 들어갈 자리가 있다.
--
-- =============================================================================================
-- 실행 방법
-- =============================================================================================
-- 마스터 계정(prod/rds-admin)으로 brbs_prod에 접속한다. RDS는 사설망이라 RDS SG가 열린 brbs-ocr-prod를
-- 거쳐 포트포워딩한다. corpus_owner_prod 비밀번호는 prod/rds-admin의 CORPUS_OWNER_PROD_PASSWORD를
-- 환경변수로 넘긴다(명령줄·파일에 남기지 않는다. RDS는 log_statement=none이라 CREATE ROLE도 로그에 안 남는다).
--   aws ssm start-session --target <brbs-ocr-prod 인스턴스 ID> \
--     --document-name AWS-StartPortForwardingSessionToRemoteHost \
--     --parameters 'host=["<RDS 엔드포인트>"],portNumber=["5432"],localPortNumber=["15432"]'
--   export PGPASSWORD=<RDS_DEV_MASTER_PASSWORD> CORPUS_OWNER_PROD_PASSWORD=<CORPUS_OWNER_PROD_PASSWORD>
--   psql "host=localhost port=15432 dbname=brbs_prod user=postgres sslmode=require" \
--     -v ON_ERROR_STOP=1 -f deploy/schema_split.prod.sql
-- psql 15 이상(\getenv)이 필요하다. RDS 마스터는 진짜 superuser가 아니라 롤 소유 객체에 GRANT하려면 그
-- 롤의 권한을 물려받아야 한다 — 1단계가 이를 확인하고, 새로 만드는 corpus_owner_prod 멤버십만 스스로 준다.

\getenv corpus_owner_prod_password CORPUS_OWNER_PROD_PASSWORD
\if :{?corpus_owner_prod_password}
\else
    -- \quit은 성공(0)으로 끝나 실행자가 놓칠 수 있다 — 오류로 멈춘다(ON_ERROR_STOP).
    DO $$ BEGIN RAISE EXCEPTION 'CORPUS_OWNER_PROD_PASSWORD 환경변수가 없다 — prod/rds-admin 값을 넘겨 다시 실행'; END $$;
\endif

BEGIN;

-- 0. 엉뚱한 DB·롤에 돌리지 않게 막는다.
DO $$
BEGIN
    IF current_database() <> 'brbs_prod' THEN
        RAISE EXCEPTION 'brbs_prod 전용 스크립트다 — 지금 접속한 DB: %', current_database();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_owner_prod')
       OR NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ai_owner_prod') THEN
        RAISE EXCEPTION 'app_owner_prod·ai_owner_prod 롤이 없다';
    END IF;
    IF to_regnamespace('core') IS NULL OR to_regnamespace('ai') IS NULL THEN
        RAISE EXCEPTION 'core·ai 스키마가 없다';
    END IF;
END
$$;

-- 1. corpus_owner_prod 롤(없을 때만). 비밀번호는 psql 변수로만 들어간다.
SELECT format('CREATE ROLE corpus_owner_prod LOGIN PASSWORD %L', :'corpus_owner_prod_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'corpus_owner_prod') \gexec

-- RDS 마스터는 진짜 superuser가 아니라, 롤 소유 객체에 GRANT하려면 그 롤의 권한을 물려받아야(INHERIT)
-- 한다. app_owner_prod·ai_owner_prod는 이미 그렇게 묶여 있지만 마스터에게 그 롤의 ADMIN이 없어 다시
-- GRANT하면 실패한다 — 확인만 한다. corpus_owner_prod는 CREATE SCHEMA ... AUTHORIZATION에 SET ROLE까지
-- 필요하고, 방금 만든 사람(마스터)이 ADMIN을 가지므로 모자라면 스스로 준다.
DO $$
BEGIN
    IF NOT pg_has_role(current_user, 'app_owner_prod', 'USAGE')
       OR NOT pg_has_role(current_user, 'ai_owner_prod', 'USAGE') THEN
        RAISE EXCEPTION '%이(가) app_owner_prod·ai_owner_prod 권한을 물려받지 못한다 — 멤버십(INHERIT) 확인',
            current_user;
    END IF;
    IF NOT (pg_has_role(current_user, 'corpus_owner_prod', 'USAGE')
            AND pg_has_role(current_user, 'corpus_owner_prod', 'SET')) THEN
        EXECUTE format('GRANT corpus_owner_prod TO %I WITH INHERIT TRUE, SET TRUE', current_user);
    END IF;
END
$$;

-- Policy-Chunker·corpus_worker는 corpus 테이블을 스키마 없이 부른다 — dev corpus_owner와 같은 기본값.
ALTER ROLE corpus_owner_prod SET search_path = corpus, ai, public;

-- 2. 접속 경계. 같은 인스턴스에 dev 롤(app_owner·ai_owner·corpus_owner)이 있고, 새 DB는 기본으로
--    PUBLIC에 CONNECT를 준다. 지금 막혀 있어도 다시 걸어 둔다.
REVOKE CONNECT, TEMPORARY ON DATABASE brbs_prod FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE brbs_prod TO ai_owner_prod, corpus_owner_prod;

-- 3. 스키마. corpus는 여기서 처음 만든다.
CREATE SCHEMA IF NOT EXISTS corpus AUTHORIZATION corpus_owner_prod;
GRANT USAGE ON SCHEMA core   TO ai_owner_prod;
GRANT USAGE ON SCHEMA corpus TO ai_owner_prod;
GRANT USAGE ON SCHEMA ai     TO app_owner_prod, corpus_owner_prod;

-- 4. 테이블 권한 — 테이블이 있을 때만. 없는 테이블에 GRANT하면 트랜잭션 전체가 롤백되므로 확인한다.
DO $$
DECLARE
    g record;
BEGIN
    FOR g IN
        SELECT * FROM (VALUES
            -- ai_owner_prod → core (report_worker가 읽고, 정해진 컬럼만 쓴다)
            ('core.reports',
             'GRANT SELECT ON core.reports TO ai_owner_prod'),
            ('core.reports',
             'GRANT UPDATE (applicable_guarantees, basis_terms_precedents, claimed_max_amount,
                            claimed_min_amount, omitted_special_contract, status, updated_at)
              ON core.reports TO ai_owner_prod'),
            ('core.user_claims',
             'GRANT SELECT ON core.user_claims TO ai_owner_prod'),
            ('core.user_insurances',
             'GRANT SELECT ON core.user_insurances TO ai_owner_prod'),
            ('core.report_issues',
             'GRANT INSERT, DELETE ON core.report_issues TO ai_owner_prod'),
            -- DELETE ... WHERE report_id = $1이 조건 컬럼을 읽으므로 그 컬럼만 SELECT를 준다.
            ('core.report_issues',
             'GRANT SELECT (report_id) ON core.report_issues TO ai_owner_prod'),
            -- 활성 PII 키 조회(SELECT key_version, wrapped_dek ... WHERE purpose = 'PII' AND is_active)에
            -- 쓰는 컬럼만. kms_key_id·created_at·retired_at·id는 열지 않는다. prod 전용(위 머리말).
            ('core.encryption_keys',
             'GRANT SELECT (purpose, is_active, key_version, wrapped_dek)
              ON core.encryption_keys TO ai_owner_prod'),
            -- ai_owner_prod → corpus (RAG 검색, 읽기만) — 4단계 적재 뒤 다시 실행하면 걸린다
            ('corpus.policy_chunks',   'GRANT SELECT ON corpus.policy_chunks TO ai_owner_prod'),
            ('corpus.case_chunks',     'GRANT SELECT ON corpus.case_chunks TO ai_owner_prod'),
            ('corpus.schedule_chunks', 'GRANT SELECT ON corpus.schedule_chunks TO ai_owner_prod'),
            ('corpus.search_terms',    'GRANT SELECT ON corpus.search_terms TO ai_owner_prod'),
            -- app_owner_prod → ai (backend 읽기 전용, 마이그레이션 self-grant와 같은 내용)
            ('ai.report_drafts',
             'GRANT SELECT ON ai.report_drafts TO app_owner_prod'),
            ('ai.ocr_job_failures',
             'GRANT SELECT ON ai.ocr_job_failures TO app_owner_prod'),
            ('ai.report_job_failures',
             'GRANT SELECT ON ai.report_job_failures TO app_owner_prod'),
            ('ai.ocr_results',
             'GRANT SELECT (id, report_id, claim_id, attachment_id, doc_type, doc_index, ocr_quality)
              ON ai.ocr_results TO app_owner_prod'),
            ('ai.ocr_results',
             'GRANT REFERENCES ON ai.ocr_results TO app_owner_prod'),
            -- corpus_owner_prod → ai (corpus_worker의 Notion 카탈로그)
            ('ai.corpus_source',
             'GRANT SELECT, INSERT, UPDATE, DELETE ON ai.corpus_source TO corpus_owner_prod'),
            ('ai.corpus_file',
             'GRANT SELECT, INSERT, UPDATE, DELETE ON ai.corpus_file TO corpus_owner_prod'),
            ('ai.corpus_file_part',
             'GRANT SELECT, INSERT, UPDATE, DELETE ON ai.corpus_file_part TO corpus_owner_prod')
        ) AS t(tbl, stmt)
    LOOP
        IF to_regclass(g.tbl) IS NULL THEN
            RAISE NOTICE '건너뜀 — % 없음(해당 단계 뒤 다시 실행)', g.tbl;
        ELSE
            EXECUTE g.stmt;
        END IF;
    END LOOP;
END
$$;

COMMIT;

-- 5. 결과 — 모든 테이블이 있으면 테이블 권한 15행(dev와 같음), 컬럼 권한 4행(dev 3행 + prod 전용
--    encryption_keys 1행)이 나와야 한다.
SELECT g.table_schema, g.table_name, g.grantee,
       string_agg(g.privilege_type, ',' ORDER BY g.privilege_type) AS privileges
FROM information_schema.role_table_grants g
JOIN pg_tables t ON t.schemaname = g.table_schema AND t.tablename = g.table_name
WHERE g.grantee IN ('ai_owner_prod', 'app_owner_prod', 'corpus_owner_prod') AND g.grantee <> t.tableowner
GROUP BY 1, 2, 3
ORDER BY 3, 1, 2;

SELECT c.table_schema, c.table_name, c.grantee, c.privilege_type,
       string_agg(c.column_name, ',' ORDER BY c.column_name) AS columns
FROM information_schema.column_privileges c
JOIN pg_tables t ON t.schemaname = c.table_schema AND t.tablename = c.table_name
WHERE c.grantee IN ('ai_owner_prod', 'app_owner_prod', 'corpus_owner_prod') AND c.grantee <> t.tableowner
  AND NOT EXISTS (
      SELECT 1 FROM information_schema.role_table_grants g
      WHERE g.table_schema = c.table_schema AND g.table_name = c.table_name
        AND g.grantee = c.grantee AND g.privilege_type = c.privilege_type)
GROUP BY 1, 2, 3, 4
ORDER BY 3, 1, 2;
