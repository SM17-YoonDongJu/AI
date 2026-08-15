-- schema_split.sql — 공유 RDS(brbs-rds-dev/prod, 단일 database) 안에서 소유 경계를 스키마로 분리한다.
--
-- =============================================================================================
-- 배경 (실제 코드·마이그레이션 확인 결과)
-- =============================================================================================
-- 지금은 backend(Spring/Flyway)·이 엔진(ocr_worker/corpus_worker, migrations/*.sql)·Policy-Chunker
-- (별도 ingest 스크립트)가 전부 같은 database의 `public` 스키마 하나를 공유한다. 소유 경계는 문서·
-- 커밋 관례로만 존재했지 DB가 강제하지 않았다. 아래 매핑은 backend Flyway V1~V30, 이 레포
-- migrations/000~006, ocr_worker/corpus_worker의 repository.py 실제 SQL, Policy-Chunker
-- db/storage.py·db/search.py·db/search_term.py를 모두 읽고 확인한 것이다(추측 없음).
--
--   core   — backend(Spring) 소유. Flyway가 관리.
--   ai     — 이 엔진(ocr_worker·corpus_worker, 향후 report_worker/rag/chatbot) 소유. migrations/*.sql이 관리.
--   corpus — Policy-Chunker 소유. db/schema.sql이 관리.
--
-- 크로스 스키마 접근은 실제 코드/계약 문서에서 확인된 것만 GRANT한다(최소 권한):
--   - ai_owner → core.user_insurances, core.user_claims : SELECT
--       (contracts.md 2026-08-04 + report_worker#11 브랜치 src/report_worker/nodes/agents.py 실제
--        쿼리로 재확인됨 — 아래 항목 참고)
--   - ai_owner → core.reports : SELECT + UPDATE(컬럼 한정)
--       (report_worker#11 미머지 브랜치 확인 — src/report_worker/nodes/agents.py:74-88이
--        `SELECT accident_type, treatment, offered_amount, question, claim_id FROM reports`로 읽고,
--        같은 파일 908행대가
--        `UPDATE reports SET applicable_guarantees=..., omitted_special_contract=...,
--         basis_terms_precedents=..., claimed_min_amount=..., claimed_max_amount=...,
--         status='AWAITING_ADOPTION', updated_at=now() WHERE id=$1`로 쓴다(가드레일 차단 시엔
--        status='BLOCKED'로만 쓰는 경로도 있음, 961행). 이 7개 컬럼 외에는 report_worker가 절대
--        건드리면 안 되므로(user_id·adjuster_id·case_no 등) 컬럼 한정 GRANT로 막아둔다.
--        처음엔 이 발견 전이라 이 GRANT가 빠져 있었다 — report_worker(#11)를 직접 안 열어보고
--        contracts.md만 보고 설계했던 게 원인. dev에 이미 schema_split.sql을 돌렸다면 이 파일을
--        다시 실행해도 안전하다(전부 IF NOT EXISTS/멱등 GRANT).
--   - ai_owner → core.report_issues : INSERT, DELETE
--       (같은 브랜치 924~934행 — 리포트 재생성 시 `DELETE FROM report_issues WHERE report_id=$1` 후
--        `INSERT INTO report_issues (id, report_id, title, description, ai_status, tags) ...`로
--        전량 교체한다. SELECT는 이 경로에서 안 쓰길래 안 줬다 — 필요해지면 그때 추가)
--   - ai_owner → corpus.policy_chunks, corpus.search_terms : SELECT
--       (Policy-Chunker db/search.py가 report_worker/chatbot 계약과 동일 시그니처의 RAG 검색 레퍼런스
--        구현이고, 우리 쪽 src/rag가 이걸 그대로 이식할 예정 — RRF 검색은 SELECT만 필요)
--   - app_owner → ai.report_drafts : SELECT
--       (contracts.md §4: "손해사정사 UI·후속 단계가 읽는다")
-- 그 반대(app_owner → ai.ocr_results 등)는 주지 않는다 — UserInsurance.java/ReportAttachment.java
-- 엔티티를 직접 확인한 결과 backend는 ocr_result_id를 UUID 컬럼으로만 저장·전달할 뿐 ocr_results의
-- 내용(masked_text/entities)을 읽는 코드가 없다("이 읽기 모델에 필요 없는 컬럼... 매핑을 생략했다" —
-- UserInsurance.java 클래스 주석). 필요해지면 그때 GRANT를 추가한다(YAGNI).
--
-- =============================================================================================
-- 알아둘 것 — report_worker(#11) 브랜치는 미머지, RAG 코퍼스 테이블도 별도 정리가 더 필요하다
-- =============================================================================================
-- 이 브랜치(origin/11-feature-langgraph-멀티에이전트-구현)는 아직 dev/main 어디에도 머지되지 않았다.
-- 위 core.reports/report_issues GRANT는 "머지되면 이렇게 쓸 것"이라는 근거로 지금 미리 열어두는
-- 것이다 — 지금 당장 core에 위험이 생기는 건 아니다(권한만 있고 쓰는 코드는 아직 배포 전).
--
-- 이 브랜치는 또한 `migrations/`를 001_extensions.sql~005_schedule_chunks.sql로 **완전히 다른
-- 번호 체계**로 갖고 있고(우리 main의 000~006과 파일명이 겹치면서 내용은 다름 — 머지 시 수동
-- 재정렬 필요), `case_chunks`·`schedule_chunks`라는 새 테이블을 그 마이그레이션으로 직접 만든다.
-- 이 두 테이블은 Policy-Chunker db/schema.sql엔 없다 — 즉 corpus 스키마(Policy-Chunker 소유)가
-- 아니라 이 레포 자체가 DDL을 만들어가는 테이블이라, ai 스키마 소속이 더 맞다. 게다가 branch #11의
-- policy_chunks/search_terms 마이그레이션(002·004)은 Policy-Chunker db/schema.sql과 컬럼이 다르다
-- (예: search_terms — Policy-Chunker는 term 1컬럼, 이 브랜치는 term/namespace/source 3컬럼).
-- 즉 policy_chunks/search_terms도 ocr_results와 같은 "두 곳에서 따로 진화" 문제가 잠재해 있다.
--
-- 이 스크립트는 이 문제를 풀지 않는다 — case_chunks/schedule_chunks는 지금 prod/dev 어디에도
-- 존재하지 않으므로(브랜치가 안 머지됐으니) 옮길 대상 자체가 없다. #11이 머지될 때 반드시
-- 다시 짚어야 할 목록: (1) migrations 번호 재정렬, (2) policy_chunks/search_terms 스키마를
-- Policy-Chunker와 브랜치#11 중 어느 쪽으로 통일할지, (3) case_chunks/schedule_chunks를 ai로
-- 넣을지 corpus로 넣을지 팀 결정.
--
-- =============================================================================================
-- 알아둘 것 — ocr_results 소유권 불일치를 여기서 정리한다
-- =============================================================================================
-- backend V1__init_schema.sql 주석은 ocr_results를 "경계 테이블(공유 DB, 백엔드가 DDL 소유·FastAPI가
-- rows 기록)"이라고 선언한다. 그런데 실제로는 이 레포의 migrations/003~006이 doc_type CHECK 확장,
-- ocr_quality 컬럼, 원본 삭제 outbox 컬럼 4종을 이미 이 테이블에 추가해왔다 — backend의 V1 이후로는
-- 이 테이블에 대한 Flyway 마이그레이션이 없다(V2~V30 어디에도 ocr_results가 없음). 즉 "백엔드가 DDL
-- 소유"라는 선언과 실제 운영은 이미 어긋나 있다.
--
-- 이 스크립트는 실제로 스키마를 계속 바꿔온 쪽(이 엔진)에 DDL 소유권을 맞춘다 — ocr_results를 `ai`
-- 스키마로 옮긴다. backend의 기존 FK(user_insurances.ocr_result_id, report_attachments.ocr_result_id
-- → ocr_results(id))는 Postgres가 스키마 간 FK를 그대로 지원하므로 이동 후에도 깨지지 않는다(별도
-- 조치 불필요 — ALTER TABLE ... SET SCHEMA는 OID 기준이라 참조 무결성이 유지된다). backend 팀에는
-- "앞으로 ocr_results 구조 변경은 이 레포 migrations/*.sql로만 한다"는 합의만 구두/PR로 받으면 된다.
--
-- =============================================================================================
-- 실행 전 필수 확인 (이 스크립트가 확인 못 하는 것들)
-- =============================================================================================
-- 1. 실제 database 이름 — deploy/.env.ocr.example·.env.corpus.example은 `/postgres`(database
--    이름이 postgres)를 가리킨다. backend application.yml 기본값은 DB_NAME:brbs_dev이지만 이건
--    로컬 기본값일 뿐, 실제 dev/prod .env는 git 미커밋이라 이 레포에서 값을 확인할 수 없다.
--    단일 공유 DB라는 전제는 사용자가 확인해줬다 — 다만 실제 database 이름이 이 스크립트의
--    가정과 다르면 아래 `current_database()` 참조 부분들이 잘못된 DB에 접속된 채로 실행될 수
--    있으니, 접속 후 `SELECT current_database();`로 한 번 더 확인하고 실행할 것.
-- 2. flyway_schema_history 위치 — 이 스크립트가 core 테이블들의 search_path 앞자리를 core로
--    바꾸므로, backend가 `spring.flyway.schemas: public`(또는 default-schema: public)을 먼저
--    배포해 두지 않으면 다음 Flyway 마이그레이션 실행 시 history 테이블을 core에 새로 만들려
--    시도해 마이그레이션 이력을 통째로 잃을 위험이 있다. **이 스크립트보다 먼저, 별도 배포로
--    반영해야 한다.**
-- 3. 이 스크립트는 CREATE ROLE·ALTER TABLE ... SET SCHEMA·OWNER TO 권한이 필요하다 — RDS
--    마스터 유저(또는 rds_superuser 멤버)로 실행한다. 각 서비스가 평소 쓰는 앱 유저로는 못 돌린다.
-- 4. **dev(brbs-rds-dev)에서 먼저 실행하고 backend·ocr·corpus 세 서비스가 정상 기동하는지
--    확인한 뒤에만 prod에 적용한다.** DeletionProtection이 걸려 있어도 이 스크립트 자체는
--    DROP을 하지 않지만, 커넥션 문자열이 바뀌는 배포이므로 크래시 루프 위험은 그대로 있다.
-- 5. 비밀번호(__REPLACE_ME__)는 실제 값으로 채우지 말고, 실행 직전에 psql -v로 주입하거나
--    Secrets Manager에서 생성한 값을 별도 채널로 붙여넣을 것 — 이 파일은 git에 커밋된다.
--
-- =============================================================================================
-- 실행 순서 (요약)
-- =============================================================================================
--   1) backend: spring.flyway.schemas=public 배포 (history 테이블 위치 고정)
--   2) dev RDS에서 이 스크립트 실행 → 세 서비스 DB_USER/DATABASE_URL을 신규 role로 교체해 재배포
--      → 정상 기동 확인
--   3) prod RDS에서 이 스크립트 실행 → 세 서비스 순차 재배포(app_owner 먼저, 그다음 ai_owner·
--      corpus_owner — backend가 먼저 살아 있어야 Kafka outbox·OCR 트리거가 끊기지 않는다)
--
-- 로컬 개발(docker-compose)은 서비스별로 이미 별도 DB(ai_engine 등)를 쓰므로 영향 없음 — 이 스크립트는
-- 공유 RDS(dev/prod) 전용이다.

BEGIN;

-- ---------------------------------------------------------------------------------------------
-- 1. 역할(Role) 생성 — 서비스당 하나, LOGIN. 이미 있으면 건너뜀(재실행 안전).
-- ---------------------------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_owner') THEN
        CREATE ROLE app_owner LOGIN PASSWORD '__REPLACE_ME_APP_OWNER__';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ai_owner') THEN
        CREATE ROLE ai_owner LOGIN PASSWORD '__REPLACE_ME_AI_OWNER__';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'corpus_owner') THEN
        CREATE ROLE corpus_owner LOGIN PASSWORD '__REPLACE_ME_CORPUS_OWNER__';
    END IF;
END
$$;

-- 셋 다 같은 database에 접속해야 한다(현재 접속된 database 기준 — 실행 전 확인 사항 #1 참고).
DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO app_owner, ai_owner, corpus_owner', current_database());
END
$$;

-- RDS 마스터 유저(예: postgres)는 진짜 superuser가 아니라 rds_superuser 멤버라, 방금 만든
-- role의 멤버가 자동으로 되지 않는다 — 이 상태로 `CREATE SCHEMA ... AUTHORIZATION app_owner`나
-- `ALTER TABLE ... OWNER TO app_owner`를 실행하면 "must be able to SET ROLE" 에러가 난다.
-- 지금 접속 중인 사용자(현재 세션의 current_user — 보통 마스터 유저)를 세 role의 멤버로
-- 만들어 SET ROLE이 가능하게 한다. WITH ADMIN OPTION은 안 준다(이 세션이 회원 관리까지 할
-- 필요는 없음 — 단지 SET ROLE만 되면 됨).
DO $$
BEGIN
    EXECUTE format('GRANT app_owner, ai_owner, corpus_owner TO %I', current_user);
END
$$;

-- 확장(vector·pg_trgm·pgaudit·pgcrypto)이 public에 설치돼 있으므로 셋 다 public을 계속 봐야 한다
-- (스키마를 나눠도 public 자체를 비우는 게 아니다 — 확장 함수·연산자 클래스 접근용으로 유지).
GRANT USAGE ON SCHEMA public TO app_owner, ai_owner, corpus_owner;

-- ---------------------------------------------------------------------------------------------
-- 2. 스키마 생성
-- ---------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS core   AUTHORIZATION app_owner;
CREATE SCHEMA IF NOT EXISTS ai     AUTHORIZATION ai_owner;
CREATE SCHEMA IF NOT EXISTS corpus AUTHORIZATION corpus_owner;
-- 새로 만든 스키마는 owner 외에는 기본적으로 USAGE/CREATE가 없다(public과 달리 PUBLIC에 자동
-- 부여되지 않음) — 아래 4번에서 필요한 것만 명시적으로 연다.

-- flyway_schema_history는 backend의 spring.flyway.schemas=public 설정 때문에 core로 옮기지
-- 않고 의도적으로 public에 남긴다(§실행 전 확인 사항 #2). 하지만 이 테이블은 예전에 postgres
-- 마스터 유저가 만들어서 소유자가 postgres다 — app_owner로 갈아탄 backend가 접속하면
-- "permission denied for table flyway_schema_history"로 기동 자체가 실패한다(실측 확인,
-- dev에서 발견). 스키마는 그대로 두고 소유자만 app_owner로 옮겨서 해결한다.
ALTER TABLE IF EXISTS public.flyway_schema_history OWNER TO app_owner;

-- ---------------------------------------------------------------------------------------------
-- 3. 기존 테이블을 스키마로 이동 + 소유자 변경
--    ALTER TABLE ... SET SCHEMA는 OID 기준 이동이라 기존 FK·인덱스·GRANT(rds_pgaudit 포함)가
--    그대로 유지된다 — 재생성 불필요.
-- ---------------------------------------------------------------------------------------------

-- 3-1. core (backend 소유, V1~V32 기준 실제 테이블 26개 — V31에서 chatroom_reports 추가 확인,
-- 처음 확인 때(V30까지)는 없던 테이블이라 한 번 빠뜨렸었다. backend는 계속 개발 중이라 이 스크립트를
-- 다시 돌리기 전엔 항상 최신 마이그레이션과 이 목록을 대조할 것.)
ALTER TABLE IF EXISTS public.users                        SET SCHEMA core;
ALTER TABLE IF EXISTS public.social_accounts               SET SCHEMA core;
ALTER TABLE IF EXISTS public.device_tokens                 SET SCHEMA core;
ALTER TABLE IF EXISTS public.notification_settings         SET SCHEMA core;
ALTER TABLE IF EXISTS public.notifications                  SET SCHEMA core;
ALTER TABLE IF EXISTS public.adjuster_profiles              SET SCHEMA core;
ALTER TABLE IF EXISTS public.adjuster_applications          SET SCHEMA core;
ALTER TABLE IF EXISTS public.adjuster_application_documents SET SCHEMA core;
ALTER TABLE IF EXISTS public.insurers                       SET SCHEMA core;
ALTER TABLE IF EXISTS public.insurance_products              SET SCHEMA core;
ALTER TABLE IF EXISTS public.user_claims                     SET SCHEMA core;
ALTER TABLE IF EXISTS public.user_insurances                 SET SCHEMA core;
ALTER TABLE IF EXISTS public.reports                         SET SCHEMA core;
ALTER TABLE IF EXISTS public.report_issues                   SET SCHEMA core;
ALTER TABLE IF EXISTS public.report_reviews                  SET SCHEMA core;
ALTER TABLE IF EXISTS public.report_issues_reviews           SET SCHEMA core;
ALTER TABLE IF EXISTS public.report_attachments              SET SCHEMA core;
ALTER TABLE IF EXISTS public.report_holds                    SET SCHEMA core;
ALTER TABLE IF EXISTS public.adjuster_reviews                SET SCHEMA core;
ALTER TABLE IF EXISTS public.chatroom                        SET SCHEMA core;
ALTER TABLE IF EXISTS public.chatroom_messages                SET SCHEMA core;
ALTER TABLE IF EXISTS public.subscriptions                    SET SCHEMA core;
ALTER TABLE IF EXISTS public.outbox_events                    SET SCHEMA core;
ALTER TABLE IF EXISTS public.kafka_outbox_events               SET SCHEMA core;
ALTER TABLE IF EXISTS public.report_case_sequences             SET SCHEMA core;
ALTER TABLE IF EXISTS public.chatroom_reports                  SET SCHEMA core;

DO $$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'users','social_accounts','device_tokens','notification_settings','notifications',
        'adjuster_profiles','adjuster_applications','adjuster_application_documents','insurers',
        'insurance_products','user_claims','user_insurances','reports','report_issues','report_reviews',
        'report_issues_reviews','report_attachments','report_holds','adjuster_reviews','chatroom',
        'chatroom_messages','subscriptions','outbox_events','kafka_outbox_events','report_case_sequences',
        'chatroom_reports'
    ]
    LOOP
        IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'core' AND tablename = t) THEN
            EXECUTE format('ALTER TABLE core.%I OWNER TO app_owner', t);
        END IF;
    END LOOP;
END
$$;

-- 3-2. ai (이 엔진 소유 — ocr_results는 "알아둘 것" 절 참고: DDL 소유권을 여기로 확정)
ALTER TABLE IF EXISTS public.ocr_results     SET SCHEMA ai;
ALTER TABLE IF EXISTS public.corpus_source   SET SCHEMA ai;
ALTER TABLE IF EXISTS public.corpus_file     SET SCHEMA ai;
ALTER TABLE IF EXISTS public.corpus_file_part SET SCHEMA ai;
-- report_drafts는 이동 대상이 아니다 — migrations/007_report_drafts.sql이 처음부터 ai 스키마에
-- 직접 만든다(그쪽이 이 스크립트보다 먼저 배포돼 있으면 이미 ai.report_drafts로 존재한다).

DO $$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['ocr_results','corpus_source','corpus_file','corpus_file_part','report_drafts']
    LOOP
        IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'ai' AND tablename = t) THEN
            EXECUTE format('ALTER TABLE ai.%I OWNER TO ai_owner', t);
        END IF;
    END LOOP;
END
$$;

-- 3-3. corpus (Policy-Chunker 소유)
ALTER TABLE IF EXISTS public.policy_chunks SET SCHEMA corpus;
ALTER TABLE IF EXISTS public.search_terms  SET SCHEMA corpus;

DO $$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['policy_chunks','search_terms']
    LOOP
        IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'corpus' AND tablename = t) THEN
            EXECUTE format('ALTER TABLE corpus.%I OWNER TO corpus_owner', t);
        END IF;
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------------------------
-- 4. 크로스 스키마 GRANT — 실제 코드/계약에서 확인된 것만(위 배경 설명 참고)
-- ---------------------------------------------------------------------------------------------
GRANT USAGE ON SCHEMA core TO ai_owner;
GRANT USAGE ON SCHEMA corpus TO ai_owner;
GRANT USAGE ON SCHEMA ai TO app_owner;

-- 아래 GRANT 대상 테이블 중 일부(특히 corpus.policy_chunks/search_terms)는 환경에 따라
-- 아직 존재하지 않을 수 있다(예: Policy-Chunker ingest를 이 DB에 한 번도 안 돌린 dev) —
-- 없는 테이블에 GRANT하면 에러가 나서 트랜잭션 전체가 롤백되므로, to_regclass로 존재를
-- 확인한 뒤에만 GRANT한다(테이블이 나중에 생기면 이 스크립트를 다시 실행해서 GRANT하면 됨).
DO $$
BEGIN
    IF to_regclass('core.user_insurances') IS NOT NULL THEN
        EXECUTE 'GRANT SELECT ON core.user_insurances TO ai_owner';
    END IF;
    IF to_regclass('core.user_claims') IS NOT NULL THEN
        EXECUTE 'GRANT SELECT ON core.user_claims TO ai_owner';
    END IF;
    IF to_regclass('corpus.policy_chunks') IS NOT NULL THEN
        EXECUTE 'GRANT SELECT ON corpus.policy_chunks TO ai_owner';
    END IF;
    IF to_regclass('corpus.search_terms') IS NOT NULL THEN
        EXECUTE 'GRANT SELECT ON corpus.search_terms TO ai_owner';
    END IF;
    -- 2026-08-15: migrations/ai/007_report_drafts.sql에도 같은 GRANT를 self-grant로
    -- 넣었다(ai_owner가 자기 소유 테이블에 SELECT만 내주는 데는 이 스크립트의 상위 권한이
    -- 필요 없다) — ocr_worker 기동마다 자동 재적용되므로 이 한 줄이 실제로 실행 안 돼도
    -- 더는 D2류 사고가 안 난다. 이 블록은 최초 부트스트랩 당시의 기록으로 남겨둔다.
    IF to_regclass('ai.report_drafts') IS NOT NULL THEN
        EXECUTE 'GRANT SELECT ON ai.report_drafts TO app_owner';
    END IF;

    -- Backend가 분석 실패 알림 계약(§8-9 설계 문서, ai.ocr_job_failures)을 읽기 전용
    -- @Subselect로 소비한다 — @Table(schema="ai")가 아니라 @Subselect라 이 GRANT가
    -- 없어도 부팅은 성공하지만, 해당 조회(ReportAnalysisStatusQueryService)는
    -- permission denied로 실패한다. Backend는 절대 write하지 않는다(SELECT만).
    -- 2026-08-15: 이 문의 실행이 누락돼 실제로 GRANT가 안 된 채로 몇 주 지나갔다(D2 사고).
    -- migrations/ai/008_ocr_job_failures.sql에 self-grant로 옮겨 ocr_worker 기동마다
    -- 자동 재적용되게 했다 — 이 스크립트를 다시 실행하는 걸 잊어도 더는 깨지지 않는다.
    IF to_regclass('ai.ocr_job_failures') IS NOT NULL THEN
        EXECUTE 'GRANT SELECT ON ai.ocr_job_failures TO app_owner';
    END IF;

    -- report_worker(#11, 미머지)가 실제로 쓰는 core.reports 컬럼만 UPDATE 허용 — 나머지
    -- (user_id·adjuster_id·case_no 등)는 절대 안 건드려야 하므로 컬럼 한정으로 연다.
    IF to_regclass('core.reports') IS NOT NULL THEN
        EXECUTE 'GRANT SELECT ON core.reports TO ai_owner';
        EXECUTE 'GRANT UPDATE (
            applicable_guarantees, omitted_special_contract, basis_terms_precedents,
            claimed_min_amount, claimed_max_amount, status, updated_at
        ) ON core.reports TO ai_owner';
    END IF;

    -- report_worker(#11)가 리포트(재)생성 시 DELETE + 전량 INSERT로 교체하는 패턴(SELECT는 안 씀).
    IF to_regclass('core.report_issues') IS NOT NULL THEN
        EXECUTE 'GRANT INSERT, DELETE ON core.report_issues TO ai_owner';
    END IF;

    -- 기존 FK(user_insurances.ocr_result_id, report_attachments.ocr_result_id →
    -- ai.ocr_results(id))는 이동 후에도 유효하지만, backend가 앞으로 ai.ocr_results를
    -- 참조하는 새 FK를 추가하려면 REFERENCES 권한이 필요하다 — 미리 열어둔다(당장 안 써도 무해).
    IF to_regclass('ai.ocr_results') IS NOT NULL THEN
        EXECUTE 'GRANT REFERENCES ON ai.ocr_results TO app_owner';
    END IF;
END
$$;

-- ---------------------------------------------------------------------------------------------
-- 5. 역할별 기본 search_path — 각자 자기 스키마만 무자격(unqualified) 이름으로 접근되게 한다.
--    크로스 스키마 조회(core.user_insurances 등)는 쿼리에서 항상 스키마를 명시한다(암묵적 해석에
--    기대지 않음 — 이미 나온 4번 GRANT는 스키마 한정 SQL을 쓸 때만 의미가 있다).
-- ---------------------------------------------------------------------------------------------
ALTER ROLE app_owner    SET search_path = core, public;
ALTER ROLE ai_owner      SET search_path = ai, public;
ALTER ROLE corpus_owner  SET search_path = corpus, public;

COMMIT;

-- =============================================================================================
-- 이 스크립트가 끝난 뒤 별도로(SQL 밖에서) 해야 하는 것
-- =============================================================================================
-- 1. Secrets Manager(또는 각 서비스 .env)에서 DB 자격증명 교체:
--      backend      DB_USER=app_owner   DB_PASSWORD=<위에서 심은 값>   DB_NAME은 기존 유지
--      ocr_worker   DATABASE_URL=postgresql://ai_owner:<...>@<host>:5432/<db>
--      corpus_worker DATABASE_URL=postgresql://corpus_owner:<...>@<host>:5432/<db>
--      (report_worker/rag/chatbot도 구현되면 ai_owner 재사용)
--      Policy-Chunker ingest DATABASE_URL=postgresql://corpus_owner:<...>@<host>:5432/<db>
-- 2. 세 서비스 재배포. 순서: backend(app_owner) → ocr_worker/corpus_worker(ai_owner/corpus_owner).
--    backend가 먼저 살아 있어야 kafka_outbox_events 기반 OCR 트리거가 안 끊긴다.
-- 3. 각 서비스 기동 로그에서 DB 연결·마이그레이션(Flyway/run_migrations) 성공 확인.
-- 4. rds_pgaudit 대상 목록 재검토 — DB_ENCRYPTION.md §3 체크리스트대로, 이번에 core로 옮긴 테이블 중
--    아직 rds_pgaudit에 없는 PII 테이블(예: adjuster_applications의 이름·전화번호,
--    social_accounts의 refresh_token)이 있다면 GRANT SELECT ... TO rds_pgaudit 추가를 backend
--    팀과 상의할 것 — 이건 백엔드 소유 테이블이라 이 스크립트가 임의로 정하지 않는다.
