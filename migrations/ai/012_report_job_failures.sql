-- 012_report_job_failures.sql — 리포트 작업 처리 실패 저널(durable).
--
-- 실패한 리포트는 저장하지 않는다(report_worker.graph — 컨텍스트·LLM 시스템·저장 실패는
-- persist 전에 멈추고 재전달된다). 재전달 한도(report 운영 SQS_MAX_RECEIVE_COUNT=3)를 다
-- 쓰면 컨슈머 poison 가드가 메시지를 지운다(core/sqs/consumer.py). 그때까지 아무것도
-- 기록하지 않으면 reports.status는 Spring 생성값(AWAITING_INSPECTION)에 남고, 초안도
-- 없어 Spring analysis_state가 **영원히 PROCESSING**이다. OCR 작업 실패 저널(008
-- ai.ocr_job_failures)과 같은 방식으로 실패를 한 행으로 남겨, Spring이 terminal 행을 읽어
-- FAILED로 표시하게 한다.
--
-- 행의 수명:
--   terminal=false  하드 실패로 재전달되는 중(회복 가능). 재전달마다 attempts가 는다.
--                   그 리포트가 나중에 종결 상태(저장·BLOCKED·NEEDS_REUPLOAD)에 이르면
--                   report_worker가 이 행을 지운다(회복).
--   terminal=true   재전달 한도를 소진해 poison으로 걷혔다(포기 확정). Spring FAILED 기준.
-- terminal은 **단방향**(false→true)이다. 비종결 업서트는 terminal을 아예 건드리지 않고,
-- poison 훅만 true로 바꾼다 — 확정된 판정이 뒤늦은 재전달 한 번으로 되돌아가지 않는다.
--
-- 싣지 않는 것: LLM 거절(LlmRefusalError). 거절은 reports.status='BLOCKED'로 이미
-- 사용자에게 보이는 종료 상태라, 여기에도 남기면 같은 리포트에 "실패"와 "차단"이 겹친다.
--
-- report_id가 NULL인 행: 본문 역직렬화 자체가 실패했거나 report_id가 UUID가 아닌
-- 메시지(schema_invalid). Spring이 조인할 키가 없어 message_id로만 추적한다(운영 조회용).
--
-- PII 규약(CODE_CONVENTIONS §9): 예외 **메시지 문자열은 저장하지 않는다**(error_type =
-- 클래스명만). 나머지 컬럼은 전부 내부 식별자·분류값이다.
--
-- 이 디렉터리는 ocr_worker와 report_worker가 **둘 다** 기동 시 적용한다 — GPU EC2(OCR)가
-- 꺼져 있어도 report가 자기 저널 테이블을 보장해야 해서다. 동시 기동 경합은
-- core.db.run_migrations의 디렉터리별 advisory lock이 막는다. 이 파일은 CREATE ...
-- IF NOT EXISTS와 GRANT뿐이라 어느 워커가 몇 번 적용해도 같은 결과다.
--
-- 로컬 PG·RDS에 **같은 SQL**을 적용해 스키마 드리프트를 차단한다(이슈 #19).

CREATE TABLE IF NOT EXISTS ai.report_job_failures (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    -- 멱등 키이자 Spring 조인 키(core.reports.id). 같은 리포트의 반복 실패는 새 행이 아니라
    -- attempts 증가로 쌓인다. NULL = schema_invalid(위 설명) — UNIQUE에 걸리지 않는다.
    report_id       uuid UNIQUE,
    message_id      text,                   -- SQS MessageId. poison 확정 시 채운다(report_id 없을 때의 유일한 추적 키)
    claim_id        text,                   -- ReportJob.claim_id(USER_CLAIMS.id, 없으면 NULL)
    job_id          uuid,                   -- ReportJob.job_id(원 OCR 작업 — 운영 추적용)
    failure_class   text NOT NULL CHECK (failure_class IN
                    ('context_unavailable','llm_unavailable','persist_failed','schema_invalid','unknown')),
    error_type      text,                   -- 예외 클래스명만(메시지 문자열 저장 절대 금지 — PII 규약 §9)
    attempts        int NOT NULL DEFAULT 1,
    terminal        boolean NOT NULL DEFAULT false,  -- true = 포기 확정(Spring FAILED 기준)
    first_failed_at timestamptz NOT NULL DEFAULT now(),
    last_failed_at  timestamptz NOT NULL DEFAULT now()
);

-- 확정 실패 조회 전용 부분 인덱스 — 운영 대시보드는 terminal 행만 최근 순으로 본다.
-- 일시 실패(terminal=false)는 대부분 재전달로 회복돼 곧 지워지므로 싣지 않는다.
-- Spring의 리포트별 조회(report_id = ? AND terminal)는 report_id UNIQUE 인덱스가 받는다.
CREATE INDEX IF NOT EXISTS report_job_failures_terminal_idx
    ON ai.report_job_failures (last_failed_at) WHERE terminal;

-- pgaudit 대상 role(RDS 고정 이름 rds_pgaudit)에 신규 테이블 등록 — DB_ENCRYPTION.md §3 체크리스트.
-- 이 테이블엔 PII가 없지만(예외 클래스명·내부 식별자만), 어느 사용자의 어떤 리포트가 왜
-- 실패했는지를 report_id·claim_id로 이어 붙일 수 있는 조회 경로다 — 008·010과 같은 기준으로
-- 감사 대상에 넣는다. 로컬 PG에는 이 role이 없으므로(RDS 전용) 존재할 때만 GRANT한다.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_pgaudit') THEN
        EXECUTE 'GRANT SELECT ON ai.report_job_failures TO rds_pgaudit';
    END IF;
END
$$;

-- Backend가 이 저널을 읽기 전용(@Subselect 등)으로 소비해 FAILED를 파생한다(008과 같은
-- 방식). ai_owner가 자기 소유 테이블에 SELECT만 내주는 self-grant라 별도 권한이 필요 없고,
-- 워커가 이 파일을 재실행할 때마다(매 기동) 자동으로 다시 적용된다. 내용 컬럼이 없어
-- 컬럼 한정 없이 테이블 전체를 연다(§14). app_owner는 절대 write하지 않는다(SELECT만).
-- 받는 롤은 접속 롤에서 끌어낸다(ai_owner → app_owner, ai_owner_prod → app_owner_prod). dev·prod
-- 롤이 같은 RDS 인스턴스에 함께 있어서, 'app_owner'를 박아 두면 prod에서 dev 롤이 권한을 받는다.
DO $$
DECLARE
    backend_role text := CASE WHEN current_user ~ '^ai_owner'
                              THEN regexp_replace(current_user, '^ai_owner', 'app_owner')
                              ELSE 'app_owner' END;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = backend_role) THEN
        EXECUTE format('GRANT SELECT ON ai.report_job_failures TO %I', backend_role);
    END IF;
END
$$;
