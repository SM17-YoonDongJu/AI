-- 008_ocr_job_failures.sql — OCR 작업 처리 실패 저널(durable).
--
-- 지금까지 종결 실패(마스킹 잔류 등)는 **아무 기록도 남기지 않았다**. 컨슈머는 메시지를
-- 삭제하지 않고 재전달에 맡기고, 수신 횟수 상한을 넘으면 poison 가드가 조용히 걷어낸다
-- (core/sqs/consumer.py) — 그 결과 사용자는 "업로드했는데 아무 일도 안 일어나는" 무음
-- 실패를 겪고, 운영은 로그를 뒤지는 것 말고 실패를 조회할 방법이 없었다.
-- 실패를 한 행으로 남겨 (a) 사용자 노출(terminal=true)과 (b) 운영 조회의 근거를 만든다.
--
-- 재시도 가능/불가능 구분:
--   terminal=false  일시 실패(S3·DB·엔진 오류 등). 재전달로 회복 가능 — 회복하면
--                   pipeline이 이 행을 지운다(clear_job_failure).
--   terminal=true   결정적 실패(재전달해도 결과가 같음). 컨슈머가 첫 시도에서 즉시
--                   ack하고 여기서 종결한다 — 재전달 낭비를 없애고 사용자 신호를 앞당긴다.
-- terminal은 **단방향**(false→true)이다. 일시 실패로 시작한 job이 나중에 poison으로
-- 종결될 수는 있어도, 종결된 판정이 재전달 한 번으로 되돌아가서는 안 된다.
--
-- PII 규약(CODE_CONVENTIONS §9): 예외 **메시지 문자열은 저장하지 않는다**(error_type =
-- 클래스명만). 정형화되지 않은 예외 문자열에 OCR 원문 조각이 섞일 수 있어서다.
-- 나머지 컬럼(s3_key·user_ref·claim_id·*_id)은 전부 내부 식별자다(파일명은 UUID로 치환됨).
--
-- 로컬 PG·RDS에 **같은 SQL**을 적용해 스키마 드리프트를 차단한다(이슈 #19).

CREATE TABLE IF NOT EXISTS ai.ocr_job_failures (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    -- 멱등 키. 같은 job의 반복 실패는 새 행이 아니라 attempts 증가로 쌓인다.
    -- 역직렬화 실패(스키마 위반)면 job_id를 알 수 없어 NULL — 그때는 message_id로 추적한다.
    job_id          uuid UNIQUE,
    message_id      text,                   -- job_id 없을 때의 추적 키(SQS MessageId)
    s3_key          text,
    user_ref        text,
    content_type    text,
    doc_type_hint   text,
    claim_id        text,
    report_id       uuid,                   -- OcrJob.report_id(Spring shell 행 조인용)
    attachment_id   uuid,                   -- OcrJob.attachment_id(〃)
    failure_class   text NOT NULL CHECK (failure_class IN
                    ('masking_residual','unreadable_file','ocr_error','schema_invalid','unknown')),
    error_type      text,                   -- 예외 클래스명만(메시지 문자열 저장 절대 금지 — PII 규약 §9)
    attempts        int NOT NULL DEFAULT 1,
    terminal        boolean NOT NULL DEFAULT false,  -- true = 확정 실패(사용자 노출 기준)
    first_failed_at timestamptz NOT NULL DEFAULT now(),
    last_failed_at  timestamptz NOT NULL DEFAULT now()
);

-- 확정 실패 조회 전용 부분 인덱스 — 사용자 노출·운영 대시보드는 terminal 행만 본다.
-- 일시 실패(terminal=false)는 대부분 재전달로 회복돼 곧 지워지므로 싣지 않는다.
CREATE INDEX IF NOT EXISTS ocr_job_failures_terminal_idx
    ON ai.ocr_job_failures (last_failed_at) WHERE terminal;

-- pgaudit 대상 role(RDS 고정 이름 rds_pgaudit)에 신규 민감 테이블 등록 — DB_ENCRYPTION.md §3 체크리스트.
-- 이 테이블 자체엔 PII를 넣지 않지만(예외 클래스명·내부 식별자만), 어떤 사용자의 어떤
-- 문서가 실패했는지를 user_ref·s3_key로 이어 붙일 수 있는 조회 경로다 — 007과 같은
-- 기준으로 감사 대상에 넣는다. 로컬 PG에는 이 role이 없으므로(RDS 전용) 존재할 때만
-- GRANT한다(있으면 켜고 없으면 조용히 건너뜀, 000_extensions.sql의 pgaudit 확장 처리와 동일 패턴).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_pgaudit') THEN
        EXECUTE 'GRANT SELECT ON ai.ocr_job_failures TO rds_pgaudit';
    END IF;
END
$$;

-- Backend가 분석 실패 알림 계약(ai.ocr_job_failures)을 읽기 전용 @Subselect로 소비한다
-- (@Table(schema="ai")가 아니라 @Subselect라 이 GRANT가 없어도 앱 부팅은 성공하지만,
-- 실제 조회(ReportAnalysisStatusQueryService)는 permission denied로 실패한다). ai_owner가
-- 자기 소유 테이블에 SELECT만 내주는 self-grant라 별도 권한이 필요 없다 — ocr_worker가
-- 이 파일을 재실행할 때마다(매 기동) 자동으로 적용되므로, deploy/schema_split.sql(수동
-- 1회성 부트스트랩)의 실행 여부와 무관하게 이 GRANT는 항상 최신 상태로 유지된다.
-- app_owner는 절대 write하지 않는다(SELECT만 — §14).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_owner') THEN
        EXECUTE 'GRANT SELECT ON ai.ocr_job_failures TO app_owner';
    END IF;
END
$$;
