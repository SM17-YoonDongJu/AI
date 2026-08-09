-- 012_report_drafts.sql — report_drafts 테이블 신설 (contracts.md §4).
--
-- report_worker가 쓰는 AI 리포트 초안(JSONB, 영구 보존 — 손해사정사 검수 근거).
-- 원래 007번으로 추가됐었으나, RAG 마이그레이션(#11)이 007~011을 먼저 점유하면서 파일이
-- 유실됐다(dev에 배포된 report_worker가 이 테이블 없이 돌다가 발견·재작성, 이슈 확인 후 012로
-- 재배치). ocr_results 때처럼 DDL 소유가 backend/engine 사이에서 갈리지 않도록 처음부터
-- `ai` 스키마에 스키마 한정(qualified)으로 만든다.
--
-- 로컬 PG·RDS에 **같은 SQL**을 적용해 스키마 드리프트를 차단한다(이슈 #19).

CREATE TABLE IF NOT EXISTS ai.report_drafts (
    report_id   uuid PRIMARY KEY,                  -- ReportJob.report_id
    draft       jsonb       NOT NULL,               -- 리포트 초안 본문(sections·estimated_range·disclaimer·judge_failures)
    status      text        NOT NULL DEFAULT 'draft',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz,                        -- 상태 전이(draft→signed/rejected) 시각 — contracts.md 명시 컬럼은 아니나
                                                      -- status 전이가 언제 일어났는지 추적할 근거가 필요해 추가
    CONSTRAINT report_drafts_status_check
        CHECK (status IN ('draft', 'signed', 'rejected'))
);

-- pgaudit 대상 role(RDS 고정 이름 rds_pgaudit)에 신규 민감 테이블 등록 — DB_ENCRYPTION.md §3 체크리스트.
-- 로컬 PG에는 이 role이 없으므로(RDS 전용) 존재할 때만 GRANT한다(있으면 켜고 없으면 조용히 건너뜀,
-- 000_extensions.sql의 pgaudit 확장 처리와 동일 패턴).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_pgaudit') THEN
        EXECUTE 'GRANT SELECT ON ai.report_drafts TO rds_pgaudit';
    END IF;
END
$$;
