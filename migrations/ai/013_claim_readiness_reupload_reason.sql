-- 013_claim_readiness_reupload_reason.sql — 청구를 재업로드로 보낸 사유를 남기고 app_owner에 연다.
--
-- reports.status='NEEDS_REUPLOAD'는 "다시 올려야 한다"까지만 알린다. Backend는 그래서 원인과
-- 무관하게 "알아보기 어려워요. 더 선명하게 다시 올려주세요." 한 문구만 보여 줬다. 보험과 무관한
-- 문서를 올린 사용자에게도 같은 문구가 나갔다. 사유를 OCR 워커가 판정하는 곳(pipeline._judge_claim)
-- 에서 남겨, Backend가 사유별 문구를 고를 수 있게 한다.
--
-- reupload_reason (NULL = 재업로드 아님. 이 컬럼 이전에 판정된 청구도 NULL이다). 위에서부터 먼저
-- 맞는 것이 사유다(pipeline._blocked_reason):
--   unreadable        처리에 실패한 문서나 저품질 문서(ocr_quality='needs_reupload')가 있다
--   off_domain        읽을 수 있지만 본문 단서로 보험·의료 문서 유형을 판정한 문서가 하나도 없다
--   missing_required  보험 문서는 인식했지만 필수 유형(보험증권·진단서) 중 빠진 게 있다
-- 값 집합은 ocr_worker.repository.ReuploadReason과 같다(테스트가 둘을 맞춘다).
--
-- status='blocked'면 언제나 채워진다. status='published'여도 저품질 문서가 섞여 대표 ReportJob이
-- ocr_quality='needs_reupload'로 나가면 'unreadable'이 채워진다(그 리포트도 NEEDS_REUPLOAD가 된다).
--
-- app_owner에는 Backend가 쓸 컬럼만 연다(§14 최소 권한). claim_id는 @Subselect 엔티티의 @Id용,
-- report_id는 리포트와 잇는 키, missing_doc_types는 "빠진 서류" 안내용이다. terminal_job_ids 같은
-- 내부 진행 상태는 열지 않는다. 받는 롤은 접속 롤에서 끌어낸다(011과 같은 이유 — dev·prod 롤이
-- 같은 RDS 인스턴스에 함께 있다).
--
-- 워커가 기동할 때마다 다시 적용된다. 컬럼·제약이 이미 있으면 카탈로그만 보고 아무 DDL도 내지
-- 않는다. 이 테이블은 Backend도 읽으므로, 매번 ALTER로 ACCESS EXCLUSIVE 잠금을 잡으면 열린 Backend
-- 조회 하나가 워커 기동과 fan-in 쓰기를 붙잡는다. 제약도 없을 때만 만든다 — 값을 늘릴 때는 새
-- 마이그레이션에서 이 제약을 지우고 다시 만든다(여기서 매번 다시 검증하면 그 마이그레이션이 넣은
-- 새 값에 이 파일이 실패한다).
--
-- 로컬 PG·RDS에 **같은 SQL**을 적용해 스키마 드리프트를 차단한다(이슈 #19).

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ai' AND table_name = 'claim_readiness'
          AND column_name = 'reupload_reason'
    ) THEN
        ALTER TABLE ai.claim_readiness ADD COLUMN reupload_reason text;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'ai.claim_readiness'::regclass
          AND conname = 'claim_readiness_reupload_reason_check'
    ) THEN
        ALTER TABLE ai.claim_readiness ADD CONSTRAINT claim_readiness_reupload_reason_check
            CHECK (reupload_reason IN ('unreadable', 'off_domain', 'missing_required'));
    END IF;
END
$$;

DO $$
DECLARE
    backend_role text := CASE WHEN current_user ~ '^ai_owner'
                              THEN regexp_replace(current_user, '^ai_owner', 'app_owner')
                              ELSE 'app_owner' END;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = backend_role) THEN
        EXECUTE format('GRANT SELECT (claim_id, report_id, missing_doc_types, reupload_reason)
                        ON ai.claim_readiness TO %I', backend_role);
    END IF;
END
$$;
