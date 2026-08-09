-- 006_ocr_results_original_delete_outbox.sql — 원본 S3 삭제 outbox(재시도 상태) 컬럼.
--
-- 마스킹 검증(5.1+5.2)을 통과한 작업은 S3 원본(OcrJob.s3_key)을 지운다. 지금까지 그
-- 삭제는 저장 직후 fire-and-forget task 1회 시도가 전부라, 실패하거나 그 전에 crash가
-- 나면 원본이 조용히 남고 재시도 근거(어떤 키를 지워야 하는지)조차 DB에 없었다.
-- 삭제 대상 키와 시도 상태를 ocr_results 같은 행에 남겨(= 저장과 같은 트랜잭션)
-- 워커 스윕이 재시도할 수 있는 durable 경로를 만든다(ocr_worker.pipeline).
--
-- original_delete_status 전이:
--   not_eligible  검증 실패 등으로 애초에 삭제 대상이 아님 — 스윕이 절대 건드리지 않음
--   pending       삭제 대상(최초 저장 시 또는 실패 후 재시도 대기)
--   deleted       삭제 성공(종결)
--   exhausted     시도 횟수 소진(종결 — 이후는 S3 라이프사이클 정책이 백스톱)
--
-- 004와 같은 스타일: IF NOT EXISTS DDL + CHECK는 NOT VALID → VALIDATE(쓰기 차단 최소화).

ALTER TABLE ai.ocr_results ADD COLUMN IF NOT EXISTS original_s3_key text;
ALTER TABLE ai.ocr_results
    ADD COLUMN IF NOT EXISTS original_delete_status text NOT NULL DEFAULT 'not_eligible';
ALTER TABLE ai.ocr_results
    ADD COLUMN IF NOT EXISTS original_delete_attempts integer NOT NULL DEFAULT 0;
-- NULL이면 "즉시 시도 가능"(최초 pending 삽입 시점). 실패하면 다음 시도 시각이 채워진다.
ALTER TABLE ai.ocr_results
    ADD COLUMN IF NOT EXISTS original_delete_next_attempt_at timestamptz;

-- NOT VALID로 추가해 기존 행 풀스캔 동안 쓰기가 막히지 않게 한다(ACCESS EXCLUSIVE는
-- 짧게만 잡힘). VALIDATE CONSTRAINT는 SHARE UPDATE EXCLUSIVE라 스캔 중에도 쓰기 가능 —
-- 새/변경 행은 NOT VALID 상태에서도 즉시 제약이 적용된다.
ALTER TABLE ai.ocr_results DROP CONSTRAINT IF EXISTS ocr_results_original_delete_status_check;
ALTER TABLE ai.ocr_results ADD CONSTRAINT ocr_results_original_delete_status_check
    CHECK (original_delete_status IN ('not_eligible', 'pending', 'deleted', 'exhausted')) NOT VALID;
ALTER TABLE ai.ocr_results VALIDATE CONSTRAINT ocr_results_original_delete_status_check;

-- 스윕 조회 전용 부분 인덱스 — 종결 상태(deleted/exhausted)와 애초 비대상(not_eligible)은
-- 인덱스에 싣지 않는다. 전체 행 대비 pending은 극소수라 인덱스가 작게 유지된다.
CREATE INDEX IF NOT EXISTS ocr_results_pending_delete_idx
    ON ai.ocr_results (original_delete_next_attempt_at)
    WHERE original_delete_status = 'pending';
