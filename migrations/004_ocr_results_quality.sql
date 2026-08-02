-- 004_ocr_results_quality.sql — ocr_results에 자동 품질 판정 결과 컬럼 추가.
--
-- 저신뢰도 문서(surya mean_confidence < 0.90)에서 이름/도메인 정보가 하나도
-- 검출되지 않으면 'needs_reupload'로 표시한다(ocr_worker.pipeline 판정 로직).
-- report_worker가 이 값을 보고 리포트 생성 여부를 결정한다(ReportJob.ocr_quality
-- 패스스루). IF NOT EXISTS/IF EXISTS로 반복 적용해도 안전하다.

ALTER TABLE ocr_results ADD COLUMN IF NOT EXISTS ocr_quality text NOT NULL DEFAULT 'ok';

ALTER TABLE ocr_results DROP CONSTRAINT IF EXISTS ocr_results_ocr_quality_check;
ALTER TABLE ocr_results ADD CONSTRAINT ocr_results_ocr_quality_check
    CHECK (ocr_quality IN ('ok', 'needs_reupload'));
