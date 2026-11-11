-- 001_ocr_results.sql — ocr_results 교차 계약 테이블 (contracts.md §3, 이슈 #19).
--
-- `ocr_worker`가 쓰고 `report_worker`가 `ocr_result_id`로 읽는다. 마스킹된 텍스트·
-- 라인좌표·비식별 이미지 참조만 보관한다 — 원문/평문 PII는 절대 저장 금지.
-- 로컬 PG·RDS에 **같은 SQL**을 적용해 스키마 드리프트를 차단한다. 모든 DDL은
-- IF NOT EXISTS로 작성해 반복 적용해도 안전하다.

CREATE TABLE IF NOT EXISTS ai.ocr_results (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id                uuid        NOT NULL,               -- OCR 작업(멱등 키)
    doc_type              text        NOT NULL,               -- DocType enum 값
    doc_type_confidence   real        NOT NULL DEFAULT 0,     -- 분류 신뢰도 0~1
    ocr_confidence        real        NOT NULL DEFAULT 0,     -- OCR 라인 평균 신뢰도 0~1(QA)
    masked_text           text        NOT NULL,               -- PII 마스킹된 OCR 텍스트
    masked_lines          jsonb       NOT NULL DEFAULT '[]'::jsonb,  -- [{masked_text,bbox,polygon,confidence}]
    entities              jsonb       NOT NULL DEFAULT '{}'::jsonb,  -- 비-PII 추출 엔티티
    masked_image_s3_keys  jsonb       NOT NULL DEFAULT '[]'::jsonb,  -- 비식별 이미지 사본 S3 키(페이지별)
    created_at            timestamptz NOT NULL DEFAULT now(),
    -- doc_type은 DocType(core.contracts) 값만 허용 — 계약 위반을 DB에서도 방어한다.
    CONSTRAINT ocr_results_doc_type_check
        CHECK (doc_type IN ('diagnosis', 'policy', 'payout_notice', 'claim', 'other'))
);

-- job_id 멱등: at-least-once 재소비/재처리 시 한 OCR 작업은 정확히 한 행만 유지한다
-- (repository의 ON CONFLICT (job_id) 업서트 앵커).
CREATE UNIQUE INDEX IF NOT EXISTS ocr_results_job_id_key ON ai.ocr_results (job_id);
