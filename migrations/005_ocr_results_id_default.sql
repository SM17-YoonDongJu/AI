-- 005_ocr_results_id_default.sql — ocr_results.id에 gen_random_uuid() 기본값 복구.
--
-- 001_ocr_results.sql은 `CREATE TABLE IF NOT EXISTS`라 이미 존재하는 테이블의 컬럼
-- 기본값을 소급 적용하지 못한다. 일부 환경(dev RDS)에서 테이블이 DEFAULT
-- gen_random_uuid() 없이 먼저 생성돼 있어, save_ocr_result()의 INSERT가 id를 명시
-- 하지 않는 전제와 어긋나 모든 저장이 NotNullViolationError로 실패하고 있었다
-- (조용히 DLQ로 빠짐 — 이슈: OCR 파이프라인이 dev에서 한 건도 저장되지 않던 근본 원인).
-- ALTER COLUMN ... SET DEFAULT는 이미 같은 기본값이어도 안전하게 반복 적용된다.

ALTER TABLE ai.ocr_results ALTER COLUMN id SET DEFAULT gen_random_uuid();
