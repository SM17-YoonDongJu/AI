-- 009_ocr_results_claim_columns.sql — ocr_results에 청구(claim) 소속 정보 추가.
--
-- 지금까지 워커는 문서 1건을 처리할 때마다 곧바로 ReportJob을 발행했다. 그런데 한
-- 청구(claim)는 보험증권·진단서 등 **여러 문서**로 구성되고, OcrJob 계약에는 이미
-- claim_id·report_id(Spring이 청구 단위로 미리 만든 공용 ID)·doc_index·doc_total이
-- 실려 온다(contracts.py §OcrJob 소유권 메모) — 워커가 그걸 안 쓰고 있었을 뿐이다.
-- 그 결과 문서 N건짜리 청구가 리포트 N건을 만들었다.
--
-- 청구의 마지막 문서가 끝난 시점에 "이 청구에 어떤 유형의 문서가 모였는가"를 판정
-- (fan-in)하려면, 저장된 결과 행에서 claim_id로 형제 문서를 되짚을 수 있어야 한다.
-- 그래서 OcrJob의 청구 컨텍스트를 결과 행에 그대로 남긴다. 전부 nullable이다 —
-- 청구에 묶이지 않은 단독 문서(기존 경로)도 계속 저장돼야 하고, 이미 쌓인 행들은
-- 소급 채울 수 없다.
--
-- 전부 내부 식별자다(PII 아님) — DB_ENCRYPTION.md 기준 신규 민감 컬럼이 아니라
-- 별도 GRANT는 필요 없다(테이블 자체는 001에서 이미 감사 대상).
--
-- 로컬 PG·RDS에 **같은 SQL**을 적용해 스키마 드리프트를 차단한다(이슈 #19).

ALTER TABLE ai.ocr_results ADD COLUMN IF NOT EXISTS claim_id text;
ALTER TABLE ai.ocr_results ADD COLUMN IF NOT EXISTS report_id uuid;
ALTER TABLE ai.ocr_results ADD COLUMN IF NOT EXISTS attachment_id uuid;
ALTER TABLE ai.ocr_results ADD COLUMN IF NOT EXISTS doc_index int;
ALTER TABLE ai.ocr_results ADD COLUMN IF NOT EXISTS doc_total int;

-- fan-in 조회(fetch_claim_documents)는 항상 claim_id로 형제 문서를 훑는다. 청구에
-- 묶이지 않은 단독 문서(claim_id IS NULL)는 이 경로로 조회될 일이 없으므로 부분
-- 인덱스로 두어 인덱스를 작게 유지한다(006의 pending delete 부분 인덱스와 같은 패턴).
CREATE INDEX IF NOT EXISTS ocr_results_claim_id_idx
    ON ai.ocr_results (claim_id) WHERE claim_id IS NOT NULL;
