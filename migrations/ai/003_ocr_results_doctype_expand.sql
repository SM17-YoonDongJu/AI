-- 003_ocr_results_doctype_expand.sql — DocType에 입원확인서·진료비영수증 추가.
--
-- ocr_results.doc_type CHECK 제약(001에서 정의)을 신규 DocType 값까지 허용하도록
-- 교체한다. PostgreSQL은 CHECK 제약을 직접 ALTER 못 하므로 이름 유지한 채
-- DROP → ADD한다. IF EXISTS로 반복 적용해도 안전하다.

ALTER TABLE ai.ocr_results DROP CONSTRAINT IF EXISTS ocr_results_doc_type_check;

ALTER TABLE ai.ocr_results ADD CONSTRAINT ocr_results_doc_type_check
    CHECK (doc_type IN (
        'diagnosis', 'policy', 'payout_notice', 'claim',
        'hospitalization_cert', 'medical_receipt', 'other'
    ));
