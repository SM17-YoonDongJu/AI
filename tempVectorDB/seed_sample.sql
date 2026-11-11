-- 실험용 시드 1세트. run_local.py SAMPLE_JOB의 UUID와 매칭.
-- 적재된 메리츠 '다모아상해보험'에 연결.

-- 멱등: 재실행 위해 기존 시드 제거
DELETE FROM report_issues  WHERE report_id = '00000000-0000-0000-0000-000000000001';
DELETE FROM report_drafts  WHERE report_id = '00000000-0000-0000-0000-000000000001';
DELETE FROM reports        WHERE id        = '00000000-0000-0000-0000-000000000001';
DELETE FROM user_claims    WHERE id        = '00000000-0000-0000-0000-000000000003';
DELETE FROM user_insurances WHERE id       = '00000000-0000-0000-0000-000000000004';
DELETE FROM ocr_results    WHERE id        = '00000000-0000-0000-0000-000000000002';

INSERT INTO ocr_results (id, job_id, doc_type, masked_text, entities) VALUES (
  '00000000-0000-0000-0000-000000000002',
  '00000000-0000-0000-0000-0000000000a2',
  'diagnosis',
  E'진단서\n환자: 홍**(******-*******)\n진단명: 우측 슬관절 전방십자인대 파열, 반월상연골 파열\n상병코드: S83.5\n수술명: 관절경적 전방십자인대 재건술 시행\n입원: 2026-03-02 ~ 2026-03-16 (14일)\n향후 후유장해 평가 필요 소견.',
  '{"diagnosis":"전방십자인대 파열","icd":"S83.5","surgery":true,"admission_days":14}'::jsonb
);

INSERT INTO user_insurances (id, user_id, insurer_name, product_name, match_status, policy_no, enrolled_at, coverages, coverage_details, ocr_result_id) VALUES (
  '00000000-0000-0000-0000-000000000004',
  '00000000-0000-0000-0000-000000000011',
  '메리츠화재',
  '다모아상해보험',
  'MATCHED',
  'POL-2026-0001',
  '2022-05-01',
  ARRAY['상해후유장해','상해입원일당','수술비특약','골절진단비'],
  -- 특약별 가입금액(증권 OCR로 확보한다고 가정한 값). type: disability|per_diem|surgery|fracture
  '[{"name":"상해후유장해","type":"disability","amount":30000000},
    {"name":"상해입원일당","type":"per_diem","amount":30000},
    {"name":"수술비특약","type":"surgery","amount":500000},
    {"name":"골절진단비","type":"fracture","amount":300000}]'::jsonb,
  NULL
);

INSERT INTO user_claims (id, user_id, product_id, offered_amount, accident_date, hospitalization, diagnosis, description, accident_type) VALUES (
  '00000000-0000-0000-0000-000000000003',
  '00000000-0000-0000-0000-000000000011',
  NULL,
  1200000,
  '2026-03-02',
  '[{"hospitalStart":"2026-03-02","hospitalEnd":"2026-03-16","hospitalReason":"전방십자인대 재건술"}]'::jsonb,
  '우측 슬관절 전방십자인대 파열',
  '계단에서 미끄러져 무릎을 다침. 수술 후 입원 치료.',
  'disability'
);

INSERT INTO reports (id, user_id, claim_id, accident_type, treatment, offered_amount, question, status) VALUES (
  '00000000-0000-0000-0000-000000000001',
  '00000000-0000-0000-0000-000000000011',
  '00000000-0000-0000-0000-000000000003',
  'disability',
  '우측 슬관절 전방십자인대 파열',
  1200000,
  '보험금이 너무 적게 나온 것 같아요. 후유장해나 누락된 특약이 있는지 확인하고 싶어요.',
  'AWAITING_INSPECTION'
);
