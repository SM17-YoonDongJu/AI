-- 앱 테이블 (Notion ERD 기준 최소 집합) — report_worker 실험용.
-- 전체 ERD 아님. report_worker가 읽고/쓰는 테이블만. policy_chunks는 01_schema.sql.

-- 증빙 OCR·PII 마스킹 결과 (report_worker 입력)
CREATE TABLE IF NOT EXISTS ocr_results (
    id          UUID PRIMARY KEY,
    job_id      UUID,
    doc_type    TEXT,                 -- diagnosis|policy|payout_notice|claim|other
    masked_text TEXT,                 -- PII 마스킹된 OCR 텍스트 (downstream 입력)
    entities    JSONB,                -- 추출 엔티티
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- 사용자 청구
CREATE TABLE IF NOT EXISTS user_claims (
    id                     UUID PRIMARY KEY,
    user_id                UUID,
    product_id             UUID,
    offered_amount         BIGINT,            -- 제안 받은 보험금
    accident_date          DATE,
    hospitalization        JSONB,             -- [{hospitalStart, hospitalEnd, hospitalReason}]
    diagnosis              TEXT,
    description            TEXT,
    additional_information TEXT,
    accident_type          TEXT,              -- ERD accident_type enum 값
    created_at             TIMESTAMPTZ DEFAULT now()
);

-- 사용자 가입보험
CREATE TABLE IF NOT EXISTS user_insurances (
    id            UUID PRIMARY KEY,
    user_id       UUID,
    insurer_name  TEXT,                -- 보험사 원문 (policy_chunks.insurer와 매칭 키)
    product_name  TEXT,                -- 상품명 원문 (policy_chunks.product_name과 매칭 키)
    product_id    UUID,                -- INSURANCE_PRODUCTS 매칭(nullable)
    match_status  TEXT,                -- MATCHED|UNMATCHED|PENDING
    policy_no     TEXT,
    enrolled_at   DATE,
    coverages     TEXT[],              -- 가입 특약 → subscribed_coverages
    policy_file_url TEXT,
    ocr_result_id UUID,
    created_at    TIMESTAMPTZ DEFAULT now()
);

-- 보상분석 리포트 (report_worker가 컬럼 업데이트)
CREATE TABLE IF NOT EXISTS reports (
    id                       UUID PRIMARY KEY,
    user_id                  UUID,
    adjuster_id              UUID,
    product_id               UUID,
    claim_id                 UUID,
    accident_type            TEXT,
    treatment                TEXT,             -- 질병명
    claimed_min_amount       BIGINT,
    claimed_max_amount       BIGINT,
    offered_amount           BIGINT,
    applicable_guarantees    TEXT[],
    omitted_special_contract TEXT[],
    basis_terms_precedents   TEXT[],
    question                 TEXT,
    case_no                  VARCHAR,
    status                   TEXT,             -- AWAITING_INSPECTION|AWAITING_ADOPTION|...
    created_at               TIMESTAMPTZ DEFAULT now(),
    updated_at               TIMESTAMPTZ DEFAULT now()
);

-- 리포트 초안 (report_worker 출력, JSONB 영구 보존)
CREATE TABLE IF NOT EXISTS report_drafts (
    report_id  UUID PRIMARY KEY,
    draft      JSONB,                  -- sections·estimated_range·disclaimer·judge_failures·issues
    status     TEXT,                   -- draft|signed|rejected
    created_at TIMESTAMPTZ DEFAULT now()
);

-- 리포트 쟁점 (report_worker가 insert)
CREATE TABLE IF NOT EXISTS report_issues (
    id               UUID PRIMARY KEY,
    report_id        UUID,
    title            VARCHAR,
    description      TEXT,                  -- AI 의견/설명
    ai_status        TEXT,                  -- CONFIRMED|TRUSTED|INFO
    review_status    TEXT DEFAULT 'PENDING',-- PENDING|ACCEPTED|MODIFIED|EXCLUDED
    tags             TEXT[],                -- 참조 (예: 약관 제5조)
    impact_amount    BIGINT,
    adjuster_opinion TEXT,
    modified_reason  TEXT,
    excluded_reason  TEXT,
    created_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_report_issues_report ON report_issues (report_id);
