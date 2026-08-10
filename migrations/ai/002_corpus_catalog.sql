-- 002_corpus_catalog.sql — 약관 코퍼스 카탈로그 미러 (이슈 #35 P1).
--
-- Notion(공식 REST API)의 두 데이터베이스를 PostgreSQL로 **읽기 전용 미러링**한다:
--   corpus_source ← "데이터 출처 카탈로그"(카테고리·우선순위 P0~P3·수집단계)
--   corpus_file   ← "보험 약관 파일"(약관 문서, 우선순위 큐 단위)
--   corpus_file_part ← 한 약관 문서의 첨부 파일 파트(다운로드/S3는 P2 이후 범위)
--
-- 동기화(corpus_worker.sync)는 Notion 메타만 갱신하고, 파생·상태 컬럼(demand_score·
-- priority·status·attempts·part_done 등)은 별도 경로(우선순위 큐·수요 부스트)가 관리한다.
-- 로컬 PG·RDS에 **같은 SQL**을 적용해 스키마 드리프트를 차단한다(이슈 #19). 모든 DDL은
-- IF NOT EXISTS로 작성해 반복 적용해도 안전하다.

-- 출처 카탈로그 미러. notion_page_id를 PK로 써서 Notion 페이지와 1:1 멱등 매핑한다.
CREATE TABLE IF NOT EXISTS ai.corpus_source (
    notion_page_id      uuid PRIMARY KEY,                       -- Notion page id(멱등 키)
    name                text        NOT NULL,                   -- 이름(title)
    category            text,                                   -- 카테고리(terms/precedent/...)
    priority_tier       text,                                   -- 우선순위(P0~P3) → priority 계산 입력
    phase               text,                                   -- 수집단계(Phase1~3/보조)
    access_method       text,                                   -- 접근방식(api/html/pdf/...)
    mvp_tier            text,                                   -- MVP(required/extension/reference)
    legal_review        boolean     NOT NULL DEFAULT false,     -- 법률검토필요(checkbox)
    license             text,                                   -- 라이선스
    reliability         smallint,                               -- 신뢰도(1/2)
    operator            text,                                   -- 운영주체
    source_url          text,                                   -- URL
    memo                text,                                   -- 메모
    notion_last_edited  timestamptz,                            -- Notion last_edited_time(증분 커서)
    synced_at           timestamptz NOT NULL DEFAULT now()      -- 마지막 미러 시각
);

-- 약관 문서 미러(우선순위 큐 단위). Notion 메타 + 큐/상태 컬럼이 한 행에 공존하되,
-- 큐/상태 컬럼은 동기화가 건드리지 않는다(repository.upsert_file 주석 참조).
CREATE TABLE IF NOT EXISTS ai.corpus_file (
    notion_page_id      uuid PRIMARY KEY,                       -- Notion page id(멱등 키)
    source_id           uuid REFERENCES ai.corpus_source(notion_page_id),  -- 출처(relation 첫값)
    category            text        NOT NULL DEFAULT 'terms',   -- 코퍼스 카테고리(MVP=terms)
    product_name        text,                                   -- 상품명(수요 매칭 trigram 대상)
    company             text,                                   -- 회사
    product_type        text,                                   -- 상품종류(표준약관/실손의료/...)
    product_code        text,                                   -- 상품코드
    version_no          text,                                   -- 버전번호
    effective_date      date,                                   -- 시행일
    license             text,                                   -- 라이선스
    source_url          text,                                   -- 출처URL
    is_latest           boolean,                                -- 최신판(checkbox)
    collect_status      text,                                   -- 수집상태(Notion 원본 상태)
    -- --- 파생·상태 컬럼(동기화가 덮어쓰지 않음 · 별도 경로 관리) ---
    demand_score        real        NOT NULL DEFAULT 0,         -- 수요도 점수(수요 부스트, P2)
    demand_last_at      timestamptz,                            -- 마지막 수요 발생 시각(P2)
    priority            double precision NOT NULL DEFAULT 0,    -- 우선순위 점수(priority.compute)
    status              text        NOT NULL DEFAULT 'pending', -- 파이프라인 상태(큐)
    part_total          smallint    NOT NULL DEFAULT 0,         -- 첨부 파트 총수(Notion files 개수)
    part_done           smallint    NOT NULL DEFAULT 0,         -- 완료 파트 수(업로드 진행, P2)
    fail_reason         text,                                   -- 실패 사유(P2)
    attempts            smallint    NOT NULL DEFAULT 0,         -- 처리 시도 횟수(P2)
    next_retry_at       timestamptz,                            -- 재시도 예정 시각(지수 백오프). claim은 이후만 집음
    notion_last_edited  timestamptz,                            -- Notion last_edited_time(증분 커서)
    synced_at           timestamptz NOT NULL DEFAULT now(),     -- 마지막 미러 시각
    updated_at          timestamptz NOT NULL DEFAULT now(),     -- 마지막 변경 시각
    -- status는 큐 상태 머신 값만 허용 — 계약 위반을 DB에서도 방어한다.
    CONSTRAINT corpus_file_status_check
        CHECK (status IN ('pending', 'in_progress', 'done', 'failed', 'archived'))
);

-- 약관 문서의 첨부 파일 파트. Notion files 첨부는 만료 URL이라 URL은 저장하지 않고
-- 이름·순서만 미러한다. sha256/s3_key/byte_size는 다운로드·업로드(P2)가 채운다.
CREATE TABLE IF NOT EXISTS ai.corpus_file_part (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    file_page_id        uuid        NOT NULL
                            REFERENCES ai.corpus_file(notion_page_id) ON DELETE CASCADE,
    part_order          smallint    NOT NULL,                   -- 파일 내 파트 순서(0-base)
    notion_file_name    text,                                   -- Notion 첨부 파일명
    sha256              text,                                   -- 내용 해시(업로드 후, P2)
    s3_key              text,                                   -- S3 키(업로드 후, P2)
    byte_size           bigint,                                 -- 바이트 크기(업로드 후, P2)
    status              text        NOT NULL DEFAULT 'pending', -- 파트 업로드 상태
    -- 한 문서 안에서 파트 순서는 유일하다(멱등 upsert 앵커).
    CONSTRAINT corpus_file_part_order_key UNIQUE (file_page_id, part_order),
    CONSTRAINT corpus_file_part_status_check
        CHECK (status IN ('pending', 'uploaded', 'failed'))
);

-- 우선순위 큐 스캔: 상태별로 priority 내림차순 상위 N개를 뽑는 폴링 쿼리 인덱스.
CREATE INDEX IF NOT EXISTS corpus_file_status_priority_idx
    ON ai.corpus_file (status, priority DESC);

-- 상품명 근사 검색(수요 매칭) — pg_trgm gin 인덱스. product_name NULL은 색인되지 않는다.
CREATE INDEX IF NOT EXISTS corpus_file_product_name_trgm_idx
    ON ai.corpus_file USING gin (product_name gin_trgm_ops);

-- 내용주소 조회·수요 매칭용(전역 유니크 아님). 여러 문서가 동일 첨부(공통 특약·별표)를
-- 공유할 수 있으므로 sha256을 유일하게 강제하지 않는다 — 실제 중복 업로드 방지는 S3
-- HeadObject(내용주소 키 corpus/{category}/{sha256}.pdf)가 담당한다.
CREATE INDEX IF NOT EXISTS corpus_file_part_sha256_idx
    ON ai.corpus_file_part (sha256) WHERE sha256 IS NOT NULL;
