-- 010_claim_readiness.sql — 청구 단위 fan-in 진행/판정 상태.
--
-- 청구 1건은 문서 여러 개(증권·진단서 등)로 이루어지는데, 워커는 문서 1건씩 독립된
-- 메시지로 받는다. "이 청구의 문서가 전부 끝났는가"는 어느 한 메시지의 처리 안에서는
-- 알 수 없다 — 형제 문서를 다른 워커 프로세스가 동시에 처리하고 있을 수 있다.
-- 그래서 종결 상태를 DB 한 행에 모아, 마지막 문서를 끝낸 워커가 스스로 그 사실을
-- 알아채고(fan-in) 청구당 ReportJob 1건만 발행하게 한다.
--
-- **개수(int)가 아니라 job_id 집합을 저장한다.** 단순 카운터(`docs_terminal = … + 1`)로
-- 시작했다가 QA 실측에서 과다 카운트가 확인됐다: SQS 가시성 타임아웃 초과로 같은 문서가
-- 두 워커에 **동시 전달**되면 둘 다 +1을 해서, 3문서 청구가 2번째 문서 시점에
-- docs_terminal == doc_total을 만족해 **3번 문서가 처리되기도 전에** 불완전한 리포트를
-- 발행한다. ack(DeleteMessage) 실패 후 재전달로 같은 결정적 실패가 두 번 저널되는
-- 경로, poison 훅이 이미 세어진 문서를 또 세는 경로도 같은 결과였다.
-- 집합에 넣으면 같은 문서가 몇 번 종결로 보고되든 개수가 늘지 않는다 = **문서별 멱등**.
-- 여전히 단일 원자적 업서트라 동시 두 워커가 서로 다른 문서를 끝내도 유실이 없다.
--
-- 정공법은 (claim_id, job_id) PK 테이블 + count(*)지만, 청구당 문서가 2~5건 수준이라
-- 배열 containment(@>)로 충분하고 행/조인이 늘지 않는다. docs_terminal은 생성 컬럼으로
-- 남겨 운영 조회·인덱스 대상이 그대로 유지된다(집합과 개수가 어긋날 수 없다).
--
-- terminal_job_ids가 uuid[]가 아니라 text[]인 이유: OcrJob.job_id는 계약상 형식 검증이
-- 없는 str이다(UUID 문자열이 관례일 뿐). uuid로 강제하면 형식이 어긋난 job_id 하나가
-- 변환 실패로 그 청구의 fan-in을 통째로 멈춘다 — 식별자를 있는 그대로 담는다.
--
-- 종결(terminal)은 "성공"이 아니라 **처리가 끝났다**(성공 + 확정 실패 + poison 소진)는
-- 뜻이다. 실패한 문서를 세지 않으면 그 청구는 영원히 doc_total에 도달하지 못해 리포트가
-- 영영 나오지 않는다. "무엇이 실제로 인식됐는가"는 여기가 아니라 ai.ocr_results를 다시
-- 조회해서 판정한다(성공한 문서만 행이 생긴다).
--
-- status:
--   pending    아직 문서가 남았다(또는 판정 전).
--   blocked    전부 종결됐는데 필수 문서 유형이 빠졌다 → 리포트를 만들지 않는다.
--              보험증권·진단서는 업로드 자체가 필수라, 여기서 빠졌다는 건 "사용자가
--              안 올렸다"가 아니라 **"올렸는데 인식하지 못했다"**는 뜻이다(분류 실패
--              또는 마스킹 잔류 등으로 저장 자체가 안 된 경우). 사용자에게 필요한
--              안내도 "다시 업로드하세요"가 아니라 "해당 문서를 다시 촬영해 주세요"다.
--   published  청구 대표 ReportJob을 발행했다.
--
-- 로컬 PG·RDS에 **같은 SQL**을 적용해 스키마 드리프트를 차단한다(이슈 #19).

CREATE TABLE IF NOT EXISTS ai.claim_readiness (
    claim_id           text PRIMARY KEY,       -- USER_CLAIMS.id(OcrJob.claim_id) — fan-in 키
    report_id          uuid NOT NULL,          -- REPORTS.id(Spring이 청구 단위로 부여) — 발행 멱등 키
    doc_total          int  NOT NULL,          -- 청구 총 문서 수(OcrJob.doc_total)
    -- 종결이 보고된 문서의 job_id 집합(중복 보고는 무시된다 — 위 설명 참고).
    terminal_job_ids   text[] NOT NULL DEFAULT '{}',
    -- 종결 문서 수. 집합의 크기라 손으로 증가시킬 수 없다(과다 카운트 구조적 차단).
    docs_terminal      int GENERATED ALWAYS AS (cardinality(terminal_job_ids)) STORED,
    missing_doc_types  text[] NOT NULL DEFAULT '{}',  -- blocked일 때 인식되지 않은 필수 유형
    status             text NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending','blocked','published')),
    judged_at          timestamptz,            -- blocked/published 판정 시각(전이 추적)
    created_at         timestamptz NOT NULL DEFAULT now()
);

-- pgaudit 대상 role(RDS 고정 이름 rds_pgaudit)에 신규 테이블 등록 — DB_ENCRYPTION.md §3 체크리스트.
-- 이 테이블에는 PII가 없지만(내부 식별자·집계 수치뿐), claim_id로 사용자의 어떤 청구가
-- 어떤 사유로 막혔는지를 이어 붙일 수 있는 조회 경로다 — 007·008과 같은 기준으로 감사
-- 대상에 넣는다. 로컬 PG에는 이 role이 없으므로(RDS 전용) 존재할 때만 GRANT한다.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_pgaudit') THEN
        EXECUTE 'GRANT SELECT ON ai.claim_readiness TO rds_pgaudit';
    END IF;
END
$$;
