-- 011_ocr_results_app_owner_readonly.sql — ai.ocr_results 문서 단위 상세를 app_owner에 컬럼 한정 공개.
--
-- reports.status='NEEDS_REUPLOAD'(report_worker.mark_needs_reupload)는 "이 리포트에
-- 재업로드가 필요하다"까지만 알린다. 클레임에 문서가 여러 개면 Backend는 "어느
-- 첨부파일인지"를 알아야 사용자에게 구체적으로 안내할 수 있다 — 그 상세를 위해
-- attachment_id로 조회할 수 있게 연다.
--
-- masked_text/entities(내용 컬럼)는 제외한다 — 필요한 건 "어느 문서인지"뿐이라
-- 그 이상을 열면 최소 권한 원칙(§14)에 어긋난다. deploy/schema_split.sql이 명시
-- 보류했던 "app_owner → ai.ocr_results"를 컬럼 한정으로 처음 여는 사례다(YAGNI가
-- 끝나고 실제 필요가 생긴 경우).
--
-- id도 연다 — 내용은 아니지만, Backend가 이 컬럼들을 @Subselect 읽기전용 엔티티로
-- 매핑하려면 Hibernate가 식별자(@Id)로 쓸 컬럼이 있어야 한다(Backend QA 피드백).
--
-- 로컬 PG·RDS에 **같은 SQL**을 적용해 스키마 드리프트를 차단한다(이슈 #19).
--
-- 받는 롤은 접속 롤에서 끌어낸다(ai_owner → app_owner, ai_owner_prod → app_owner_prod). dev·prod
-- 롤이 같은 RDS 인스턴스에 함께 있어서, 'app_owner'를 박아 두면 prod에서 dev 롤이 권한을 받는다.

DO $$
DECLARE
    backend_role text := CASE WHEN current_user ~ '^ai_owner'
                              THEN regexp_replace(current_user, '^ai_owner', 'app_owner')
                              ELSE 'app_owner' END;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = backend_role) THEN
        EXECUTE format('GRANT SELECT (id, report_id, claim_id, attachment_id, doc_type, doc_index, ocr_quality)
                        ON ai.ocr_results TO %I', backend_role);
    END IF;
END
$$;
