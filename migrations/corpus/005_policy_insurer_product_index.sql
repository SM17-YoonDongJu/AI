-- 005_policy_insurer_product_index.sql
-- corpus.policy_chunks (insurer, product_name) 인덱스 — 성능 실험 G3(docs/perf).
--
-- 왜: 리포트의 가입 약관 존재 조회(report_worker policy_in_db)와 RAG terms 검색은 보험사·상품명으로
--   좁힌다. 기존 idx_policy_meta는 (insurer, chunk_type, effective_date)라 상품명 조건을 못 써서
--   보험사 한 곳의 청크(dev 메리츠화재 38만 행)를 힙에서 다 읽는다 — 테이블 14GB > RDS 메모리
--   2GB라 콜드 캐시에서 존재 조회 하나가 44초였다(리포트 221초 중 20%).
--
-- 적용 절차(중요): 264만 행 테이블이라 생성에 수 분이 걸리고 dev·prod가 같은 RDS 인스턴스를 쓴다.
--   corpus_worker 기동 시 자동 적용되면 배포 시점에 예고 없이 빌드가 돈다 — 그래서 **운영 시간을
--   피해 corpus_owner로 이 문장을 먼저 수동 실행**한다(dev brbs_dev, prod brbs_prod 각각). 그 뒤의
--   자동 적용은 IF NOT EXISTS라 아무 일도 하지 않는다.
--   CONCURRENTLY는 트랜잭션 안에서 못 돈다 — 이 파일엔 **문장 하나만** 둔다(run_migrations는 파일을
--   simple query로 실행하므로 문장이 하나면 트랜잭션 블록 밖에서 돈다).
--   빌드가 중간에 실패하면 INVALID 인덱스가 남고 IF NOT EXISTS가 그걸 건너뛴다 —
--   `SELECT indexrelid::regclass, indisvalid FROM pg_index WHERE indexrelid = 'corpus.idx_policy_insurer_product'::regclass`
--   로 확인하고, false면 DROP INDEX CONCURRENTLY 후 다시 만든다.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_policy_insurer_product
    ON corpus.policy_chunks (insurer, product_name);
