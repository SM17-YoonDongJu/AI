# langGraph — 리포트 멀티에이전트 (이슈 #11 실험 공간)

`report-job` Kafka 메시지를 소비해 LangGraph 멀티에이전트로 리포트 초안을 생성하는 워커의
**실험/프로토타입 공간**. 안정화되면 AI 레포 `src/report_worker/`로 이관한다.

## 확정 계약 (코드 우선 — Notion/다이어그램은 참고)

- **입력**: `core.contracts.ReportJob` (얇음) — `report_id, ocr_result_id, job_id, doc_type, user_ref, claim_id, created_at`
  - 사고 정보는 메시지에 없음 → DB 조회로 조립: `ocr_results`(masked_text·entities), `user_claims`(claim_id), `reports`
- **출력**: `report_drafts.draft(jsonb)` 저장 + `reports`(applicable_guarantees·omitted_special_contract·basis_terms_precedents·claimed_min/max) + `report_issues[]`
- **토픽**: `report-job` (ocr_worker → report_worker). DLQ `report-job.dlq`
- **LLM**: 모델 미정 → env 주입(`ai_client`). 하드코딩 금지
- **임베딩 정합**: 쿼리 임베딩 모델 == Policy-Chunker 적재 모델 (qwen3:embedding 1024d 또는 BGE-M3)

## 그래프 (순차 7노드 + 장해 분기)

```
START
 → load_context        # DB 조회로 사고/약관 컨텍스트 조립
 → input_guardrail     # PII 마스킹·도메인 외 차단
 → diagnosis           # 진단/사고 분류, requires_disability_review 판정
 → [disability?] ─yes→ terms_parse(약관 파싱)
 → coverage_parse      # 가입 특약 추출 (subscribed_coverages)
 → coverage_analysis   # Hybrid RAG → applicable/missing + 인용
 → case_search         # 판례 검색 → legal_references
 → payment_calc        # estimated_range (단정 금지)
 → report_compose      # 8섹션 + issues[] (생성 가드레일)
 → output_guardrail    # 고지문 삽입 + LLM Judge(인용검증)
 → persist             # report_drafts/reports/report_issues
 → END
```

전부 순차 → state reducer 불필요. 노드 실패 1회 재시도 후 부분결과+실패섹션 표기.

## 구조

```
langGraph/
  state.py          # ReportState (TypedDict)
  graph.py          # StateGraph 조립 + 조건분기
  nodes/            # 노드별 함수
  mocks/            # rag.search / guardrail.* / ai_client.* 시그니처 stub (8·9 미구현)
  run_local.py      # Kafka 없이 단건 dict로 그래프 돌려보는 실험 진입점
```

## 의존 상태

| 모듈 | 상태 | 처리 |
|---|---|---|
| `rag.search()` | README만 (미구현) | mock |
| `guardrail.*` 3단 | README만 (미구현) | mock |
| `core.ai_client` | 미구현 | mock |
| `policy_chunks` DB | tempVectorDB + Policy-Chunker 적재 필요 | Track A |

## 실험 전제 (Track A)

`rag.search()` 실측하려면 `../tempVectorDB` 기동 + 메리츠 약관 임베딩 적재 선행. README 참조.
