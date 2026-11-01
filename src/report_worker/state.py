"""ReportState — LangGraph 그래프 전역 상태.

전부 순차 실행이므로 동시 쓰기가 없다 → reducer(Annotated[..., add]) 불필요.
키 이름은 ERD/API 출력 계약에 맞춘다(매핑은 persist 노드에서 수행).
"""

from __future__ import annotations

from typing import Any, TypedDict


class ReportState(TypedDict, total=False):
    # ── 입력 (ReportJob 패스스루) ──────────────────────────
    report_id: str
    ocr_result_id: str
    claim_id: str | None
    user_ref: str
    doc_type: str

    # ── load_context: DB 조회로 조립한 사고 컨텍스트 ───────
    # accident_type, diagnosis, offered_amount, insurer, product_name, enrolled_at, 그리고
    # 자유입력 question·description·additional_information. 자유입력은 mask_pii 마스킹본이다
    # (원문 없음, §13). 마스킹에 실패한 필드는 None이고 errors에 pii_mask_failed:<필드>가 남는다.
    case_info: dict[str, Any]
    # claim_id가 있으면 청구의 전 문서를 병합한 값이다(문서 헤더로 구분 · 엔티티는 평평 병합).
    masked_text: str  # ocr_results.masked_text
    entities: dict[str, Any]  # ocr_results.entities
    subscribed_coverages: list[str]  # 가입 특약명 (user_insurances.coverages)
    coverage_details: list[dict[str, Any]]  # 특약별 가입금액 [{name,type,amount}] (payment_calc용)

    # ── 진단/분류 ─────────────────────────────────────────
    diagnosis: dict[str, Any]  # {diagnosis, icd_codes, accident_type, requires_disability_review}

    # ── 약관/특약 분석 ────────────────────────────────────
    retrieved_clauses: list[dict[str, Any]]  # rag.search 결과 청크
    applicable_coverages: list[str]  # → reports.applicable_guarantees
    missing_coverages: list[str]  # → reports.omitted_special_contract
    coverage_analysis: dict[str, Any]  # 면책/할증 + citations

    # ── 분석 결과 ─────────────────────────────────────────
    disability_analysis: dict[str, Any]  # 장해 분기 시만 채움 (P1)
    legal_references: list[dict[str, Any]]  # 판례 → reports.basis_terms_precedents
    estimated_range: dict[str, int]  # {"min": .., "max": ..} (단정 금지)
    payment_breakdown: list[dict[str, Any]]  # 특약별 지급 산출 [{name, payout, basis}]
    payment_excluded: list[dict[str, Any]]  # 지급 제외 특약 [{name, reason}]

    # ── 최종 산출 ─────────────────────────────────────────
    sections: dict[str, str]  # 8섹션 본문
    issues: list[
        dict[str, Any]
    ]  # report_issues[] {title, description, ai_status, tags, impact_amount}
    report: str  # 통합 Markdown
    judge_failures: list[str]  # 출력 가드레일 인용검증 실패 섹션

    # ── 운영 ──────────────────────────────────────────────
    errors: list[str]  # 노드별 fallback/실패 기록 (부분결과 표기)
    # 노드 안에서 난 ai_client 예외의 종류(safe_node가 예외 타입으로 판정). 값이 있으면 그래프가
    # 다음 간선에서 멈추고 초안을 저장하지 않는다 — "llm_system_failure"(429·5xx·타임아웃·연결·
    # 설정 오류·잘림 → worker가 재전달)·"llm_refused"(거절 → BLOCKED 기록 후 즉시 종결).
    llm_failure: str
