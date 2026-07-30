"""골드셋 — battery.SCENARIOS에 정답 라벨을 얹고, 검색 골드 템플릿을 제공한다.

정직성 원칙: 도메인 전문 판단이 필요한 라벨(적용/누락 특약, 장해 지급률)은 **날조하지 않는다**.
입력에서 결정론적으로 확신 가능한 라벨(도메인외 차단·후유장해 검토 필요·타보험 유형·PII 마스킹·
약관없음 분기)만 채우고, 나머지는 None으로 둔다 → 채점 시 라벨 없는 항목은 건너뛴다.

특약/지급률/검색 relevant_chunk_ids는 손해사정사·실제 tempVectorDB 적재 내용으로 채워야 한다
(README "골드 채우기" 참고). 검색 후보는 `python -m benchmark.run --discover-retrieval`로 뽑는다.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from battery import SCENARIOS, SEARCH_QUERIES

__all__ = ["GOLD", "RETRIEVAL_GOLD", "SCENARIOS", "SEARCH_QUERIES", "CaseGold", "RetrievalGold"]


@dataclass(slots=True)
class CaseGold:
    """시나리오 정답 라벨. None 필드는 채점에서 제외(라벨 미보유)."""

    # 결정론적으로 확신 가능(입력에서 도출) — 기본 제공
    should_block: bool | None = None  # 입력 가드레일 도메인외 차단 여부
    requires_disability_review: bool | None = None  # 후유장해 검토 분기 진입 여부
    accident_type: str | None = None  # 사고 유형 분류 정답
    expect_terms_parse: bool | None = None  # 약관없음(런타임 파싱 스텁) 분기 여부
    pii_must_absent: list[str] = field(
        default_factory=list
    )  # 최종 masked_text에 남으면 안 되는 원문

    # 도메인 전문 라벨(TODO — 손해사정사가 채움). 채우면 자동 채점됨.
    applicable_coverages: set[str] | None = None  # 적용 가능 특약 정답 집합
    missing_coverages: set[str] | None = None  # 청구 누락 가능 특약 정답 집합
    disability_min_rate: float | None = None  # 합산 장해지급률 하한(% 허용범위)
    disability_max_rate: float | None = None  # 합산 장해지급률 상한(%)


# battery SCENARIOS 라벨(A~G) 기준. 확신 가능한 결정론 라벨만 기본 채움.
GOLD: dict[str, CaseGold] = {
    "A_약관있음_상해": CaseGold(
        should_block=False,
        # accident_type/장해검토/특약은 입력만으로 단정 불가 → 도메인 라벨 필요(None)
    ),
    "B_약관있음_골절": CaseGold(
        should_block=False,
    ),
    "C_약관없음_상품": CaseGold(
        should_block=False,
        expect_terms_parse=True,  # '존재하지않는상품XYZ' → policy_chunks 미존재 → 런타임 파싱 분기
    ),
    "D_실손": CaseGold(
        should_block=False,
        accident_type="medical_indemnity",  # 급성 충수염 복강경 + 실손 청구 → 실손형
        requires_disability_review=False,
    ),
    "E_PII포함": CaseGold(
        should_block=False,  # 경추 염좌, 도메인외 키워드 없음
        # 원문 PII가 최종 masked_text에 남으면 안 됨(guard_input 마스킹 검증)
        pii_must_absent=["860312-1948571", "010-9876-5432", "110-234-567890"],
    ),
    "F_도메인외": CaseGold(
        should_block=True,  # '비트코인'·'코인' 키워드 → 입력 가드레일 차단
    ),
    "G_장해명시": CaseGold(
        should_block=False,
        requires_disability_review=True,  # "영구 후유장해 예상, 장해지급률 평가 필요" 명시
    ),
}


@dataclass(slots=True)
class RetrievalGold:
    """검색 골드 1건. relevant_chunk_ids는 tempVectorDB 적재 내용으로 채워야 채점된다."""

    query: str
    relevant_chunk_ids: set[str] = field(
        default_factory=set
    )  # RagResult.ranked_chunks[].source_ref
    namespaces: list[str] = field(default_factory=lambda: ["terms"])
    note: str = ""


# battery SEARCH_QUERIES를 골드 템플릿으로. relevant_chunk_ids는 비어 있음(TODO) →
# 채우기 전에는 검색 정확성(Recall/MRR/nDCG) 채점이 건너뜀. --discover-retrieval로 후보 확인.
RETRIEVAL_GOLD: list[RetrievalGold] = [RetrievalGold(query=q) for q in SEARCH_QUERIES]
