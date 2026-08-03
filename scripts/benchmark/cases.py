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


# 검색 골드 — retrieval_eval.py --discover 후보에서 정답 source_ref를 채운 1차 라벨.
# 본문 머리로 판단한 provisional 라벨이며 손해사정사 검수로 정련해야 한다(특히 면책/수술 범위).
# 코퍼스(현대해상 상해보험 4종)에 답이 없는 쿼리(예: 암 진단비)는 빈 집합 → 채점에서 스킵된다.
_M = "무배당현대해상마음플러스___상해종합보험_Hi2601__20260101_e0b4dc0d.pdf"
_B = "보통상해보험_20260101_e041e665.pdf"
_H = "무배당현대해상행복가득생활보장보험_Hi2601_ 1종_연만기__20260101_27c5aa4d.pdf"

RETRIEVAL_GOLD: list[RetrievalGold] = [
    RetrievalGold(
        query="후유장해 장해등급 보험금",
        relevant_chunk_ids={
            f"{_H}#p312#t4469",  # 후유장해등급별 지급보험금표
            f"{_M}#p91#t0629",  # 후유장해보험금 지급사유(3%이상)
            f"{_B}#p38#0369",  # 보통 후유장해보험금 지급사유
            f"{_M}#p36#t0098",  # 제36조 상해후유장해
            f"{_H}#p68#t0370",  # 행복가득 후유장해보험금(20%이상)
        },
        note="후유장해등급표 + 각 상품 후유장해보험금 지급사유(1차 라벨)",
    ),
    RetrievalGold(
        query="상해 입원일당 지급 사유",
        relevant_chunk_ids={
            f"{_H}#p72#t0447",  # 상해입원급여금 지급사유 표
            f"{_H}#p32#t0101",  # 상해입원일당(1~180일)
            f"{_M}#p144#1654",  # 제1조 입원 지급사유
            f"{_M}#p135#1482",  # 제1조 상해 입원 지급사유
            f"{_B}#p43#0462",  # 보통 상해 입원 지급사유
        },
        note="상해입원급여금/입원일당 지급사유(1차 라벨)",
    ),
    RetrievalGold(
        query="보험금을 지급하지 않는 면책 사유",
        relevant_chunk_ids={
            f"{_M}#p223#3074",  # 제4조 보험금 부지급 사유(실내용)
            f"{_M}#p137#1516",  # 제5조 면책(보통약관 제6조 준용)
            f"{_H}#p116#1260",  # 제4조 면책
        },
        note="부지급(면책) 조항 — 참조체가 많아 실내용 위주(검수 필요)",
    ),
    RetrievalGold(
        query="수술비 특약 보장 범위",
        relevant_chunk_ids={
            f"{_M}#p187#2459",  # 제1조 상해수술 지급사유
            f"{_M}#p171#2146",  # 제7조 '수술' 정의
            f"{_M}#p164#2016",  # 검사/진단 수술
            f"{_H}#p85#0691",  # 제3조 '수술' 정의
        },
        note="상해수술 지급사유 및 '수술' 정의(1차 라벨)",
    ),
    RetrievalGold(
        query="골절 진단비 지급 조건",
        relevant_chunk_ids={
            f"{_M}#p123#t1269",  # 골절진단보장 특약 지급사유 표
            f"{_B}#p45#0493",  # 보통 골절 진단확정시 지급
            f"{_B}#p45#0495",  # 보통 골절 진단확정시(치아파절 제외)
            f"{_M}#p126#1319",  # 제1조 골절 진단확정 지급사유
            f"{_H}#p91#0794",  # 행복가득 골절 진단확정 지급사유
        },
        note="골절진단보험금 지급사유(1차 라벨)",
    ),
    RetrievalGold(
        query="암 진단 확정시 보험금",
        relevant_chunk_ids=set(),  # 상해보험 코퍼스라 암 진단비 담보 없음 → 정답 부재(스킵)
        note="이 코퍼스(상해보험 4종)엔 암 담보가 없어 정답 청크 부재 → 미라벨로 스킵",
    ),
    RetrievalGold(
        query="교통사고 변호사 선임비용",
        relevant_chunk_ids={
            f"{_M}#p285#4047",  # 변호사선임비용 1사고 5백만원 한도
            f"{_M}#p283#4003",  # 일반교통사고/변호사선임비용 정의
            f"{_M}#p51#t0205",  # 자동차사고변호사선임비용 표
            f"{_M}#p285#4044",  # 제1조 지급사유
        },
        note="자동차사고 변호사선임비용 지급사유/정의(1차 라벨)",
    ),
    RetrievalGold(
        query="보험료 납입 면제 사유",
        relevant_chunk_ids={
            f"{_M}#p62#0265",  # 제3조 보험료 납입면제 사유
            f"{_M}#p65#0313",  # 제5조 납입면제 사유 언급
            f"{_M}#p64#0309",  # 제5조 납입면제 사유
        },
        note="보험료 납입면제 사유 조항(1차 라벨)",
    ),
]
