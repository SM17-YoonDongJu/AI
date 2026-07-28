"""우선순위 점수 계산 seam (이슈 #35 P1).

``compute(source, file)``는 약관 문서의 우선순위 큐 정렬 점수를 낸다. P1은 **출처 tier
기반(tier_base)만** 반영하고, 수요도·긴급도·수요 부스트는 P2 자리표시(현재 0)로 확장점만
남긴다 — 순수 함수라 네트워크·DB 없이 단위테스트한다.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from corpus_worker.repository import FileRecord, SourceRecord

# 출처 우선순위 tier → 기본 점수. 카탈로그의 P0~P3에 대응한다.
TIER_BASE_SCORES: dict[str, float] = {
    "P0": 3000.0,
    "P1": 2000.0,
    "P2": 1000.0,
    "P3": 0.0,
}
# 출처가 없거나 priority_tier가 비면 P2(1000)로 취급한다.
DEFAULT_TIER_SCORE: float = 1000.0

# P2 자리표시(TODO): 아래 성분을 config 가중치로 확장한다 — 지금은 전부 0.
#   demand:  상품종류(product_type) 룩업 × corpus_w_demand
#   urgency: 시행일(effective_date) 신선도 × corpus_w_urgency
#   boost:   최근 수요(demand_score) 반감기(corpus_demand_halflife_days) 부스트,
#            corpus_demand_boost_cap 상한.
DEMAND_COMPONENT: float = 0.0
URGENCY_COMPONENT: float = 0.0
DEMAND_BOOST_COMPONENT: float = 0.0


def compute(source: SourceRecord | None, file: FileRecord) -> float:
    """약관 문서의 우선순위 점수를 계산한다.

    Args:
        source: 연결된 출처 카탈로그 레코드(없으면 None → P2 취급).
        file: 약관 문서 레코드(P2에서 수요도·긴급도 입력으로 사용, 현재는 미사용).

    Returns:
        우선순위 점수(클수록 먼저 처리). P1은 tier_base 값이다.
    """
    tier = None if source is None else source.priority_tier
    if tier is None:
        tier_base = DEFAULT_TIER_SCORE
    else:
        tier_base = TIER_BASE_SCORES.get(tier, DEFAULT_TIER_SCORE)
    return tier_base + DEMAND_COMPONENT + URGENCY_COMPONENT + DEMAND_BOOST_COMPONENT
