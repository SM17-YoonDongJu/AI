"""우선순위 점수 계산 (이슈 #35 P2 — P1 seam 확장).

``compute(source, file)``는 약관 문서의 우선순위 큐 정렬 점수를 낸다. 성분은 네 가지다:

1. **카테고리 base**: 약관(terms)을 다른 코퍼스보다 항상 앞세우는 지배적 불변식.
2. **출처 tier base**: 카탈로그의 P0~P3(신뢰·중요도) 등급.
3. **수요도(demand)**: 상품종류(product_type) 룩업 * ``corpus_w_demand``.
4. **긴급도(urgency)**: 시행일(effective_date) 신선도 반감기 * ``corpus_w_urgency``.

P3(최근 수요 부스트)는 범위 밖이라 ``demand_boost``는 0으로 고정한 seam만 남긴다.
네트워크·DB에 의존하지 않는 **순수 함수**라 ``now`` 주입으로 결정적 단위테스트가 된다.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from core.config import Settings, get_settings

if TYPE_CHECKING:
    from corpus_worker.repository import FileRecord, SourceRecord

# 카테고리 base — 약관을 타 코퍼스보다 항상 앞세운다(지배항). 확장 카테고리는 0에서 시작.
CATEGORY_BASE: dict[str, float] = {
    "terms": 100000.0,
    "precedent": 0.0,
    "medical": 0.0,
    "legal": 0.0,
}
DEFAULT_CATEGORY_BASE: float = 0.0

# 출처 우선순위 tier → 기본 점수. 카탈로그의 P0~P3에 대응한다.
TIER_BASE_SCORES: dict[str, float] = {
    "P0": 3000.0,
    "P1": 2000.0,
    "P2": 1000.0,
    "P3": 0.0,
}
# 출처가 없거나 priority_tier가 비면 P2(1000)로 취급한다.
DEFAULT_TIER_SCORE: float = 1000.0

# 상품종류(product_type) → 수요도. 신체손해(실손·질병·상해)를 상위로 둔다.
PRODUCT_TYPE_DEMAND: dict[str, float] = {
    "실손의료": 100.0,
    "질병보험": 95.0,
    "상해보험": 95.0,
    "장해분류표": 90.0,
    "표준약관": 80.0,
    "생명보험": 70.0,
    "생활보험": 60.0,
    "자동차보험": 50.0,
    "법령별표": 45.0,
    "화재보험": 35.0,
    "기타": 30.0,
    "연금": 25.0,
}
# 미지의 상품종류·미지정은 중립 수요로 둔다.
DEFAULT_DEMAND: float = 30.0

# 시행일 신선도 반감기(년)와 시행일 없음 시 중립 긴급도.
URGENCY_HALFLIFE_YEARS: float = 5.0
DEFAULT_URGENCY: float = 50.0
# 신선도 만점 기준(시행일 == now).
_URGENCY_PEAK: float = 100.0
_DAYS_PER_YEAR: float = 365.25

# P3 자리표시 — 최근 수요 부스트(반감기·상한)는 범위 밖이라 0 고정.
DEMAND_BOOST_COMPONENT: float = 0.0


def _category_base(category: str | None) -> float:
    """카테고리 지배항 점수(미지 카테고리는 ``DEFAULT_CATEGORY_BASE``)."""
    if category is None:
        return DEFAULT_CATEGORY_BASE
    return CATEGORY_BASE.get(category, DEFAULT_CATEGORY_BASE)


def _tier_base(source: SourceRecord | None) -> float:
    """출처 tier 점수(출처·tier가 없으면 P2 취급)."""
    tier = None if source is None else source.priority_tier
    if tier is None:
        return DEFAULT_TIER_SCORE
    return TIER_BASE_SCORES.get(tier, DEFAULT_TIER_SCORE)


def _demand(product_type: str | None) -> float:
    """상품종류 룩업 수요도(미지·미지정은 ``DEFAULT_DEMAND``)."""
    if product_type is None:
        return DEFAULT_DEMAND
    return PRODUCT_TYPE_DEMAND.get(product_type, DEFAULT_DEMAND)


def _urgency(effective_date: date | None, now: datetime) -> float:
    """시행일 신선도 긴급도(지수 반감기).

    시행일이 없거나 미래(음수 age)면 ``DEFAULT_URGENCY``(중립)를 준다. 그 외에는
    ``100 * 0.5 ** (age_years / 반감기)``로 오래될수록 낮아진다.

    Args:
        effective_date: 약관 시행일(없으면 중립 취급).
        now: 기준 시각(tz-aware). age 계산의 결정성을 위해 주입한다.

    Returns:
        0~100 범위의 긴급도 점수.
    """
    if effective_date is None:
        return DEFAULT_URGENCY
    age_years = (now.date() - effective_date).days / _DAYS_PER_YEAR
    if age_years < 0:  # 미래 시행일 → 신선도 판단 불가 → 중립
        return DEFAULT_URGENCY
    return _URGENCY_PEAK * 0.5 ** (age_years / URGENCY_HALFLIFE_YEARS)


def compute(
    source: SourceRecord | None,
    file: FileRecord,
    *,
    settings: Settings | None = None,
    now: datetime | None = None,
) -> float:
    """약관 문서의 우선순위 점수를 계산한다(순수 함수).

    Args:
        source: 연결된 출처 카탈로그 레코드(없으면 None → P2 취급).
        file: 약관 문서 레코드(category·product_type·effective_date 입력).
        settings: 가중치(corpus_w_demand·corpus_w_urgency) 주입. None이면 ``get_settings()``.
        now: 긴급도 age 기준 시각. None이면 ``datetime.now(UTC)``.

    Returns:
        우선순위 점수(클수록 먼저 처리).
    """
    settings = settings or get_settings()
    now = now or datetime.now(UTC)
    demand = _demand(file.product_type)
    urgency = _urgency(file.effective_date, now)
    return (
        _category_base(file.category)
        + _tier_base(source)
        + settings.corpus_w_demand * demand
        + settings.corpus_w_urgency * urgency
        + DEMAND_BOOST_COMPONENT
    )
