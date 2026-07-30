"""priority.compute 우선순위 점수 테스트 (이슈 #35 P2).

순수 함수라 네트워크·DB 없이 검증한다. 가중치(settings)와 기준시각(now)을 주입해
결정적으로 만든다: 카테고리 base 지배·tier·수요도 룩업·긴급도 반감기·default 분기.
"""

from datetime import UTC, date, datetime

from core.config import Settings
from corpus_worker import priority
from corpus_worker.repository import FileRecord, SourceRecord

# age 계산 기준 고정 시각(결정성).
NOW = datetime(2025, 1, 1, tzinfo=UTC)


def _settings() -> Settings:
    return Settings(corpus_w_demand=0.6, corpus_w_urgency=0.4)


def _source(tier: str | None) -> SourceRecord:
    return SourceRecord(notion_page_id="s", name="출처", priority_tier=tier)


def _file(
    *,
    category: str = "terms",
    product_type: str | None = None,
    effective_date: date | None = None,
) -> FileRecord:
    return FileRecord(
        notion_page_id="f",
        category=category,
        product_type=product_type,
        effective_date=effective_date,
    )


def _score(source: SourceRecord | None, file: FileRecord) -> float:
    return priority.compute(source, file, settings=_settings(), now=NOW)


# --------------------------------------------------------------------------- #
# 카테고리 지배 불변식
# --------------------------------------------------------------------------- #


def test_terms_category_dominates_other_categories() -> None:
    # Arrange: 약관(P3, 최저 tier) vs 판례(P0, 최고 tier)
    terms = _score(_source("P3"), _file(category="terms"))
    precedent = _score(_source("P0"), _file(category="precedent"))

    # Assert: 약관은 최고 tier의 타 카테고리보다도 항상 앞선다(지배항)
    assert terms > precedent


def test_unknown_category_uses_default_base() -> None:
    # Arrange / Act
    unknown = _score(_source("P2"), _file(category="우주법"))
    precedent = _score(_source("P2"), _file(category="precedent"))

    # Assert: 미지 카테고리·미지정은 DEFAULT_CATEGORY_BASE(0)로 precedent와 같은 base
    assert unknown == precedent


# --------------------------------------------------------------------------- #
# tier base
# --------------------------------------------------------------------------- #


def test_tier_base_orders_p0_to_p3() -> None:
    # Arrange / Act: 카테고리·수요·긴급 고정, tier만 변화
    p0 = _score(_source("P0"), _file())
    p1 = _score(_source("P1"), _file())
    p2 = _score(_source("P2"), _file())
    p3 = _score(_source("P3"), _file())

    # Assert
    assert p0 > p1 > p2 > p3


def test_none_and_unknown_source_default_to_p2() -> None:
    # Arrange / Act
    p2 = _score(_source("P2"), _file())
    none_source = _score(None, _file())
    none_tier = _score(_source(None), _file())
    unknown_tier = _score(_source("P9"), _file())

    # Assert: 출처 없음·tier 없음·계약 밖 tier는 모두 P2로 취급
    assert none_source == none_tier == unknown_tier == p2


# --------------------------------------------------------------------------- #
# 수요도(상품종류 룩업)
# --------------------------------------------------------------------------- #


def test_body_injury_product_types_rank_above_annuity() -> None:
    # Arrange / Act: 신체손해(실손·질병·상해)가 연금보다 수요 상위
    silson = _score(_source("P2"), _file(product_type="실손의료"))
    disease = _score(_source("P2"), _file(product_type="질병보험"))
    injury = _score(_source("P2"), _file(product_type="상해보험"))
    annuity = _score(_source("P2"), _file(product_type="연금"))

    # Assert
    assert silson > annuity
    assert disease > annuity
    assert injury > annuity


def test_unknown_product_type_uses_default_demand() -> None:
    # Arrange / Act
    unknown = _score(_source("P2"), _file(product_type="우주보험"))
    unset = _score(_source("P2"), _file(product_type=None))

    # Assert: 미지·미지정 상품종류는 DEFAULT_DEMAND(중립)
    assert unknown == unset


# --------------------------------------------------------------------------- #
# 긴급도(시행일 신선도 반감기)
# --------------------------------------------------------------------------- #


def test_urgency_decays_with_effective_date_age() -> None:
    # Arrange / Act: 최근 시행일이 오래된 시행일보다 긴급도 높음
    recent = _score(_source("P2"), _file(effective_date=date(2025, 1, 1)))
    old = _score(_source("P2"), _file(effective_date=date(2010, 1, 1)))

    # Assert
    assert recent > old


def test_missing_and_future_effective_date_use_default_urgency() -> None:
    # Arrange / Act: 시행일 없음·미래 시행일 모두 중립(DEFAULT_URGENCY)
    none_date = _score(_source("P2"), _file(effective_date=None))
    future = _score(_source("P2"), _file(effective_date=date(2999, 1, 1)))

    # Assert
    assert none_date == future


# --------------------------------------------------------------------------- #
# 합성 — 전 성분 합
# --------------------------------------------------------------------------- #


def test_composite_sums_category_tier_demand_urgency() -> None:
    # Arrange: 시행일=now → 긴급도 100(신선도 만점)
    settings = _settings()
    file = _file(category="terms", product_type="표준약관", effective_date=NOW.date())

    # Act
    score = priority.compute(_source("P1"), file, settings=settings, now=NOW)

    # Assert: 각 성분의 가중합(compute와 동일 연산 순서로 기대값 구성)
    expected = (
        priority.CATEGORY_BASE["terms"]
        + priority.TIER_BASE_SCORES["P1"]
        + settings.corpus_w_demand * priority.PRODUCT_TYPE_DEMAND["표준약관"]
        + settings.corpus_w_urgency * 100.0
    )
    assert score == expected


def test_demand_boost_seam_is_zero() -> None:
    # Assert: P3(수요 부스트) seam은 0 고정(범위 밖)
    assert priority.DEMAND_BOOST_COMPONENT == 0.0
