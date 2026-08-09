"""_verify_rate_quote 결정론 백스톱 테스트 — 환각 지급률이 verified로 통과하지 않는지."""

import re

from report_worker.nodes.agents import _verify_rate_quote

_SCHED_TEXT = """제12조(보험금의 지급)
한 팔의 3대관절 중 1관절의 기능에 심한 장해를 남긴 때  20
두 눈이 멀었을 때  100
평형기능에 장해를 남긴 때  10
약관을 12개월 이내 갱신하는 경우 120일의 유예기간을 둔다
추상(추한 모습) 장해를 남긴 때  12.5
"""
_SCHED_NORM = re.sub(r"\s+", "", _SCHED_TEXT)


def test_real_quote_with_rate_verifies() -> None:
    quote = "한 팔의 3대관절 중 1관절의 기능에 심한 장해를 남긴 때  20"
    assert _verify_rate_quote(quote, 20.0, _SCHED_NORM)


def test_whitespace_differences_are_ignored() -> None:
    quote = "한 팔의 3대관절 중\n1관절의 기능에 심한 장해를 남긴 때 20"
    assert _verify_rate_quote(quote, 20.0, _SCHED_NORM)


def test_fabricated_quote_rejected() -> None:
    # 원문에 없는 인용문은 지급률 숫자가 그럴듯해도 거부 (환각 통과 회귀 방지)
    quote = "한 팔을 잃었을 때 지급률 20"
    assert not _verify_rate_quote(quote, 20.0, _SCHED_NORM)


def test_rate_must_appear_in_quote_not_just_document() -> None:
    # 인용문은 실재하지만 지급률 숫자가 그 안에 없으면 거부 —
    # 과거엔 원문 전체에서 "12"를 찾아 "제12조"·"12개월"에 오매칭됐다
    quote = "평형기능에 장해를 남긴 때"
    assert not _verify_rate_quote(quote, 12.0, _SCHED_NORM)


def test_decimal_rate_preserved() -> None:
    # 12.5%가 int로 뭉개져 "12"로 검색되던 회귀 방지 — 소수 그대로 대조
    quote = "추상(추한 모습) 장해를 남긴 때  12.5"
    assert _verify_rate_quote(quote, 12.5, _SCHED_NORM)


def test_empty_quote_rejected() -> None:
    assert not _verify_rate_quote("", 20.0, _SCHED_NORM)
