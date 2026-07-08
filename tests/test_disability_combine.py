"""combine_disability_rate 순수 규칙 테스트 (LLM/DB 없음)."""

from report_worker.disability_rules import combine_disability_rate


def test_different_regions_sum() -> None:
    # Arrange / Act
    r = combine_disability_rate(
        [{"body_region": "팔", "rate": 10}, {"body_region": "다리", "rate": 20}]
    )
    # Assert
    assert r["combined_rate"] == 30.0
    assert any("합산" in n for n in r["rule_notes"])


def test_same_region_absorbs_max() -> None:
    r = combine_disability_rate(
        [{"body_region": "팔", "rate": 10}, {"body_region": "팔", "rate": 20}]
    )
    assert r["combined_rate"] == 20.0
    assert any("동일부위" in n for n in r["rule_notes"])


def test_cap_at_100() -> None:
    r = combine_disability_rate(
        [{"body_region": "팔", "rate": 60}, {"body_region": "다리", "rate": 70}]
    )
    assert r["combined_rate"] == 100.0
    assert any("상한" in n for n in r["rule_notes"])


def test_temporary_5years_reduced_to_20pct() -> None:
    r = combine_disability_rate(
        [{"body_region": "다리", "rate": 50, "temporary": True, "temporary_years": 5}]
    )
    assert r["combined_rate"] == 10.0


def test_temporary_under_5years_excluded() -> None:
    r = combine_disability_rate(
        [{"body_region": "다리", "rate": 50, "temporary": True, "temporary_years": 3}]
    )
    assert r["combined_rate"] == 0.0


def test_empty_items() -> None:
    r = combine_disability_rate([])
    assert r["combined_rate"] == 0.0
    assert r["rule_notes"] == ["산입 항목 없음"]
