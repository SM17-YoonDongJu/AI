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


def test_left_right_paired_regions_sum() -> None:
    # 총칙 2항: 좌·우의 팔은 각각 다른 신체부위 → 합산 (과소산정 회귀 방지)
    r = combine_disability_rate(
        [
            {"body_region": "팔", "laterality": "left", "rate": 30},
            {"body_region": "팔", "laterality": "right", "rate": 30},
        ]
    )
    assert r["combined_rate"] == 60.0


def test_same_side_paired_region_absorbs_max() -> None:
    # 같은 쪽 팔의 여러 장해는 여전히 최고값만
    r = combine_disability_rate(
        [
            {"body_region": "팔", "laterality": "left", "rate": 10},
            {"body_region": "팔", "laterality": "left", "rate": 30},
        ]
    )
    assert r["combined_rate"] == 30.0


def test_unknown_laterality_stays_conservative() -> None:
    # 좌우 미상(none)은 같은 부위로 취급 — 근거 없이 합산해 과대산정하지 않는다
    r = combine_disability_rate(
        [
            {"body_region": "팔", "laterality": "none", "rate": 30},
            {"body_region": "팔", "rate": 20},
        ]
    )
    assert r["combined_rate"] == 30.0


def test_laterality_ignored_for_unpaired_region() -> None:
    # 척추는 좌우 구분 부위가 아니므로 laterality가 있어도 같은 부위
    r = combine_disability_rate(
        [
            {"body_region": "척추", "laterality": "left", "rate": 20},
            {"body_region": "척추", "laterality": "right", "rate": 30},
        ]
    )
    assert r["combined_rate"] == 30.0
