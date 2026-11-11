"""장해지급률 결정론 합산 (MVP).

LLM이 약관 장해분류표에서 추출·검증한 항목(items)을 받아 순수 규칙으로 최종 장해지급률을
계산한다. LLM·DB·IO가 전혀 없어 재현·감사·단위테스트가 가능하다("분류는 LLM, 합산은 결정론").

MVP 규칙:
  1. 한시장해 — 존속기간 5년 이상이면 지급률의 20%만 인정, 5년 미만/미상은 미산입(0%).
  2. 동일 신체부위(body_region)의 여러 장해는 합산하지 않고 가장 높은 지급률만 인정(파생·중복 방지).
     단, 총칙 2항에 따라 좌·우의 눈·귀·팔·다리·손가락·발가락은 각각 다른 신체부위로 본다
     (laterality가 left/right로 확인된 경우만 분리 — 미상(none)은 보수적으로 같은 부위 취급).
  3. 서로 다른 신체부위끼리는 합산한다.
  4. 합산 상한 100%.
정밀 규칙(기존장해 공제·파생장해 세부·부위별 상한)은 후속 단계.
"""

from __future__ import annotations

from typing import Any

_TEMPORARY_MIN_YEARS = 5.0
_TEMPORARY_FACTOR = 0.20
_MAX_RATE = 100.0

# 총칙 2항: 좌·우를 각각 다른 신체부위로 보는 6개 부위.
_PAIRED_REGIONS = frozenset({"눈", "귀", "팔", "다리", "손가락", "발가락"})


def _region_key(item: dict[str, Any]) -> str:
    """그룹핑 키. 좌우 구분 부위는 확인된 laterality를 붙여 별개 부위로 분리한다."""
    region = str(item.get("body_region") or "기타")
    laterality = str(item.get("laterality") or "none")
    if region in _PAIRED_REGIONS and laterality in ("left", "right"):
        return f"{region}:{laterality}"
    return region


def _effective_rate(item: dict[str, Any]) -> tuple[float, str | None]:
    """한시장해 환산을 적용한 유효 지급률과 (있으면) 규칙 로그를 반환한다."""
    rate = float(item.get("rate") or 0.0)
    label = str(item.get("injury") or item.get("category_label") or "장해")
    if not item.get("temporary"):
        return rate, None
    years = item.get("temporary_years")
    if years is not None and float(years) >= _TEMPORARY_MIN_YEARS:
        eff = round(rate * _TEMPORARY_FACTOR, 2)
        return eff, f"한시장해 5년이상 → 20% 인정: {label} {rate}%→{eff}%"
    return 0.0, f"한시장해 5년미만/미상 → 미산입: {label} {rate}%"


def combine_disability_rate(items: list[dict[str, Any]]) -> dict[str, Any]:
    """검증된 장해 항목들을 규칙에 따라 합산해 최종 장해지급률(%)을 낸다.

    Args:
        items: 각 원소는 최소 `body_region`, `rate`(%)를 갖고, 선택적으로
            `laterality`("left"|"right"|"none" — 좌우 구분 부위의 별개 취급),
            `temporary`(bool), `temporary_years`(number|None),
            `injury`/`category_label`(로그용)을 갖는다.

    Returns:
        {"combined_rate": float, "rule_notes": list[str], "normalized_items": list[dict]}
    """
    if not items:
        return {"combined_rate": 0.0, "rule_notes": ["산입 항목 없음"], "normalized_items": []}

    rule_notes: list[str] = []

    # 1. 한시장해 환산 → effective_rate
    normalized: list[dict[str, Any]] = []
    for it in items:
        eff, note = _effective_rate(it)
        if note:
            rule_notes.append(note)
        normalized.append({**it, "effective_rate": eff, "region": _region_key(it)})

    # 2. 동일 신체부위는 합산하지 않고 최고 지급률만 인정
    by_region: dict[str, list[dict[str, Any]]] = {}
    for it in normalized:
        by_region.setdefault(it["region"], []).append(it)
    region_best: dict[str, dict[str, Any]] = {}
    for region, group in by_region.items():
        best = max(group, key=lambda x: x["effective_rate"])
        region_best[region] = best
        if len(group) > 1:
            top = best["effective_rate"]
            rule_notes.append(f"동일부위({region}) {len(group)}건 중 최고 {top}%만 인정")

    # 3. 서로 다른 부위 합산
    combined = round(sum(it["effective_rate"] for it in region_best.values()), 2)
    if len(region_best) > 1:
        rule_notes.append(f"서로 다른 {len(region_best)}개 부위 합산 = {combined}%")

    # 4. 상한 100%
    if combined > _MAX_RATE:
        rule_notes.append(f"상한 100% 적용({combined}% → 100%)")
        combined = _MAX_RATE

    return {"combined_rate": combined, "rule_notes": rule_notes, "normalized_items": normalized}
