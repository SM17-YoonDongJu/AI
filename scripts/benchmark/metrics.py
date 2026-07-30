"""채점·집계 — 순수 함수. 최종 상태(final_state)에서 정확도·근거성 신호를 뽑고 속도를 집계한다.

정확도 신호는 파이프라인이 이미 계산해 둔 결정론 백스톱을 최대한 활용한다:
  - verified_rate: 지급률 숫자가 인용 원문에 실제 존재할 때만 verified=True(disability_rag)
  - judge_failures: 출력 가드레일 LLM Judge가 근거 없는 주장으로 판정한 문장 수
  - json 실패: errors[]의 "*_llm_failed" 마커(구조화 출력 신뢰도)
  - 금액 단정 치환: guard_generation이 단정 표현을 "참고 추정 범위(약 …"로 바꾼 횟수
라벨 기반 정확도(사고유형·특약 F1 등)는 골드가 있을 때만 계산한다.
"""

from __future__ import annotations

from typing import Any

from benchmark.cases import CaseGold

# guard_generation이 단정 금액을 치환할 때 삽입하는 고정 접두(치환 횟수 카운트용, guards.py와 일치).
_AMOUNT_GUARD_MARK = "참고 추정 범위(약 "


def percentile(values: list[float], p: float) -> float:
    """정렬 후 선형보간 백분위수(numpy 비의존). p는 0~100."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (p / 100.0) * (len(ordered) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return ordered[lo] + (ordered[hi] - ordered[lo]) * frac


def set_prf(pred: set[str], gold: set[str]) -> dict[str, float]:
    """집합 예측의 precision/recall/f1."""
    if not gold and not pred:
        return {"precision": 1.0, "recall": 1.0, "f1": 1.0}
    tp = len(pred & gold)
    precision = tp / len(pred) if pred else 0.0
    recall = tp / len(gold) if gold else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1}


def _norm_set(values: Any) -> set[str]:
    """리스트/None을 정규화된 문자열 집합으로."""
    if not values:
        return set()
    return {str(v).strip() for v in values if str(v).strip()}


def verified_rate(final_state: dict[str, Any]) -> float | None:
    """장해 추출 grounding = verified 항목 / 전체 추출 항목. 장해 분기 없으면 None."""
    da = final_state.get("disability_analysis") or {}
    items = da.get("items") or []
    if not items:
        return None
    verified = sum(1 for it in items if it.get("verified"))
    return verified / len(items)


def score_case(final_state: dict[str, Any], gold: CaseGold, *, draft_saved: bool) -> dict[str, Any]:
    """한 시나리오 실행 결과를 채점한다. 라벨 없는 항목은 결과에서 생략(None).

    Args:
        final_state: 그래프 최종 상태.
        gold: 시나리오 정답 라벨.
        draft_saved: report_drafts에 저장됐는지(runner가 DB 조회로 전달).

    Returns:
        지표명 → 값. 항상 있는 신호(근거성·JSON·금액·저장)와 라벨 기반 정확도가 섞여 있다.
    """
    errors: list[str] = list(final_state.get("errors", []))
    diagnosis = final_state.get("diagnosis") or {}
    report = final_state.get("report", "") or ""

    blocked = any(str(e).startswith("input_blocked") for e in errors)
    terms_parse = any("runtime_parse_stub" in str(e) for e in errors)
    json_failures = sum(1 for e in errors if "_llm_failed" in str(e))
    rag_empty = any("rag_empty" in str(e) for e in errors)

    out: dict[str, Any] = {
        # 항상 계산 가능한 신호
        "draft_saved": draft_saved,
        "blocked": blocked,
        "terms_parse_branch": terms_parse,
        "json_failures": json_failures,
        "rag_empty": rag_empty,
        "judge_failures": len(final_state.get("judge_failures", []) or []),
        "amount_assertions_caught": report.count(_AMOUNT_GUARD_MARK),
        "n_issues": len(final_state.get("issues", []) or []),
        "report_chars": len(report),
        "errors": errors,
    }

    vr = verified_rate(final_state)
    if vr is not None:
        out["verified_rate"] = vr
        out["disability_combined_rate"] = (final_state.get("disability_analysis") or {}).get(
            "combined_rate"
        )
        out["disability_confidence"] = (final_state.get("disability_analysis") or {}).get(
            "confidence"
        )

    # ── 라벨 기반 정확도(골드 있을 때만) ──────────────────────────
    if gold.should_block is not None:
        out["block_correct"] = blocked == gold.should_block
    if gold.expect_terms_parse is not None:
        out["terms_parse_correct"] = terms_parse == gold.expect_terms_parse
    if gold.requires_disability_review is not None:
        out["disability_route_correct"] = (
            bool(diagnosis.get("requires_disability_review")) == gold.requires_disability_review
        )
    if gold.accident_type is not None:
        out["accident_type_correct"] = diagnosis.get("accident_type") == gold.accident_type
    if gold.pii_must_absent:
        masked = final_state.get("masked_text", "") or ""
        leaked = [p for p in gold.pii_must_absent if p in masked]
        out["pii_leaked"] = leaked
        out["pii_ok"] = not leaked
    if gold.applicable_coverages is not None:
        out["applicable_f1"] = set_prf(
            _norm_set(final_state.get("applicable_coverages")), gold.applicable_coverages
        )["f1"]
    if gold.missing_coverages is not None:
        out["missing_f1"] = set_prf(
            _norm_set(final_state.get("missing_coverages")), gold.missing_coverages
        )["f1"]
    if gold.disability_min_rate is not None and gold.disability_max_rate is not None:
        rate = float((final_state.get("disability_analysis") or {}).get("combined_rate") or 0.0)
        out["disability_rate_in_range"] = (
            gold.disability_min_rate <= rate <= gold.disability_max_rate
        )

    return out


def aggregate_speed(e2e_seconds: list[float], per_call_latency: list[float]) -> dict[str, float]:
    """e2e·호출 지연을 백분위로 집계한다."""
    return {
        "e2e_p50_s": percentile(e2e_seconds, 50),
        "e2e_p90_s": percentile(e2e_seconds, 90),
        "e2e_p95_s": percentile(e2e_seconds, 95),
        "e2e_mean_s": sum(e2e_seconds) / len(e2e_seconds) if e2e_seconds else 0.0,
        "call_p50_s": percentile(per_call_latency, 50),
        "call_p90_s": percentile(per_call_latency, 90),
        "n_runs": len(e2e_seconds),
        "n_calls": len(per_call_latency),
    }


def mean(values: list[float]) -> float:
    """빈 리스트는 0.0."""
    return sum(values) / len(values) if values else 0.0


def rate_of(flags: list[bool]) -> float | None:
    """불리언 비율. 라벨 없는(빈) 경우 None."""
    if not flags:
        return None
    return sum(1 for f in flags if f) / len(flags)
