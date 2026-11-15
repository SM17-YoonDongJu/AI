"""보장 분석 ∥ 후유장해 경로(R6, agents.coverage_and_disability) — 동시 실행·결과 합치기·실패 취소.

노드 본문은 대역(safe_node로 감싼 가짜 노드)으로 바꿔 끼운다. 합친 결과는 같은 노드를 순차로
돌린 결과와 같아야 한다(노드 소요 시간·줄기 표식은 빼고 비교).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from report_worker.metrics import critical_path_ms
from report_worker.nodes import agents

_DX_DISABILITY = {"diagnosis": {"diagnosis": "요추 염좌", "requires_disability_review": True}}
_DX_PLAIN = {"diagnosis": {"diagnosis": "급성 위염", "requires_disability_review": False}}


def _chunk(ref: str) -> dict[str, Any]:
    return {"source_ref": ref, "text": ref}


# ── 가짜 노드: 실제 노드와 같은 방식으로 상태 키를 쓴다 ─────────────────
@agents.safe_node
async def fake_coverage(state: Any) -> dict[str, Any]:
    # 보장 분석: retrieved_clauses를 덮어쓰고 errors에 덧붙인다
    return {
        "retrieved_clauses": [_chunk("약관-1"), _chunk("약관-2")],
        "applicable_coverages": ["골절진단비"],
        "errors": agents._err(state, "rag_partial"),
    }


@agents.safe_node
async def fake_case(state: Any) -> dict[str, Any]:
    return {"legal_references": [{"ref": "판례-1"}], "errors": agents._err(state, "case_few")}


@agents.safe_node
async def fake_disability(state: Any) -> dict[str, Any]:
    # 장해분류표: 기존 목록에 없는 청크만 덧붙인다(disability_rag와 같은 규칙)
    existing = state.get("retrieved_clauses", [])
    seen = {c.get("source_ref") for c in existing}
    new = [_chunk("약관-2"), _chunk("별표-3")]
    return {
        "disability_analysis": {"items": [{"rate": 10}]},
        "retrieved_clauses": existing + [c for c in new if c["source_ref"] not in seen],
        "errors": [*state.get("errors", []), "disability_pending_reeval"],
    }


@agents.safe_node
async def fake_verify(state: Any) -> dict[str, Any]:
    return {"disability_analysis": {**state["disability_analysis"], "verified": True}}


def _install_lanes(
    monkeypatch: pytest.MonkeyPatch, coverage: tuple[Any, ...], disability: tuple[Any, ...]
) -> None:
    monkeypatch.setattr(agents, "_COVERAGE_LANE", coverage)
    monkeypatch.setattr(agents, "_DISABILITY_LANE", disability)


def _without_timing(state: dict[str, Any]) -> dict[str, Any]:
    """비교용 — 노드 소요 시간(ms)과 줄기 표식(group·lane)을 뺀다."""
    out = dict(state)
    out["node_trace"] = [
        {k: v for k, v in entry.items() if k not in {"ms", "group", "lane"}}
        for entry in out.get("node_trace", [])
    ]
    return out


async def _sequential(state: dict[str, Any], nodes: tuple[Any, ...]) -> dict[str, Any]:
    current = dict(state)
    for node in nodes:
        current.update(await node(current))
    return current


async def test_merged_state_matches_sequential_run(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 보장(약관 → 판례) + 장해(분류표 → 감수).
    # 둘 다 errors·node_trace·retrieved_clauses를 쓴다
    _install_lanes(monkeypatch, (fake_coverage, fake_case), (fake_disability, fake_verify))
    state: dict[str, Any] = {**_DX_DISABILITY, "errors": ["earlier"], "node_trace": []}

    # Act
    parallel = {**state, **await agents.coverage_and_disability(state)}  # type: ignore[arg-type]
    sequential = await _sequential(state, (fake_coverage, fake_case, fake_disability, fake_verify))

    # Assert: 순서·내용이 순차 실행과 같다(청크는 보장 것 뒤에 장해가 새로 더한 것만)
    assert _without_timing(parallel) == _without_timing(sequential)
    assert [c["source_ref"] for c in parallel["retrieved_clauses"]] == [
        "약관-1",
        "약관-2",
        "별표-3",
    ]
    assert parallel["errors"] == ["earlier", "rag_partial", "case_few", "disability_pending_reeval"]
    lanes = [(e["node"], e["lane"]) for e in parallel["node_trace"]]
    assert lanes == [
        ("fake_coverage", "coverage"),
        ("fake_case", "coverage"),
        ("fake_disability", "disability"),
        ("fake_verify", "disability"),
    ]


async def test_lanes_run_concurrently(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 두 줄기의 첫 노드가 서로를 기다린다 — 순차면 한쪽이 시한에 걸린다
    coverage_started = asyncio.Event()
    disability_started = asyncio.Event()
    met: dict[str, bool] = {}

    @agents.safe_node
    async def waits_for_disability(state: Any) -> dict[str, Any]:
        coverage_started.set()
        try:
            await asyncio.wait_for(disability_started.wait(), timeout=1.0)
            met["coverage"] = True
        except TimeoutError:
            met["coverage"] = False
        return {}

    @agents.safe_node
    async def waits_for_coverage(state: Any) -> dict[str, Any]:
        disability_started.set()
        try:
            await asyncio.wait_for(coverage_started.wait(), timeout=1.0)
            met["disability"] = True
        except TimeoutError:
            met["disability"] = False
        return {}

    _install_lanes(monkeypatch, (waits_for_disability,), (waits_for_coverage,))

    # Act
    await agents.coverage_and_disability({**_DX_DISABILITY})  # type: ignore[arg-type]

    # Assert
    assert met == {"coverage": True, "disability": True}


async def test_disability_lane_skipped_when_review_not_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 진단이 후유장해 검토 대상이 아니다(route_after_case — 진단만 본다)
    called: list[str] = []

    @agents.safe_node
    async def disability_spy(state: Any) -> dict[str, Any]:
        called.append("disability")
        return {}

    _install_lanes(monkeypatch, (fake_coverage, fake_case), (disability_spy,))

    # Act
    out = await agents.coverage_and_disability({**_DX_PLAIN})  # type: ignore[arg-type]

    # Assert
    assert called == []
    assert [e["node"] for e in out["node_trace"]] == ["fake_coverage", "fake_case"]


@pytest.mark.parametrize("failing_lane", ["coverage", "disability"])
async def test_failure_in_one_lane_cancels_the_other(
    monkeypatch: pytest.MonkeyPatch, failing_lane: str
) -> None:
    # Arrange: 한 줄기는 LLM 시스템 실패로 멈추고, 다른 줄기는 응답을 기다리는 중이다.
    # 순차였다면 부르지 않았을 호출이므로 끊어야 한다(결과를 버릴 Claude 호출이 과금되지 않게)
    cancelled = asyncio.Event()

    @agents.safe_node
    async def stuck(state: Any) -> dict[str, Any]:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return {}

    @agents.safe_node
    async def fails(state: Any) -> dict[str, Any]:
        await asyncio.sleep(0)
        raise agents.ai_client.AiClientError("chat 호출 실패")

    lanes = {"coverage": (stuck,), "disability": (stuck,)}
    lanes[failing_lane] = (fails,)
    _install_lanes(monkeypatch, lanes["coverage"], lanes["disability"])

    # Act
    out = await asyncio.wait_for(
        agents.coverage_and_disability({**_DX_DISABILITY}),  # type: ignore[arg-type]
        timeout=1.0,
    )

    # Assert: 실패가 남았고(다음 간선이 그래프를 멈춘다) 다른 줄기는 취소됐다
    assert out["llm_failure"] == agents.LLM_SYSTEM_FAILURE
    assert "fails_failed:AiClientError" in out["errors"]
    assert cancelled.is_set()
    assert [e["node"] for e in out["node_trace"]] == ["fails"]


def test_critical_path_counts_longest_lane_only() -> None:
    # Arrange: 묶음 밖 노드 + 묶음 안 두 줄기(보장 30·판례 5 = 35, 장해 20·감수 25 = 45)
    trace = [
        {"node": "diagnosis", "ms": 10},
        {"node": "coverage_analysis", "ms": 30, "group": "g", "lane": "coverage"},
        {"node": "case_search", "ms": 5, "group": "g", "lane": "coverage"},
        {"node": "disability_rag", "ms": 20, "group": "g", "lane": "disability"},
        {"node": "disability_verify", "ms": 25, "group": "g", "lane": "disability"},
        {"node": "report_compose", "ms": 40},
    ]

    # Act / Assert: 10 + max(35, 45) + 40
    assert critical_path_ms(trace) == 95
