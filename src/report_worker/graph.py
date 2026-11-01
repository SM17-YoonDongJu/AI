"""StateGraph 조립 — 순차 노드 + 조건 분기 + 실패 단락.

build_graph()는 compile된 그래프를 반환. 노드 함수는 nodes.agents.

종료 노드(persist·persist_blocked·persist_refused)를 뺀 **모든 간선**은 다음 노드로 가기 전에
agents.failure_route를 먼저 본다. 컨텍스트 하드 실패나 노드 안의 ai_client 실패가 기록되면
바로 다음 간선에서 멈춘다. 어느 노드가 실패했든 판정이 같으므로 LLM 노드 개수와 무관하다.
간선은 `_edge`·`_branch`로만 잇는다 — `g.add_edge`로 직결하면 그 간선에서 단락이 빠진다
(구조 테스트가 막는다).
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Hashable

from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from report_worker.nodes import agents
from report_worker.state import ReportState

type Route = Callable[[ReportState], str | Awaitable[str]]
type Graph = StateGraph[ReportState, None, ReportState, ReportState]

# 실패 단락 경로. 컨텍스트·LLM 시스템 실패는 아무것도 쓰지 않고 끝낸다(worker가 재전달).
# 거절은 상태만 기록하고 끝낸다(worker가 즉시 종결).
_FAILURE_PATHS: dict[Hashable, str] = {
    agents.CONTEXT_FAILURE: END,
    agents.LLM_SYSTEM_FAILURE: END,
    agents.LLM_REFUSED: "persist_refused",
}
# 끝에서 END로 직결하는 노드. 이 뒤에는 더 부를 노드가 없어 단락이 필요 없다.
TERMINAL_NODES = ("persist", "persist_blocked", "persist_refused")


def _stop_on_failure(route: Route) -> Callable[[ReportState], Awaitable[str]]:
    """원래 분기보다 실패 단락을 먼저 본다. 실패면 원래 분기(DB 조회 등)는 아예 안 탄다."""

    async def _route(state: ReportState) -> str:
        failed = agents.failure_route(state)
        if failed is not None:
            return failed
        target = route(state)
        return await target if inspect.isawaitable(target) else target

    return _route


def _branch(g: Graph, source: str, route: Route, path_map: dict[Hashable, str]) -> None:
    """조건 분기 간선. 실패 단락 경로를 path_map에 함께 건다."""
    g.add_conditional_edges(source, _stop_on_failure(route), {**path_map, **_FAILURE_PATHS})


def _edge(g: Graph, source: str, target: str) -> None:
    """정적 간선(source → target). 실패 단락만 조건으로 붙는다."""
    _branch(g, source, lambda _state: target, {target: target})


def build_graph() -> CompiledStateGraph[ReportState, None, ReportState, ReportState]:
    g: Graph = StateGraph(ReportState)

    g.add_node("load_context", agents.load_context)
    g.add_node("input_guardrail", agents.input_guardrail)
    g.add_node("diagnosis", agents.diagnosis)
    g.add_node("terms_parse", agents.terms_parse)
    g.add_node("coverage_parse", agents.coverage_parse)
    g.add_node("coverage_analysis", agents.coverage_analysis)
    g.add_node("case_search", agents.case_search)
    g.add_node("disability_rag", agents.disability_rag)
    g.add_node("disability_verify", agents.disability_verify)
    g.add_node("disability_calc", agents.disability_calc)
    g.add_node("payment_calc", agents.payment_calc)
    g.add_node("report_compose", agents.report_compose)
    g.add_node("output_guardrail", agents.output_guardrail)
    g.add_node("persist", agents.persist)
    g.add_node("persist_blocked", agents.persist_blocked)
    g.add_node("persist_refused", agents.persist_refused)

    g.add_edge(START, "load_context")
    # 컨텍스트 하드 실패(load_context_failed·ocr_result_missing)는 이 간선에서 끝난다.
    _edge(g, "load_context", "input_guardrail")

    # 차단(도메인외·PII정책 등) 시 LLM 파이프라인을 건너뛰고 persist_blocked로 → 비용/오출력
    # 방지. persist_blocked가 reports.status='BLOCKED'만 기록해 상태 미갱신(무한 처리중)을 막는다.
    _branch(
        g,
        "input_guardrail",
        agents.route_after_input,
        {"blocked": "persist_blocked", "diagnosis": "diagnosis"},
    )

    # 분기: 사용자 약관이 우리 DB(policy_chunks)에 있나?
    #   없음 → terms_parse(런타임 파싱 스텁) → coverage_parse
    #   있음 → coverage_parse 직행
    _branch(
        g,
        "diagnosis",
        agents.policy_in_db,
        {"terms_parse": "terms_parse", "coverage_parse": "coverage_parse"},
    )
    _edge(g, "terms_parse", "coverage_parse")

    _edge(g, "coverage_parse", "coverage_analysis")
    _edge(g, "coverage_analysis", "case_search")

    # 분기: 후유장해 검토 필요 시 장해 서브그래프(분류·지급률 → 결정론 합산) 경유,
    #   아니면 payment_calc 직행. 장해 경로는 payment_calc에서 재합류(2 incoming).
    _branch(
        g,
        "case_search",
        agents.route_after_case,
        {"disability": "disability_rag", "payment_calc": "payment_calc"},
    )
    _edge(g, "disability_rag", "disability_verify")
    _edge(g, "disability_verify", "disability_calc")
    _edge(g, "disability_calc", "payment_calc")

    _edge(g, "payment_calc", "report_compose")
    _edge(g, "report_compose", "output_guardrail")
    # 출력 가드레일의 Judge 실패는 guard_output이 JUDGE_UNAVAILABLE 표식으로 바꿔 돌려준다(예외
    # 아님) → 본문이 완성됐으므로 저장으로 간다.
    _edge(g, "output_guardrail", "persist")
    for node in TERMINAL_NODES:
        g.add_edge(node, END)

    return g.compile()
