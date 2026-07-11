"""StateGraph 조립 — 순차 노드 + '약관 DB 존재 여부' 조건 분기.

build_graph()는 compile된 그래프를 반환. 노드 함수는 nodes.agents.
"""

from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from .nodes import agents
from .state import ReportState


def build_graph():
    g = StateGraph(ReportState)

    g.add_node("load_context", agents.load_context)
    g.add_node("input_guardrail", agents.input_guardrail)
    g.add_node("diagnosis", agents.diagnosis)
    g.add_node("terms_parse", agents.terms_parse)
    g.add_node("coverage_parse", agents.coverage_parse)
    g.add_node("coverage_analysis", agents.coverage_analysis)
    g.add_node("case_search", agents.case_search)
    g.add_node("disability_rag", agents.disability_rag)
    g.add_node("disability_calc", agents.disability_calc)
    g.add_node("payment_calc", agents.payment_calc)
    g.add_node("report_compose", agents.report_compose)
    g.add_node("output_guardrail", agents.output_guardrail)
    g.add_node("persist", agents.persist)
    g.add_node("persist_blocked", agents.persist_blocked)

    g.add_edge(START, "load_context")
    g.add_edge("load_context", "input_guardrail")

    # 차단(도메인외·PII정책 등) 시 LLM 파이프라인을 건너뛰고 persist_blocked로 → 비용/오출력
    # 방지. persist_blocked가 reports.status='BLOCKED'만 기록해 상태 미갱신(무한 처리중)을 막는다.
    g.add_conditional_edges(
        "input_guardrail",
        agents.route_after_input,
        {"blocked": "persist_blocked", "diagnosis": "diagnosis"},
    )

    # 분기: 사용자 약관이 우리 DB(policy_chunks)에 있나?
    #   없음 → terms_parse(런타임 파싱 스텁) → coverage_parse
    #   있음 → coverage_parse 직행
    g.add_conditional_edges(
        "diagnosis",
        agents.policy_in_db,
        {"terms_parse": "terms_parse", "coverage_parse": "coverage_parse"},
    )
    g.add_edge("terms_parse", "coverage_parse")

    g.add_edge("coverage_parse", "coverage_analysis")
    g.add_edge("coverage_analysis", "case_search")

    # 분기: 후유장해 검토 필요 시 장해 서브그래프(분류·지급률 → 결정론 합산) 경유,
    #   아니면 payment_calc 직행. 장해 경로는 payment_calc에서 재합류(2 incoming).
    g.add_conditional_edges(
        "case_search",
        agents.route_after_case,
        {"disability": "disability_rag", "payment_calc": "payment_calc"},
    )
    g.add_edge("disability_rag", "disability_calc")
    g.add_edge("disability_calc", "payment_calc")

    g.add_edge("payment_calc", "report_compose")
    g.add_edge("report_compose", "output_guardrail")
    g.add_edge("output_guardrail", "persist")
    g.add_edge("persist", END)
    g.add_edge("persist_blocked", END)

    return g.compile()
