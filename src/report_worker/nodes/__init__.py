"""LangGraph 노드 함수.

각 노드는 ReportState 부분 dict만 반환한다(전체 state 반환 금지 — LangGraph가 머지).
노드 실패는 상위(graph)에서 1회 재시도 후 errors에 기록하고 부분결과로 진행한다.
"""
