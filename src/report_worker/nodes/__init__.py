"""LangGraph 노드 함수.

각 노드는 ReportState 부분 dict만 반환한다(전체 state 반환 금지 — LangGraph가 머지).
노드 실패는 재시도 없이 errors에 기록하고 부분결과로 진행한다(safe_node). 단 컨텍스트 하드
실패와 ai_client 실패(시스템 실패·거절)는 graph.py가 다음 간선에서 그래프를 멈춘다.
"""
