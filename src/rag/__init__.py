"""Hybrid RAG 검색 파이프라인 (04). report_worker·chatbot이 함수 호출로 공유.

공개 진입점은 `search()`. 결과 모델(Chunk·Citation·RagResult)은 core.contracts가 단일
출처다. 내부 단계는 router·typo·fusion·search 모듈로 분리되어 있다.
"""

from rag.search import RagError, search

__all__ = ["RagError", "search"]
