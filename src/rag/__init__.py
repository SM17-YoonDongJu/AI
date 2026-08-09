"""Hybrid RAG 검색 파이프라인 (04). report_worker·chatbot이 함수 호출로 공유.

공개 진입점은 `search()`. 결과 모델(Chunk·Citation·RagResult)은 core.contracts가 단일
출처다. 내부 단계는 router·typo·fusion·search 모듈로 분리되어 있다.
"""

# rag는 report_worker·chatbot이 함께 쓰는 공유 모듈이라 이 경로 변경은
# CI/CD paths-filter에서 report·chatbot 두 서비스의 재배포를 유발한다(ocr 제외).

from rag.search import RagError, search

__all__ = ["RagError", "search"]
