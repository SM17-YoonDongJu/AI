"""Hybrid RAG 검색 파이프라인 (04). report_worker가 함수 호출로 사용.

공개 진입점은 `search()`. 결과 모델(Chunk·Citation·RagResult)은 core.contracts가 단일
출처다. 내부 단계는 router·typo·fusion·search 모듈로 분리되어 있다. `preload_kiwi()`는
오타 보정이 쓰는 형태소 분석기(Kiwi)를 워커 기동 때 미리 올려 둔다.
"""

# rag는 report_worker가 쓰는 모듈이라 이 경로 변경은
# CI/CD paths-filter에서 report 서비스의 재배포를 유발한다(ocr 제외).

from rag.search import RagError, search
from rag.typo import preload_kiwi

__all__ = ["RagError", "preload_kiwi", "search"]
