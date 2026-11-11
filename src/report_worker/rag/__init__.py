"""Hybrid RAG 검색 (실험용) — BM25(tsvector) + 벡터(halfvec) + RRF.

레포 src/rag 정식 이관 전 langGraph 안에서 pgvector(12,821청크)로 빠르게 검증.
계약(src/rag/README.md): RRF_K=60, SIMILARITY_THRESHOLD=0.4, EMBEDDING_DIM=1024,
입력=쿼리+namespace힌트, 출력=ranked_chunks+citations.
"""

from .hybrid import hybrid_search, search

__all__ = ["hybrid_search", "search"]
