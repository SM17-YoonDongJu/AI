"""report_worker RAG 어댑터 — canonical `src/rag`를 호출해 노드가 기대하는 dict로 변환한다.

과거엔 자체 하이브리드 검색 구현이었으나 `src/rag`로 단일화했다(중복 제거). 여기서는 pydantic
`RagResult`를 리포트 노드들이 쓰는 dict 형태로만 바꾼다 — `source_ref`는 사람이 읽는 인용
문자열로 포맷. 검색 로직·insurer/product 필터·임베딩은 모두 `src/rag`가 담당한다.
(모듈명 `hybrid`는 레거시 — 노드 임포트 호환을 위해 유지.)
"""

from __future__ import annotations

import datetime
from typing import Any

from core.contracts import Chunk
from rag import search as _rag_search


def _fmt_citation(c: Chunk) -> str:
    """`[조항/사례번호, 상품명]` 형식의 사람이 읽는 인용. 메타 없으면 chunk_id로 폴백."""
    inside = c.article_number or c.source_ref
    return f"[{inside}, {c.product_name or ''}]"


async def search(
    query: str,
    insurance_type: str | None = None,
    namespaces: list[str] | None = None,
    top_k: int = 8,
    *,
    insurer: str | None = None,
    product: str | None = None,
    contract_date: datetime.date | None = None,
) -> dict[str, Any]:
    """canonical `rag.search` 호출 후 리포트 노드용 dict로 매핑.

    반환: {"ranked_chunks": [{text, namespace, score, source_ref, article_number,
    product_name, chunk_type}], "citations": [str]}. namespaces 기본 ["terms"].

    `contract_date`는 `namespaces=["level"]`(표준 장해분류표) 검색 시 계약일이 속하는 버전만
    반환하도록 canonical `rag.search`로 패스스루한다(None이면 현행판).
    """
    res = await _rag_search(
        query,
        insurance_type,
        namespaces or ["terms"],
        top_k,
        insurer=insurer,
        product=product,
        contract_date=contract_date,
    )
    chunks = [
        {
            "text": c.text,
            "namespace": c.namespace,
            "score": c.score,
            "source_ref": _fmt_citation(c),
            "article_number": c.article_number,
            "product_name": c.product_name,
            "chunk_type": c.chunk_type,
        }
        for c in res.ranked_chunks
    ]
    return {"ranked_chunks": chunks, "citations": [c["source_ref"] for c in chunks]}


# 하위호환 별칭
hybrid_search = search


async def close_pool() -> None:
    """진입점/scripts의 `close_rag_pool` 호환. 풀은 core.db가 소유하므로 no-op."""
    return None
