"""Hybrid 검색 (async): 쿼리 임베딩 → BM25 ∥ 벡터 → RRF 통합 → 인용 역추적.

DB: tempVectorDB pgvector (policy_chunks). 쿼리 임베딩: Ollama qwen3-embedding(httpx).
적재(Policy-Chunker)와 동일 모델·1024d를 써야 벡터공간이 일치한다.

계약(contracts.md) 시그니처:
  async def search(query, insurance_type=None, namespaces=None, top_k=8) -> RagResult
  RagResult.ranked_chunks: [{text, namespace, score, source_ref}]
  RagResult.citations: [str]
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import asyncpg
import httpx

# ── 계약 상수 (src/rag/README.md) ──
RRF_K = 60
EMBEDDING_DIM = 1024
SIMILARITY_THRESHOLD = 0.4

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/aiengine")
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "qwen3-embedding:0.6b")

# qwen3-embedding은 쿼리에 instruction 프리픽스 권장(문서는 raw로 임베딩됨 → 쿼리만 프리픽스).
_QUERY_INSTRUCT = "Instruct: 보험 약관에서 사용자 질문에 답할 수 있는 관련 조항을 검색한다\nQuery: "

# namespace → 테이블.
_NS_TABLE = {"terms": "policy_chunks", "case": "case_chunks"}

# 테이블별 컬럼 매핑 → (article_number, article_title, page_number) 자리에 쓸 실제 컬럼.
# policy_chunks와 case_chunks는 스키마가 달라 검색 SELECT에서 정합용으로 별칭 처리.
_COLS = {
    "policy_chunks": ("article_number", "article_title", "page_number"),
    "case_chunks": ("section", "case_title", "chunk_index"),
}

# 쿼리 형태소 토큰화(BM25용). content_tokens가 Kiwi 결과라 쿼리도 맞춰야 매칭률↑.
try:
    from kiwipiepy import Kiwi  # type: ignore

    _kiwi = Kiwi()

    def _tokenize(text: str) -> str:
        return " ".join(t.form for t in _kiwi.tokenize(text))
except Exception:  # kiwi 없으면 raw 폴백
    def _tokenize(text: str) -> str:
        return text


_pool: asyncpg.Pool | None = None


async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=5)
    return _pool


async def _embed_query(query: str) -> str:
    """qwen3 프리픽스 붙여 쿼리 임베딩 → pgvector 리터럴 문자열."""
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": EMBED_MODEL, "input": _QUERY_INSTRUCT + query},
        )
        resp.raise_for_status()
        vec = resp.json()["embeddings"][0]
    if len(vec) != EMBEDDING_DIM:
        raise RuntimeError(f"쿼리 임베딩 차원 {len(vec)} != {EMBEDDING_DIM}")
    return "[" + ",".join(map(str, vec)) + "]"


def _meta_filter(args: list, insurer: str | None, product: str | None) -> str:
    """insurer/product 메타 필터 절을 만들고 args에 파라미터를 덧붙인다(다음 $N)."""
    clause = ""
    if insurer:
        args.append(insurer)
        clause += f" AND insurer = ${len(args)}"
    if product:
        args.append(product)
        clause += f" AND product_name = ${len(args)}"
    return clause


async def _vector_search(conn, table, qvec, k, insurer, product) -> list[dict[str, Any]]:
    a, b, p = _COLS.get(table, _COLS["policy_chunks"])
    args: list = [qvec]
    where = "embedding IS NOT NULL" + _meta_filter(args, insurer, product)
    rows = await conn.fetch(
        f"""
        SELECT chunk_id, content, {a} AS article_number, {b} AS article_title, product_name,
               {p} AS page_number, chunk_type,
               1 - (embedding <=> $1::halfvec) AS score
        FROM {table}
        WHERE {where}
        ORDER BY embedding <=> $1::halfvec
        LIMIT {k}
        """,
        *args,
    )
    return [_row(r) for r in rows]


async def _bm25_search(conn, table, q_tokens, k, insurer, product) -> list[dict[str, Any]]:
    a, b, p = _COLS.get(table, _COLS["policy_chunks"])
    args: list = [q_tokens]
    extra = _meta_filter(args, insurer, product)
    rows = await conn.fetch(
        f"""
        SELECT chunk_id, content, {a} AS article_number, {b} AS article_title, product_name,
               {p} AS page_number, chunk_type,
               ts_rank(to_tsvector('simple', coalesce(content_tokens,'')),
                       plainto_tsquery('simple', $1)) AS score
        FROM {table}
        WHERE to_tsvector('simple', coalesce(content_tokens,''))
              @@ plainto_tsquery('simple', $1){extra}
        ORDER BY score DESC
        LIMIT {k}
        """,
        *args,
    )
    return [_row(r) for r in rows]


def _row(r) -> dict[str, Any]:
    return {
        "chunk_id": r["chunk_id"],
        "content": r["content"],
        "article_number": r["article_number"],
        "article_title": r["article_title"],
        "product_name": r["product_name"],
        "page_number": r["page_number"],
        "chunk_type": r["chunk_type"],
        "score": float(r["score"]) if r["score"] is not None else 0.0,
    }


def _rrf(lists: list[list[dict[str, Any]]], k: int = RRF_K) -> list[dict[str, Any]]:
    """RRF 통합: score = Σ 1/(k + rank)."""
    fused: dict[str, dict[str, Any]] = {}
    for ranked in lists:
        for rank, item in enumerate(ranked, start=1):
            cid = item["chunk_id"]
            if cid not in fused:
                fused[cid] = {**item, "rrf": 0.0}
            fused[cid]["rrf"] += 1.0 / (k + rank)
    return sorted(fused.values(), key=lambda x: x["rrf"], reverse=True)


def _citation(c: dict[str, Any]) -> str:
    parts = [p for p in (c.get("article_number"), c.get("article_title")) if p]
    inside = " ".join(parts) if parts else f"p{c.get('page_number')}"
    return f"[{inside}, {c.get('product_name') or ''}]"


async def _table_exists(conn, table: str) -> bool:
    return (await conn.fetchval("SELECT to_regclass($1)", table)) is not None


async def search(
    query: str,
    insurance_type: str | None = None,
    namespaces: list[str] | None = None,
    top_k: int = 8,
    *,
    insurer: str | None = None,
    product: str | None = None,
) -> dict[str, Any]:
    """BM25 + 벡터 병렬 후 RRF 통합.

    namespaces: ["terms", "case", ...]. 기본 ["terms"].
    반환: {"ranked_chunks": [{text, namespace, score, source_ref, ...}], "citations": [str]}.
    """
    namespaces = namespaces or ["terms"]
    qvec = await _embed_query(query)
    q_tokens = _tokenize(query)
    pool = await _get_pool()

    ranked: list[dict[str, Any]] = []
    async with pool.acquire() as check_conn:
        valid_ns = [ns for ns in namespaces
                    if _NS_TABLE.get(ns) and await _table_exists(check_conn, _NS_TABLE[ns])]

    for ns in valid_ns:  # case_chunks 미적재 ns는 위에서 제외(폴백)
        table = _NS_TABLE[ns]
        pool_n = max(top_k * 3, 20)  # RRF 후보 풀

        async def _vec():  # 동시 쿼리 → 각자 별도 커넥션 (단일 conn 동시사용 불가)
            async with pool.acquire() as c:
                return await _vector_search(c, table, qvec, pool_n, insurer, product)

        async def _bm():
            async with pool.acquire() as c:
                return await _bm25_search(c, table, q_tokens, pool_n, insurer, product)

        vec, bm = await asyncio.gather(_vec(), _bm())
        for c in _rrf([vec, bm])[:top_k]:
            c["namespace"] = ns
            ranked.append(c)

    ranked.sort(key=lambda x: x.get("rrf", 0.0), reverse=True)
    chunks = [
        {
            "text": c["content"],
            "namespace": c["namespace"],
            "score": c["rrf"],
            "source_ref": _citation(c),
            "article_number": c.get("article_number"),
            "product_name": c.get("product_name"),
        }
        for c in ranked
    ]
    citations = [c["source_ref"] for c in chunks]
    return {"ranked_chunks": chunks, "citations": citations}


# 하위호환 별칭
hybrid_search = search


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
