"""canonical rag.search 라이브 검증 — src/rag가 tempVectorDB에서 실제 동작하는지 + 필터.

    PYTHONPATH=src python scripts/rag_check.py
전제: tempVectorDB + Ollama, env(DATABASE_URL·EMBEDDING_MODEL).
"""

from __future__ import annotations

import asyncio
import sys

from core.db import close_pool, get_pool, init_pool
from rag import search

sys.stdout.reconfigure(encoding="utf-8")


def _snip(t: str) -> str:
    return " ".join(t.split())[:50]


async def main() -> None:
    await init_pool()
    q = "후유장해 장해분류표 지급률"

    r0 = await search(q, namespaces=["terms"], top_k=5)
    print(f"[terms no-filter] chunks={len(r0.ranked_chunks)} citations={len(r0.citations)}")
    for c in r0.ranked_chunks[:3]:
        print(
            f"  - ct={c.chunk_type} art={c.article_number} prod={c.product_name} | {_snip(c.text)}"
        )

    r1 = await search(q, namespaces=["terms"], top_k=5, insurer="현대해상", product="보통상해보험")
    products = {c.product_name for c in r1.ranked_chunks}
    print(f"[terms filtered 보통] chunks={len(r1.ranked_chunks)} products={products}")

    rc = await search("무릎 동요관절 장해 판정", namespaces=["case"], top_k=3)
    sample = _snip(rc.ranked_chunks[0].text) if rc.ranked_chunks else "-"
    print(f"[case] chunks={len(rc.ranked_chunks)} sample={sample}")

    # 시나리오 A 패턴: 전방십자인대 파열 + 보통 필터 (coverage_analysis가 rag_empty였던 케이스)
    ra = await search(
        "전방십자인대 파열 관절경 재건술 누락 특약",
        namespaces=["terms"],
        top_k=8,
        insurer="현대해상",
        product="보통상해보험",
    )
    print(f"[A패턴 보통필터] chunks={len(ra.ranked_chunks)}")

    pool = get_pool()
    async with pool.acquire() as c:
        n = await c.fetchval("SELECT count(*) FROM policy_chunks WHERE product_name='보통상해보험'")
    print(f"[보통상해보험 총 청크수] {n}")

    await close_pool()


asyncio.run(main())
