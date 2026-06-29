"""하이브리드 검색(async) 빠른 검증 CLI.

    python -m langGraph.rag.search_test "후유장해 장해등급 보험금"
환경: DATABASE_URL(:5433), OLLAMA_URL, EMBED_MODEL=qwen3-embedding:0.6b
"""

import asyncio
import sys

try:
    from .hybrid import close_pool, search
except ImportError:
    import os

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from langGraph.rag.hybrid import close_pool, search

QUERIES = [
    "후유장해 장해등급에 따른 보험금 지급",
    "상해로 입원했을 때 입원일당 지급 사유",
    "보험금을 지급하지 않는 면책 사유",
]


async def run(q: str) -> None:
    print(f"\n=== Q: {q} ===")
    res = await search(q, namespaces=["terms"], top_k=5)
    for i, c in enumerate(res["ranked_chunks"], 1):
        text = " ".join(c["text"].split())[:70]
        print(f"{i}. score={c['score']:.4f} | {c['product_name']} | {c.get('article_number') or '-'}")
        print(f"   {text}")


async def main() -> None:
    qs = [" ".join(sys.argv[1:])] if len(sys.argv) > 1 else QUERIES
    for q in qs:
        await run(q)
    await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
