"""검색 정확성 단독 평가 — RETRIEVAL_GOLD로 Recall@k·MRR·nDCG@k·Hit@1 출력(챗 LLM 무관).

임베더/리랭커를 바꿔 비교할 때 이걸로 잰다. 라벨(relevant_chunk_ids) 있는 쿼리만 채점한다.
먼저 --discover로 각 쿼리의 후보(source_ref)를 뽑아 cases.RETRIEVAL_GOLD를 채운 뒤 채점한다.

    # 1) 라벨링용 후보 뽑기 (source_ref를 골라 cases.RETRIEVAL_GOLD에 채움)
    PYTHONPATH=src:scripts uv run python scripts/retrieval_eval.py --discover | tee gold_candidates.txt
    # 2) 채점 (임베더/리랭커 바꿀 때마다 재적재 후 재실행해 비교)
    PYTHONPATH=src:scripts uv run python scripts/retrieval_eval.py

전제: tempVectorDB(policy_chunks 적재) + Ollama + env(DATABASE_URL·EMBEDDING_BASE_URL·
EMBEDDING_MODEL). 챗 모델은 불필요(AI_BASE_URL 없어도 됨).
"""

from __future__ import annotations

import argparse
import asyncio

from benchmark import retrieval
from benchmark.cases import RETRIEVAL_GOLD
from core.db import close_pool, init_pool
from report_worker.rag.hybrid import close_pool as close_rag_pool

_BAR = "=" * 90


async def _discover(top_k: int) -> None:
    """각 쿼리의 상위 후보를 사람이 읽기 좋게 출력한다(relevant_chunk_ids 라벨링용)."""
    cand = await retrieval.discover(RETRIEVAL_GOLD, top_k=top_k)
    for item in cand:
        print(f"\n{_BAR}\nQ: {item['query']}\n{'-' * 90}")
        for c in item["candidates"]:
            print(
                f"  [{c['source_ref']}]  ({c['article_number']} · {c['product_name']} · "
                f"{c['chunk_type']} · score={c['score']})"
            )
            print(f"      {c['text_head']}")
    print(f"\n{_BAR}")
    print(
        "→ 각 Q에서 정답 청크의 [source_ref]를 골라 cases.RETRIEVAL_GOLD의 relevant_chunk_ids에 채우세요."
    )


async def _score(top_k: int) -> None:
    """RETRIEVAL_GOLD 채점 결과를 출력한다(라벨된 쿼리만 집계)."""
    res = await retrieval.score_retrieval(RETRIEVAL_GOLD, top_k=top_k)
    print(f"\n=== 검색 정확성 (top_k={top_k}) ===")
    print(f"라벨 {res['n_labeled']}건 / 미라벨 {res['n_unlabeled']}건")
    if not res["n_labeled"]:
        print("→ 라벨이 0건입니다. --discover로 후보를 뽑아 relevant_chunk_ids를 채우세요.")
        return
    print(f"Recall@{top_k} = {res[f'recall@{top_k}']:.4f}")
    print(f"MRR          = {res['mrr']:.4f}")
    print(f"nDCG@{top_k}   = {res[f'ndcg@{top_k}']:.4f}")
    print(f"Hit@1        = {res['hit@1']:.4f}")
    print("\n쿼리별 (R=Recall, H1=Hit@1):")
    for q in res["per_query"]:
        print(
            f"  R={q['recall']:.2f} MRR={q['mrr']:.2f} nDCG={q['ndcg']:.2f} "
            f"H1={q['hit1']:.0f}  {q['query']}"
        )


async def main() -> None:
    p = argparse.ArgumentParser(description="검색 정확성 단독 평가(임베더/리랭커 비교용)")
    p.add_argument("--discover", action="store_true", help="채점 대신 쿼리별 후보를 출력(라벨링용)")
    p.add_argument("--top-k", type=int, default=8, help="평가 컷오프(파이프라인 기본 8)")
    args = p.parse_args()

    await init_pool()
    try:
        if args.discover:
            await _discover(args.top_k)
        else:
            await _score(args.top_k)
    finally:
        await close_pool()
        await close_rag_pool()


if __name__ == "__main__":
    asyncio.run(main())
