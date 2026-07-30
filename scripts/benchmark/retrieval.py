"""검색(retrieval) 정확성 — rag.search 직접 호출로 Recall@k·MRR·nDCG@k·Hit@1 측정.

이 축은 임베딩 모델·RRF에 좌우되며 **챗 LLM과 무관**하다(→ 챗 LLM 비교 시 상수). 임베딩 모델을
바꿔 비교할 때만 다시 돌린다. 골드(relevant_chunk_ids)가 빈 항목은 건너뛴다.

`discover()`는 각 쿼리의 상위 검색 결과(source_ref·조항·본문)를 뽑아 골드 라벨링을 돕는다.
"""

from __future__ import annotations

import math
from typing import Any

from benchmark.cases import RetrievalGold
from rag import search


def _dcg(relevances: list[int]) -> float:
    """이진 관련도 리스트의 DCG(1-based 위치, log2 할인)."""
    return sum(rel / math.log2(pos + 2) for pos, rel in enumerate(relevances))


def _score_one(retrieved_ids: list[str], relevant: set[str], top_k: int) -> dict[str, float]:
    """단일 쿼리의 검색 지표(Recall@k·MRR·nDCG@k·Hit@1)."""
    rels = [1 if rid in relevant else 0 for rid in retrieved_ids[:top_k]]
    hit_positions = [pos for pos, rel in enumerate(rels) if rel]
    recall = (sum(rels) / len(relevant)) if relevant else 0.0
    mrr = (1.0 / (hit_positions[0] + 1)) if hit_positions else 0.0
    ideal = _dcg(sorted(rels, reverse=True))
    ndcg = (_dcg(rels) / ideal) if ideal else 0.0
    hit1 = 1.0 if rels[:1] == [1] else 0.0
    return {"recall": recall, "mrr": mrr, "ndcg": ndcg, "hit1": hit1}


async def score_retrieval(gold: list[RetrievalGold], top_k: int = 8) -> dict[str, Any]:
    """골드 쿼리들의 검색 정확성을 평균낸다. 라벨(relevant_chunk_ids) 있는 항목만 집계.

    Args:
        gold: 검색 골드 목록.
        top_k: 평가 컷오프(파이프라인 기본 8).

    Returns:
        {"n_labeled", "n_unlabeled", "recall@k", "mrr", "ndcg@k", "hit@1", "per_query"}.
    """
    labeled = [g for g in gold if g.relevant_chunk_ids]
    per_query: list[dict[str, Any]] = []
    acc = {"recall": [], "mrr": [], "ndcg": [], "hit1": []}
    for item in labeled:
        res = await search(item.query, namespaces=item.namespaces, top_k=top_k)
        retrieved = [c.source_ref for c in res.ranked_chunks]
        scores = _score_one(retrieved, item.relevant_chunk_ids, top_k)
        per_query.append({"query": item.query, **scores})
        for key in acc:
            acc[key].append(scores[key])

    def avg(xs: list[float]) -> float:
        return sum(xs) / len(xs) if xs else 0.0

    return {
        "n_labeled": len(labeled),
        "n_unlabeled": len(gold) - len(labeled),
        f"recall@{top_k}": avg(acc["recall"]),
        "mrr": avg(acc["mrr"]),
        f"ndcg@{top_k}": avg(acc["ndcg"]),
        "hit@1": avg(acc["hit1"]),
        "per_query": per_query,
    }


async def discover(gold: list[RetrievalGold], top_k: int = 8) -> list[dict[str, Any]]:
    """각 쿼리 상위 검색 결과를 뽑아 골드 라벨링을 돕는다(relevant_chunk_ids 채우기용)."""
    out: list[dict[str, Any]] = []
    for item in gold:
        res = await search(item.query, namespaces=item.namespaces, top_k=top_k)
        out.append(
            {
                "query": item.query,
                "candidates": [
                    {
                        "source_ref": c.source_ref,
                        "article_number": c.article_number,
                        "product_name": c.product_name,
                        "chunk_type": c.chunk_type,
                        "score": round(c.score, 5),
                        "text_head": " ".join((c.text or "").split())[:80],
                    }
                    for c in res.ranked_chunks
                ],
            }
        )
    return out
