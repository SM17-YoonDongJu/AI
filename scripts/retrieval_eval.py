"""검색 정확성 단독 평가 + 다중 리트리버 후보 풀링 — 챗 LLM 무관.

임베더/리랭커 비교용. RETRIEVAL_GOLD로 Recall@k·MRR·nDCG@k·Hit@1를 잰다.

**pooling bias 완화**: 후보를 한 임베더로만 뽑으면 그 임베더에 유리한 골드가 된다.
여러 임베더로 후보를 각각 덤프한 뒤 union해서 라벨한다(source_ref는 임베더와 무관).

    # 1) 임베더 A(qwen) 적재 상태에서 후보 덤프
    ... retrieval_eval.py --discover --dump scripts/benchmark/results/pool_qwen.json --top-k 30
    # 2) 임베더 B(bge)로 재적재 후 후보 덤프
    ... retrieval_eval.py --discover --dump scripts/benchmark/results/pool_bge.json --top-k 30
    # 3) 두 덤프 union → 라벨용 출력(<태그>=어느 임베더가 뽑았는지)
    ... retrieval_eval.py --pool scripts/benchmark/results/pool_qwen.json scripts/benchmark/results/pool_bge.json
    # 4) cases.RETRIEVAL_GOLD 채운 뒤, 각 임베더로 채점(임베더 바꿀 때마다 재적재)
    ... retrieval_eval.py

전제: tempVectorDB + Ollama + env(DATABASE_URL·EMBEDDING_BASE_URL·EMBEDDING_MODEL).
챗 모델 불필요. --pool은 DB 없이 파일만 읽는다.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from benchmark import retrieval
from benchmark.cases import RETRIEVAL_GOLD
from core.db import close_pool, init_pool
from report_worker.rag.hybrid import close_pool as close_rag_pool

_BAR = "=" * 90


def _print_candidates(query: str, cands: list[dict], sources: dict[str, set[str]] | None) -> None:
    """쿼리 후보를 사람이 읽기 좋게 출력(sources 있으면 어느 리트리버가 뽑았는지 태그)."""
    print(f"\n{_BAR}\nQ: {query}\n{'-' * 90}")
    for c in cands:
        tag = f"  <{'+'.join(sorted(sources[c['source_ref']]))}>" if sources else ""
        print(
            f"  [{c['source_ref']}]  ({c['article_number']} · {c['product_name']} · "
            f"{c['chunk_type']} · score={c['score']}){tag}"
        )
        print(f"      {c['text_head']}")


async def _discover(top_k: int, dump: str | None) -> None:
    cand = await retrieval.discover(RETRIEVAL_GOLD, top_k=top_k)
    for item in cand:
        _print_candidates(item["query"], item["candidates"], None)
    if dump:
        p = Path(dump)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(cand, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n덤프 저장: {dump}  (다른 임베더 덤프와 --pool로 합쳐 라벨링)")
    print(
        f"\n{_BAR}\n→ 정답 [source_ref]를 골라 cases.RETRIEVAL_GOLD의 relevant_chunk_ids에 채우세요."
    )


def _pool(paths: list[str]) -> None:
    """여러 discover 덤프를 union해 라벨용으로 출력한다(pooling bias 완화, DB 불필요)."""
    by_query: dict[str, dict[str, dict]] = {}
    sources: dict[str, set[str]] = {}
    order: list[str] = []
    for path in paths:
        tag = Path(path).stem.replace("pool_", "")
        for item in json.loads(Path(path).read_text(encoding="utf-8")):
            q = item["query"]
            if q not in by_query:
                by_query[q] = {}
                order.append(q)
            for c in item["candidates"]:
                by_query[q].setdefault(c["source_ref"], c)
                sources.setdefault(c["source_ref"], set()).add(tag)
    for q in order:
        cands = sorted(by_query[q].values(), key=lambda c: -c["score"])
        _print_candidates(q, cands, sources)
    print(
        f"\n{_BAR}\n→ union 후보(중복 제거). <태그>=뽑은 리트리버. "
        "양쪽이 뽑은 것 + 한쪽만 뽑은 것 모두 검토해 정답을 라벨하세요."
    )


async def _score(top_k: int) -> None:
    res: dict[str, Any] = await retrieval.score_retrieval(RETRIEVAL_GOLD, top_k=top_k)
    print(f"\n=== 검색 정확성 (top_k={top_k}) ===")
    print(f"라벨 {res['n_labeled']}건 / 미라벨 {res['n_unlabeled']}건")
    if not res["n_labeled"]:
        print("→ 라벨이 0건입니다. --discover/--pool로 후보를 뽑아 relevant_chunk_ids를 채우세요.")
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
    p = argparse.ArgumentParser(description="검색 정확성 평가 + 다중 리트리버 후보 풀링")
    p.add_argument("--discover", action="store_true", help="채점 대신 쿼리별 후보 출력(라벨링용)")
    p.add_argument("--dump", default="", help="--discover 후보를 JSON으로 저장(풀링용)")
    p.add_argument(
        "--pool", nargs="+", default=[], help="여러 덤프를 union해 라벨용 출력(DB 불필요)"
    )
    p.add_argument(
        "--top-k", type=int, default=8, help="평가 컷오프(채점 기본 8, 풀링 덤프는 30 권장)"
    )
    args = p.parse_args()

    if args.pool:  # 순수 파일 연산 — DB 불필요
        _pool(args.pool)
        return

    await init_pool()
    try:
        if args.discover:
            await _discover(args.top_k, args.dump or None)
        else:
            await _score(args.top_k)
    finally:
        await close_pool()
        await close_rag_pool()


if __name__ == "__main__":
    asyncio.run(main())
