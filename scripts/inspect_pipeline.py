"""파이프라인 산출물 육안 확인 — 모델 × 시나리오별 [입력 → 검색 → 분석 → 리포트] 덤프.

벤치마크(benchmark.run)는 리포트 본문을 저장하지 않고 채점 후 폐기하므로, 여기서 그래프를
직접 실행해 사용자 입력·검색된 약관 청크·최종 리포트를 눈으로 확인한다. 시나리오/시드/정리는
battery와 동일(임시 UUID 시드→실행→정리)하므로 벤치마크와 같은 입력을 본다.

전제: tempVectorDB(policy_chunks 적재) + Ollama + env(DATABASE_URL·AI_BASE_URL·
EMBEDDING_BASE_URL·EMBEDDING_MODEL). 실행:

    PYTHONPATH=src:scripts uv run python scripts/inspect_pipeline.py --models gemma4:12b
    PYTHONPATH=src:scripts uv run python scripts/inspect_pipeline.py \
        --models gemma3:12b,gemma4:12b,qwen3.6:27b --only A,B,G --chunks 3

주의: 모델 × 시나리오를 모두 돌리면 그래프 실행 수가 벤치마크 1회분과 비슷하다(느림).
빠르게 보려면 --only 로 시나리오를, --models 로 모델을 좁혀라.
"""

from __future__ import annotations

import argparse
import asyncio
import textwrap

from battery import SCENARIOS, _cleanup, _seed
from core.config import settings
from core.db import close_pool, init_pool
from report_worker.graph import build_graph
from report_worker.rag.hybrid import close_pool as close_rag_pool

_BAR = "=" * 92


def _wrap(v: object, width: int = 92) -> str:
    s = str(v).strip()
    return textwrap.fill(s, width) if s else "(없음)"


def _get(chunk: object, key: str, default: object = None) -> object:
    """청크가 dict든 dataclass든 안전하게 필드를 꺼낸다."""
    if isinstance(chunk, dict):
        return chunk.get(key, default)
    return getattr(chunk, key, default)


async def _run_scenario(app: object, model: str, sc: tuple, chunks: int) -> None:
    """시나리오 1개를 시드→실행→덤프→정리한다."""
    label, insurer, product, atype = sc[0], sc[1], sc[2], sc[3]
    diagnosis, masked, offered, question = sc[4], sc[5], sc[6], sc[7]

    ids = await _seed(*sc)
    try:
        job = {
            "report_id": ids[0],
            "ocr_result_id": ids[1],
            "claim_id": ids[2],
            "user_ref": str(ids[3]),
            "doc_type": "diagnosis",
        }
        st = await app.ainvoke(job)

        print(f"\n{_BAR}\n[{label}]  모델={model}\n{_BAR}")
        print("■ 사용자 입력")
        print(f"  보험사/상품 : {insurer} / {product}")
        print(f"  사고유형    : {atype}   |   제시금액: {offered:,}원")
        print(f"  진단(요약)  : {diagnosis}")
        print(f"  진단서 원문 : {_wrap(masked)}")
        print(f"  질문        : {question}")

        clauses = st.get("retrieved_clauses") or []
        prods = sorted({str(_get(c, "product_name")) for c in clauses}) if clauses else []
        print(f"\n■ 검색된 약관 청크: {len(clauses)}개  (상품: {prods or '-'})")
        for c in clauses[:chunks]:
            art = _get(c, "article_number") or _get(c, "article_no") or ""
            print(f"    - {art} {_wrap(_get(c, 'text', ''))[:200]}")

        print("\n■ 분석 결과")
        print(f"  적용특약    : {st.get('applicable_coverages')}")
        print(f"  누락특약    : {st.get('missing_coverages')}")
        er = st.get("estimated_range") or {}
        print(f"  추정범위    : {er.get('min')} ~ {er.get('max')}")
        da = st.get("disability_analysis") or {}
        if da:
            print(
                f"  장해지급률  : {da.get('combined_rate')}% "
                f"(신뢰도 {da.get('confidence')}, 근거 {len(da.get('citations', []))}건)"
            )
        jf = st.get("judge_failures") or []
        print(f"  인용검증실패: {len(jf)}건 {jf or ''}")
        print(
            f"  이슈        : {len(st.get('issues') or [])}건   |   errors: {st.get('errors', [])}"
        )

        print("\n■ 리포트 본문 ▼")
        print(st.get("report") or "(리포트 없음 — 차단/오류)")
    finally:
        await _cleanup(*ids)


async def main() -> None:
    p = argparse.ArgumentParser(description="리포트 파이프라인 산출물 확인 (모델 × 시나리오)")
    p.add_argument(
        "--models",
        required=True,
        help="ollama 챗 모델 태그 CSV (예: gemma3:12b,gemma4:12b,qwen3.6:27b)",
    )
    p.add_argument("--only", default="", help="라벨 접두 CSV 필터 (예: A,B,G). 생략 시 전체")
    p.add_argument("--chunks", type=int, default=0, help="검색된 약관 청크 본문을 앞 N개까지 출력")
    args = p.parse_args()

    models = [m.strip() for m in args.models.split(",") if m.strip()]
    only = {x.strip() for x in args.only.split(",") if x.strip()}
    scenarios = [sc for sc in SCENARIOS if not only or any(sc[0].startswith(o) for o in only)]

    await init_pool()
    app = build_graph()
    try:
        for model in models:
            settings.llm_model = model
            for sc in scenarios:
                await _run_scenario(app, model, sc, args.chunks)
    finally:
        await close_pool()
        await close_rag_pool()


if __name__ == "__main__":
    asyncio.run(main())
