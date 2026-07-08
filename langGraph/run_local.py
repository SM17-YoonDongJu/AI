"""Kafka 없이 그래프를 단건(async) 실행하는 실험 진입점.

    python -m langGraph.run_local
전제: tempVectorDB 기동 + seed_sample.sql 적용 + Ollama(qwen2.5:3b, qwen3-embedding) 기동.
"""

from __future__ import annotations

import asyncio
import json

from core.db import close_pool, init_pool

from .graph import build_graph
from .rag.hybrid import close_pool as close_rag_pool

# ReportJob 패스스루 (seed_sample.sql과 매칭)
SAMPLE_JOB = {
    "report_id": "00000000-0000-0000-0000-000000000001",
    "ocr_result_id": "00000000-0000-0000-0000-000000000002",
    "claim_id": "00000000-0000-0000-0000-000000000003",
    "user_ref": "user-1",
    "doc_type": "diagnosis",
}


async def main() -> None:
    await init_pool()
    app = build_graph()
    final = await app.ainvoke(SAMPLE_JOB)
    print("=== 그래프 실행 완료 ===")
    print("errors:", final.get("errors"))
    print("applicable:", final.get("applicable_coverages"))
    print("missing:", final.get("missing_coverages"))
    print("estimated_range:", final.get("estimated_range"))
    print("judge_failures:", final.get("judge_failures"))
    print("issues:", json.dumps(final.get("issues", []), ensure_ascii=False, indent=2))
    print("\n--- report ---\n", (final.get("report") or "")[:900])
    await close_pool()
    await close_rag_pool()


if __name__ == "__main__":
    asyncio.run(main())
