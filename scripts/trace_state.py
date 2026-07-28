"""노드 한 바퀴 돌면서 각 노드가 state에 얹는 키/값을 찍는 실험 스크립트.

    PYTHONPATH=src python scripts/trace_state.py
시나리오 A(약관있음_상해)를 시드→astream(updates)로 노드별 델타 출력→정리.
"""

from __future__ import annotations

import asyncio
import sys

from battery import SCENARIOS, _cleanup, _seed
from core.db import close_pool, init_pool
from report_worker.graph import build_graph
from report_worker.rag.hybrid import close_pool as close_rag_pool

sys.stdout.reconfigure(encoding="utf-8")


def _short(v, width=300):
    s = repr(v)
    return s if len(s) <= width else s[:width] + f"… (+{len(s) - width}자)"


async def main():
    await init_pool()
    sc = SCENARIOS[0]  # A_약관있음_상해
    ids = await _seed(*sc)
    rid, oid, cid, uid = ids
    app = build_graph()
    job = {
        "report_id": rid,
        "ocr_result_id": oid,
        "claim_id": cid,
        "user_ref": str(uid),
        "doc_type": "diagnosis",
    }
    try:
        print(f"### 시나리오 {sc[0]} — 노드별 state 델타 ###\n")
        step = 0
        async for update in app.astream(job, stream_mode="updates"):
            for node, delta in update.items():
                step += 1
                print(f"[{step:02d}] ── {node} ──────────────")
                if not delta:
                    print("     (state 변경 없음)")
                    continue
                for k, v in delta.items():
                    print(f"     {k}: {_short(v)}")
                print()
    finally:
        await _cleanup(*ids)
        await close_pool()
        await close_rag_pool()


if __name__ == "__main__":
    asyncio.run(main())
