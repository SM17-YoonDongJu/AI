"""로컬 실험 러너 — OCR/Kafka 없이 report_worker 그래프를 직접 태워 state 흐름을 관찰한다.

흐름: init_pool → seed_sample.sql 적재 → build_graph().astream(SAMPLE_STATE)
각 노드가 끝날 때마다 그 노드가 state에 merge한 '델타(변경분)'만 찍는다(stream_mode="updates").
Kafka·Spring 불필요. .env(DATABASE_URL=5433/aiengine, Ollama qwen3:8b)를 그대로 사용한다.

실행: 프로젝트 루트에서  uv run python run_local.py   (또는  python run_local.py)
"""

from __future__ import annotations

import asyncio
import io
import sys
from pathlib import Path

# Windows 콘솔에서 한글 깨짐 방지
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# src 레이아웃을 import 경로에 추가
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core import db  # noqa: E402
from report_worker.graph import build_graph  # noqa: E402

# seed_sample.sql 이 넣는 리포트 1건(메리츠 다모아상해보험 · 전방십자인대 파열 · 후유장해 검토).
SAMPLE_STATE = {
    "report_id": "00000000-0000-0000-0000-000000000001",
    "ocr_result_id": "00000000-0000-0000-0000-000000000002",
    "claim_id": "00000000-0000-0000-0000-000000000003",
    "user_ref": "00000000-0000-0000-0000-000000000011",
    "doc_type": "diagnosis",
}

_SEED = Path(__file__).parent / "tempVectorDB" / "seed_sample.sql"


def _fmt(value: object, width: int = 300) -> str:
    """state 값 1개를 한 줄로 압축 표시(리스트는 길이, 긴 문자열은 자름)."""
    if isinstance(value, list):
        head = ", ".join(_fmt(v, 60) for v in value[:3])
        more = f" …(+{len(value) - 3})" if len(value) > 3 else ""
        return f"[{len(value)}] {head}{more}"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{k}={_fmt(v, 40)}" for k, v in value.items()) + "}"
    s = str(value).replace("\n", " ⏎ ")
    return s if len(s) <= width else s[:width] + f"…(+{len(s) - width}자)"


async def main() -> None:
    await db.init_pool()
    pool = db.get_pool()

    # 1) 시드 적재(멱등: seed_sample.sql 이 DELETE 후 INSERT)
    async with pool.acquire() as c:
        await c.execute(_SEED.read_text(encoding="utf-8"))
    print("✓ 시드 적재 완료 (report 00…01 / 메리츠 다모아상해보험 / 전방십자인대 파열)\n")

    # 2) 그래프 실행 — 노드별 델타 스트리밍
    graph = build_graph()
    print("입력 state:")
    for k, v in SAMPLE_STATE.items():
        print(f"    {k:16} = {_fmt(v)}")
    print("\n" + "=" * 72)
    print("노드별 state 델타 (각 노드가 채운 키만)")
    print("=" * 72)

    merged: dict = dict(SAMPLE_STATE)
    step = 0
    async for update in graph.astream(SAMPLE_STATE, stream_mode="updates"):
        for node, delta in update.items():
            step += 1
            print(f"\n[{step:02}] ▶ {node}")
            if not delta:
                print("       (state 변경 없음 — 분기/패스스루)")
                continue
            for k, v in delta.items():
                # errors 는 누적 리스트라 '새로 추가된 항목'만 보여준다
                if k == "errors":
                    prev = merged.get("errors", [])
                    added = v[len(prev):] if len(v) >= len(prev) else v
                    if added:
                        print(f"       + errors      {_fmt(added)}")
                    merged["errors"] = v
                    continue
                print(f"       · {k:14} {_fmt(v)}")
                merged[k] = v

    # 3) 최종 요약
    print("\n" + "=" * 72)
    print("최종 state 요약")
    print("=" * 72)
    er = merged.get("estimated_range", {})
    da = merged.get("disability_analysis", {})
    print(f"  진단          : {(merged.get('diagnosis') or {}).get('diagnosis')}")
    print(f"  적용 특약     : {_fmt(merged.get('applicable_coverages', []))}")
    print(f"  누락 가능특약 : {_fmt(merged.get('missing_coverages', []))}")
    print(f"  장해 지급률   : {da.get('combined_rate')}% (신뢰도 {da.get('confidence')})")
    print(f"  추정 보상범위 : {er.get('min'):,} ~ {er.get('max'):,} 원" if er else "  추정 보상범위 : -")
    print("  ── 특약별 산출 ──")
    for it in merged.get("payment_breakdown", []):
        print(f"     • {it['name']:10} {it['payout']:>12,} 원   ({it['basis']})")
    for ex in merged.get("payment_excluded", []):
        print(f"     ✕ {ex['name']:10} {'제외':>12}       ({ex['reason']})")
    print(f"  판례 근거     : {_fmt(merged.get('legal_references', []))}")
    print(f"  섹션          : {list((merged.get('sections') or {}).keys())}")
    print(f"  errors        : {_fmt(merged.get('errors', []))}")

    # 4) DB에 실제 저장됐는지 확인
    async with pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT status, claimed_min_amount, claimed_max_amount, "
            "array_length(applicable_guarantees,1) AS n_appl "
            "FROM reports WHERE id = $1",
            __import__("uuid").UUID(SAMPLE_STATE["report_id"]),
        )
    print(f"\n  reports 저장  : status={row['status']} "
          f"금액={row['claimed_min_amount']}~{row['claimed_max_amount']} "
          f"적용특약수={row['n_appl']}")

    await db.close_pool()


if __name__ == "__main__":
    asyncio.run(main())
