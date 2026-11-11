"""다중 시나리오 실험 — 커밋된 report_worker 그래프를 4개 케이스로 돌려 결과를 비교한다.

OCR/Kafka 없이 시나리오별 DB row를 심고 그래프를 실행, 핵심 산출(장해율·금액·항목·판례·
폴백 마커)만 요약 출력한다. LLM(qwen3:8b)이 끼어 케이스당 수십 초 걸린다.
"""

from __future__ import annotations

import asyncio
import io
import json
import sys
import uuid
from datetime import date
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core import db  # noqa: E402
from report_worker.graph import build_graph  # noqa: E402

# 공용 특약 세트(가입금액 포함) — C 시나리오만 다른 보험사.
_COVERAGES = ["상해후유장해", "상해입원일당", "수술비특약", "골절진단비"]
_COVERAGE_DETAILS = [
    {"name": "상해후유장해", "type": "disability", "amount": 30000000},
    {"name": "상해입원일당", "type": "per_diem", "amount": 30000},
    {"name": "수술비특약", "type": "surgery", "amount": 500000},
    {"name": "골절진단비", "type": "fracture", "amount": 300000},
]


def _ids(n: int) -> dict[str, str]:
    """시나리오 n의 고정 UUID 묶음(가독성용 접미 번호)."""
    b = f"00000000-0000-0000-0000-0000000{n:03d}"
    return {"report": f"{b}01", "ocr": f"{b}02", "claim": f"{b}03",
            "user": f"{b}11", "ins": f"{b}04"}


SCENARIOS = [
    {
        "n": 101, "name": "A. 십자인대 파열(재현)",
        "insurer": "메리츠화재", "product": "다모아상해보험", "enrolled": date(2022, 5, 1),
        "offered": 1200000, "accident_type": "disability",
        "claim_dx": "우측 슬관절 전방십자인대 파열",
        "masked_text": ("진단서\n진단명: 우측 슬관절 전방십자인대 파열, 반월상연골 파열\n"
                        "상병코드: S83.5\n수술명: 관절경적 전방십자인대 재건술\n"
                        "입원: 14일\n향후 후유장해 평가 필요 소견."),
        "entities": {"icd": "S83.5", "surgery": True, "admission_days": 14},
        "question": "보험금이 적게 나온 것 같아요. 후유장해나 누락 특약 확인해주세요.",
    },
    {
        "n": 102, "name": "B. 대퇴골 골절",
        "insurer": "메리츠화재", "product": "다모아상해보험", "enrolled": date(2022, 5, 1),
        "offered": 2000000, "accident_type": "disability",
        "claim_dx": "우측 대퇴골 골절",
        "masked_text": ("진단서\n진단명: 우측 대퇴골 골절(폐쇄성)\n상병코드: S72.0\n"
                        "수술명: 관혈적 정복술 및 금속내고정술\n입원: 21일\n"
                        "골유합 지연 시 후유장해 평가 필요."),
        "entities": {"icd": "S72.0", "surgery": True, "admission_days": 21},
        "question": "골절로 수술받고 21일 입원했는데 받을 수 있는 특약이 뭔가요?",
    },
    {
        "n": 103, "name": "C. 커버리지 없는 보험사",
        "insurer": "가상손해보험", "product": "없는상해보험", "enrolled": date(2021, 3, 1),
        "offered": 800000, "accident_type": "disability",
        "claim_dx": "좌측 손목 골절",
        "masked_text": ("진단서\n진단명: 좌측 요골 원위부 골절\n상병코드: S52.5\n"
                        "처치: 도수정복 및 석고고정\n입원: 5일."),
        "entities": {"icd": "S52.5", "surgery": False, "admission_days": 5},
        "question": "제 약관이 조회가 안 되는데 보상 가능한가요?",
    },
    {
        "n": 104, "name": "D. 발목 염좌(경미)",
        "insurer": "메리츠화재", "product": "다모아상해보험", "enrolled": date(2022, 5, 1),
        "offered": 100000, "accident_type": "other",
        "claim_dx": "우측 발목 염좌",
        "masked_text": ("진단서\n진단명: 우측 발목 염좌(경도)\n상병코드: S93.4\n"
                        "처치: 통원 물리치료 2주\n수술·입원 없음. 후유장해 없음."),
        "entities": {"icd": "S93.4", "surgery": False, "admission_days": 0},
        "question": "발목 삐어서 통원치료만 했는데 받을 게 있나요?",
    },
]


async def _seed(sc: dict) -> dict[str, str]:
    ids = _ids(sc["n"])
    pool = db.get_pool()
    async with pool.acquire() as c, c.transaction():
        for tbl, key in (("report_issues", "report"), ("report_drafts", "report"),
                         ("reports", "report"), ("user_claims", "claim"),
                         ("user_insurances", "ins"), ("ocr_results", "ocr")):
            col = "report_id" if tbl in ("report_issues", "report_drafts") else "id"
            await c.execute(f"DELETE FROM {tbl} WHERE {col} = $1", uuid.UUID(ids[key]))
        await c.execute(
            "INSERT INTO ocr_results (id, doc_type, masked_text, entities) VALUES ($1,'diagnosis',$2,$3::jsonb)",
            uuid.UUID(ids["ocr"]), sc["masked_text"], json.dumps(sc["entities"]))
        await c.execute(
            "INSERT INTO user_insurances (id,user_id,insurer_name,product_name,match_status,"
            "enrolled_at,coverages,coverage_details) VALUES ($1,$2,$3,$4,'MATCHED',$5,$6,$7::jsonb)",
            uuid.UUID(ids["ins"]), uuid.UUID(ids["user"]), sc["insurer"], sc["product"],
            sc["enrolled"], _COVERAGES, json.dumps(_COVERAGE_DETAILS))
        await c.execute(
            "INSERT INTO user_claims (id,user_id,offered_amount,diagnosis,accident_type) "
            "VALUES ($1,$2,$3,$4,$5)",
            uuid.UUID(ids["claim"]), uuid.UUID(ids["user"]), sc["offered"],
            sc["claim_dx"], sc["accident_type"])
        await c.execute(
            "INSERT INTO reports (id,user_id,claim_id,accident_type,treatment,offered_amount,"
            "question,status) VALUES ($1,$2,$3,$4,$5,$6,$7,'AWAITING_INSPECTION')",
            uuid.UUID(ids["report"]), uuid.UUID(ids["user"]), uuid.UUID(ids["claim"]),
            sc["accident_type"], sc["claim_dx"], sc["offered"], sc["question"])
    return ids


async def main() -> None:
    await db.init_pool()
    graph = build_graph()
    for sc in SCENARIOS:
        ids = await _seed(sc)
        state = {"report_id": ids["report"], "ocr_result_id": ids["ocr"],
                 "claim_id": ids["claim"], "user_ref": ids["user"], "doc_type": "diagnosis"}
        final = await graph.ainvoke(state)

        dx = final.get("diagnosis") or {}
        da = final.get("disability_analysis") or {}
        er = final.get("estimated_range") or {}
        errs = final.get("errors", [])
        print("\n" + "=" * 74)
        print(f"■ {sc['name']}  ({sc['insurer']} / {sc['product']})")
        print("=" * 74)
        print(f"  진단→분류        : {dx.get('diagnosis')} "
              f"(장해검토={dx.get('requires_disability_review')}, 수술={dx.get('surgery')})")
        drv = dx.get("requires_disability_review")
        if drv:
            disp = any(i.get("disputed") for i in da.get("items", []))
            print(f"  후유장해         : {da.get('combined_rate')}% "
                  f"(신뢰도 {da.get('confidence')}, 과대분류교정={disp})")
        else:
            print("  후유장해         : 미검토(분기 안 탐)")
        print(f"  추정 보상범위    : {er.get('min', 0):,} ~ {er.get('max', 0):,} 원 "
              f"(제안 {sc['offered']:,})")
        for it in final.get("payment_breakdown", []):
            print(f"     • {it['name']:8} {it['payout']:>10,}원  ({it['basis']})")
        for ex in final.get("payment_excluded", []):
            print(f"     ✕ {ex['name']:8} {'제외':>10}      ({ex['reason']})")
        refs = final.get("legal_references", [])
        print(f"  판례 근거({len(refs)})     : "
              + " | ".join((r.get("case_title") or "?")[:28] for r in refs[:3]))
        print(f"  errors           : {errs or '없음'}")
    await db.close_pool()


if __name__ == "__main__":
    asyncio.run(main())
