"""테스트 배터리 — 검색 다종 + e2e 다중 시나리오.

    python -m langGraph.test_battery
전제: tempVectorDB 기동 + Ollama 기동. 시나리오는 임시 UUID로 시드→실행→정리.
"""

from __future__ import annotations

import asyncio
import uuid

from core.db import close_pool, get_pool, init_pool

from .graph import build_graph
from .rag.hybrid import close_pool as close_rag_pool
from .rag.hybrid import search

SEARCH_QUERIES = [
    "후유장해 장해등급 보험금",
    "상해 입원일당 지급 사유",
    "보험금을 지급하지 않는 면책 사유",
    "수술비 특약 보장 범위",
    "골절 진단비 지급 조건",
    "암 진단 확정시 보험금",
    "교통사고 변호사 선임비용",
    "보험료 납입 면제 사유",
]

# (라벨, insurer, product, accident_type, diagnosis, masked_text, offered, question)
SCENARIOS = [
    ("A_약관있음_상해", "메리츠화재", "다모아상해보험", "disability",
     "우측 슬관절 전방십자인대 파열",
     "진단서\n환자 홍**(900101-*******)\n진단명: 전방십자인대 파열, 상병 S83.5\n관절경 재건술, 입원 14일.",
     1200000, "보험금이 적게 나온 것 같아요. 누락 특약 확인 원해요."),
    ("B_약관있음_골절", "메리츠화재", "단체안심상해보험", "disability",
     "좌측 요골 원위부 골절",
     "진단서\n진단명: 요골 골절 S52.5\n수술적 정복술, 입원 7일.",
     500000, "골절 진단비랑 수술비 받을 수 있나요?"),
    ("C_약관없음_상품", "메리츠화재", "존재하지않는상품XYZ", "disability",
     "발목 인대 손상",
     "진단서\n진단명: 발목 인대 파열.",
     300000, "보장 되나요?"),
    ("D_타보험사", "DB손해보험", "프로미라이프", "medical_indemnity",
     "급성 충수염 복강경 수술",
     "진단서\n진단명: 급성 충수염 K35.\n복강경 수술, 입원 5일.",
     800000, "실손 청구 가능한가요?"),
    ("E_PII포함", "메리츠화재", "다모아상해보험", "disability",
     "경추 염좌",
     "진단서\n환자 김철수 860312-1948571 연락처 010-9876-5432 계좌 110-234-567890\n진단명: 경추 염좌.",
     200000, "교통사고 후유증 보상 되나요?"),
    ("F_도메인외", "메리츠화재", "다모아상해보험", "disability",
     "무릎 타박상",
     "비트코인 시세랑 부동산 투자 문의입니다.",
     0, "코인 수익 보장 되나요?"),
    ("G_장해명시", "메리츠화재", "다모아상해보험", "disability",
     "우측 슬관절 전방십자인대·반월상연골 파열",
     "진단서\n진단명: 우측 슬관절 전방십자인대 파열, 반월상연골 파열, 상병 S83.5\n"
     "관절경 재건술 시행, 입원 21일. 치료 종결 후에도 우측 무릎 관절의 운동범위 제한이 "
     "영구적으로 남아 후유장해가 예상되며 장해진단 및 장해지급률 평가가 필요한 소견임.",
     900000, "영구 후유장해가 남았는데 장해지급률이 어떻게 산정되나요?"),
]


async def _seed(label, insurer, product, atype, diagnosis, masked, offered, question):
    rid, oid, cid, uid = uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    pool = get_pool()
    async with pool.acquire() as c:
        await c.execute(
            "INSERT INTO ocr_results (id, job_id, doc_type, masked_text, entities) VALUES ($1,$2,'diagnosis',$3,'{}'::jsonb)",
            oid, uuid.uuid4(), masked,
        )
        await c.execute(
            "INSERT INTO user_insurances (id, user_id, insurer_name, product_name, match_status, coverages) "
            "VALUES ($1,$2,$3,$4,'MATCHED',$5)",
            uuid.uuid4(), uid, insurer, product, ["상해후유장해", "상해입원일당", "수술비특약", "골절진단비"],
        )
        await c.execute(
            "INSERT INTO user_claims (id, user_id, offered_amount, diagnosis, accident_type) VALUES ($1,$2,$3,$4,$5)",
            cid, uid, offered, diagnosis, atype,
        )
        await c.execute(
            "INSERT INTO reports (id, user_id, claim_id, accident_type, treatment, offered_amount, question, status) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,'AWAITING_INSPECTION')",
            rid, uid, cid, atype, diagnosis, offered, question,
        )
    return str(rid), str(oid), str(cid), uid


async def _cleanup(rid, oid, cid, uid):
    pool = get_pool()
    async with pool.acquire() as c:
        await c.execute("DELETE FROM report_issues WHERE report_id=$1", uuid.UUID(rid))
        await c.execute("DELETE FROM report_drafts WHERE report_id=$1", uuid.UUID(rid))
        await c.execute("DELETE FROM reports WHERE id=$1", uuid.UUID(rid))
        await c.execute("DELETE FROM user_claims WHERE id=$1", uuid.UUID(cid))
        await c.execute("DELETE FROM user_insurances WHERE user_id=$1", uid)
        await c.execute("DELETE FROM ocr_results WHERE id=$1", uuid.UUID(oid))


async def search_battery():
    print("\n########## 검색 배터리 ##########")
    for q in SEARCH_QUERIES:
        res = await search(q, namespaces=["terms"], top_k=3)
        top = res["ranked_chunks"][0] if res["ranked_chunks"] else None
        if top:
            t = " ".join(top["text"].split())[:55]
            print(f"  ✓ {q:28s} → {top['product_name']} {top.get('article_number') or ''} | {t}")
        else:
            print(f"  ✗ {q:28s} → 결과없음")


async def e2e_battery():
    print("\n########## e2e 시나리오 배터리 ##########")
    app = build_graph()
    pool = get_pool()
    for sc in SCENARIOS:
        label = sc[0]
        ids = await _seed(*sc)
        rid, oid, cid, uid = ids
        try:
            job = {"report_id": rid, "ocr_result_id": oid, "claim_id": cid,
                   "user_ref": str(uid), "doc_type": "diagnosis"}
            final = await app.ainvoke(job)
            async with pool.acquire() as c:
                saved = await c.fetchval("SELECT count(*) FROM report_drafts WHERE report_id=$1", uuid.UUID(rid))
            errs = final.get("errors", [])
            branch = "terms_parse(약관없음)" if any("runtime_parse_stub" in e for e in errs) else "직행(약관있음)"
            blocked = any("input_blocked" in e for e in errs)
            er = final.get("estimated_range", {})
            da = final.get("disability_analysis") or {}
            requires = bool((final.get("diagnosis") or {}).get("requires_disability_review"))
            print(f"\n[{label}]")
            print(f"  분기: {branch} | 입력차단: {blocked}")
            print(f"  applicable: {final.get('applicable_coverages')}")
            print(f"  estimated_range: {er.get('min')}~{er.get('max')}")
            print(f"  장해검토: {requires} | 장해분기실행: {bool(da)} | 지급률: {da.get('combined_rate')}% "
                  f"| 신뢰도: {da.get('confidence')} | 근거수: {len(da.get('citations', []))}")
            print(f"  draft 저장: {'OK' if saved else 'FAIL'} | issues: {len(final.get('issues',[]))} | errors: {errs}")
            # 라우팅 불변식: 후유장해 검토 필요면 반드시 장해 서브그래프가 실행돼 결과를 남겨야 한다
            assert (not requires) or bool(da), f"{label}: requires_disability_review인데 장해 분기 미실행"
        finally:
            await _cleanup(*ids)


async def main():
    await init_pool()
    await search_battery()
    await e2e_battery()
    await close_pool()
    await close_rag_pool()


if __name__ == "__main__":
    asyncio.run(main())
