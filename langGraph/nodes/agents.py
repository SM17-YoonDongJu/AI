"""순차 노드 (async, 실제 의존: db·rag·ai_client·guardrail).

각 노드는 ReportState 부분 dict만 반환(LangGraph가 머지). 노드 내부 실패는 errors에
기록하고 부분결과로 진행한다(전체 실패 회피, 이슈 #11 방침).
"""

from __future__ import annotations

import functools
import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import guardrail
from core import ai_client, db
from report_worker.disability_rules import combine_disability_rate

from ..rag import hybrid
from ..state import ReportState


def _err(state: ReportState, msg: str) -> list[str]:
    return list(state.get("errors", [])) + [msg]


def safe_node(fn: Callable[[ReportState], Awaitable[dict[str, Any]]]):
    """노드 예외를 삼켜 errors에 기록하고 부분결과로 진행한다(이슈 #11 방침).

    노드 본문이 던지면 그래프 전체가 죽는 대신 {"errors": [...]}만 머지된다.
    다운스트림 노드는 전부 state.get(key, default)로 읽으므로 누락 키에 안전하다.
    """

    @functools.wraps(fn)
    async def wrapper(state: ReportState) -> dict[str, Any]:
        try:
            return await fn(state)
        except Exception as e:
            return {"errors": _err(state, f"{fn.__name__}_failed:{type(e).__name__}:{e}")}

    return wrapper


def _as_str_list(v: Any) -> list[str]:
    """LLM이 list[str] 대신 list[dict]/str로 줄 때 안전하게 문자열 리스트로 정규화."""
    if v is None:
        return []
    if isinstance(v, str):
        return [v]
    out: list[str] = []
    for x in v if isinstance(v, list) else [v]:
        if isinstance(x, dict):
            out.append(str(x.get("name") or x.get("title") or x.get("특약") or next(iter(x.values()), "")))
        else:
            out.append(str(x))
    return [s for s in out if s]


# ── load_context: DB 조회로 사고/약관 컨텍스트 조립 ──────────────
@safe_node
async def load_context(state: ReportState) -> dict[str, Any]:
    pool = db.get_pool()
    async with pool.acquire() as c:
        ocr = await c.fetchrow(
            "SELECT masked_text, entities FROM ocr_results WHERE id = $1",
            uuid.UUID(state["ocr_result_id"]),
        )
        rep = await c.fetchrow(
            "SELECT accident_type, treatment, offered_amount, question, claim_id "
            "FROM reports WHERE id = $1",
            uuid.UUID(state["report_id"]),
        )
        claim = None
        if rep and rep["claim_id"]:
            claim = await c.fetchrow(
                "SELECT diagnosis, accident_date, accident_type, offered_amount, description, "
                "hospitalization FROM user_claims WHERE id = $1",
                rep["claim_id"],
            )
        ins = await c.fetchrow(
            "SELECT insurer_name, product_name, coverages FROM user_insurances "
            "WHERE user_id = (SELECT user_id FROM reports WHERE id = $1) LIMIT 1",
            uuid.UUID(state["report_id"]),
        )

    errors = list(state.get("errors", []))
    if not ocr:
        errors.append("ocr_result_missing")
    entities = {}
    if ocr and ocr["entities"]:
        entities = ocr["entities"] if isinstance(ocr["entities"], dict) else json.loads(ocr["entities"])

    case_info = {
        "accident_type": (rep["accident_type"] if rep else None) or (claim["accident_type"] if claim else None),
        "diagnosis": (claim["diagnosis"] if claim else None) or (rep["treatment"] if rep else None),
        "offered_amount": (rep["offered_amount"] if rep else None),
        "question": (rep["question"] if rep else None),
        "description": (claim["description"] if claim else None),
        "insurer": ins["insurer_name"] if ins else None,
        "product_name": ins["product_name"] if ins else None,
    }
    return {
        "case_info": case_info,
        "masked_text": (ocr["masked_text"] if ocr else ""),
        "entities": entities,
        "subscribed_coverages": list(ins["coverages"]) if ins and ins["coverages"] else [],
        "errors": errors,
    }


# ── 입력 가드레일 ──────────────────────────────────────────────
@safe_node
async def input_guardrail(state: ReportState) -> dict[str, Any]:
    g = await guardrail.guard_input(state.get("masked_text", ""))
    out: dict[str, Any] = {"masked_text": g.masked_text}
    if g.blocked:
        out["errors"] = _err(state, f"input_blocked:{g.reason}")
    return out


# ── 진단/사고 분류 (LLM) ───────────────────────────────────────
_ACCIDENT_TYPES = "medical_indemnity, traffic, disability, cancer_diagnosis, fire, liability, other"


@safe_node
async def diagnosis(state: ReportState) -> dict[str, Any]:
    text = state.get("masked_text", "")
    res = await ai_client.chat_json(
        [
            {"role": "system", "content": "너는 보험 손해사정 진단 분석가다. 의료문서에서 정보를 추출해 JSON만 출력한다."},
            {
                "role": "user",
                "content": (
                    f"[문서]\n{text}\n\n"
                    f"다음 JSON 키로만 답하라: diagnosis(진단명 str), icd_codes(list), "
                    f"accident_type(아래 중 하나: {_ACCIDENT_TYPES}), surgery(bool), "
                    f"hospitalization(bool), requires_disability_review(bool 후유장해 검토 필요)."
                ),
            },
        ]
    )
    if not isinstance(res, dict) or not res:
        return {
            "diagnosis": {"diagnosis": state.get("case_info", {}).get("diagnosis"), "accident_type": "other", "requires_disability_review": False},
            "errors": _err(state, "diagnosis_llm_failed"),
        }
    res.setdefault("accident_type", state.get("case_info", {}).get("accident_type") or "other")
    res.setdefault("requires_disability_review", False)
    return {"diagnosis": res}


# ── 분기: 사용자 약관이 우리 DB(policy_chunks)에 있나? ──────────
async def policy_in_db(state: ReportState) -> str:
    insurer = state.get("case_info", {}).get("insurer")
    product = state.get("case_info", {}).get("product_name")
    if not insurer:
        return "terms_parse"
    try:
        pool = db.get_pool()
        async with pool.acquire() as c:
            if product:
                n = await c.fetchval(
                    "SELECT count(*) FROM policy_chunks WHERE insurer = $1 AND product_name = $2",
                    insurer, product,
                )
            else:
                n = await c.fetchval("SELECT count(*) FROM policy_chunks WHERE insurer = $1", insurer)
    except Exception:  # DB 조회 실패 시 안전하게 런타임 파싱 경로로
        return "terms_parse"
    return "coverage_parse" if (n or 0) > 0 else "terms_parse"


# ── 분기: 입력 가드레일 차단 여부 → 차단 시 파이프라인 단락 ─────
def route_after_input(state: ReportState) -> str:
    """input_guardrail이 도메인외/차단을 표시하면 LLM 파이프라인을 건너뛴다."""
    if any(str(e).startswith("input_blocked") for e in state.get("errors", [])):
        return "blocked"
    return "diagnosis"


# ── 약관 파싱 (분기 No 전용, 실험은 스텁) ──────────────────────
@safe_node
async def terms_parse(state: ReportState) -> dict[str, Any]:
    # 실제: 사용자 업로드 약관을 PDFPlumber/VLM 파싱 → 청킹 → 임시 임베딩.
    # 실험에서는 무거워 스킵하고 폴백 기록.
    return {"errors": _err(state, "policy_not_in_db:runtime_parse_stub")}


# ── 특약 파싱 (가입 특약 확정) ─────────────────────────────────
@safe_node
async def coverage_parse(state: ReportState) -> dict[str, Any]:
    return {"subscribed_coverages": state.get("subscribed_coverages", [])}


# ── 특약·약관 분석 (Hybrid RAG + LLM) ──────────────────────────
@safe_node
async def coverage_analysis(state: ReportState) -> dict[str, Any]:
    ci = state.get("case_info", {})
    dx = state.get("diagnosis") or {}
    dx_name = dx.get("diagnosis") or ci.get("diagnosis") or ""
    icd = " ".join(dx.get("icd_codes") or [])
    query = f"{dx_name} {icd} {ci.get('question','')}".strip()
    res = await hybrid.search(
        query, namespaces=["terms"], top_k=8,
        insurer=ci.get("insurer"), product=ci.get("product_name"),
    )
    chunks = res["ranked_chunks"]
    if not chunks:
        return {"retrieved_clauses": [], "errors": _err(state, "rag_empty")}

    ctx = "\n---\n".join(f"{c['source_ref']} {c['text'][:300]}" for c in chunks[:6])
    analysis = await ai_client.chat_json(
        [
            {"role": "system", "content": "너는 보험 약관 분석가다. 가입 특약과 약관 조항을 대조해 JSON만 출력한다."},
            {
                "role": "user",
                "content": (
                    f"[가입 특약]\n{state.get('subscribed_coverages', [])}\n\n"
                    f"[사고]\n{dx_name}\n\n[약관 조항]\n{ctx}\n\n"
                    'JSON 키: applicable(적용 가능 특약 list), missing(청구 누락 가능 특약 list), '
                    'analysis(면책·감액 등 분석 str).'
                ),
            },
        ]
    )
    analysis = analysis if isinstance(analysis, dict) else {}
    return {
        "retrieved_clauses": chunks,
        "applicable_coverages": _as_str_list(analysis.get("applicable")),
        "missing_coverages": _as_str_list(analysis.get("missing")),
        "coverage_analysis": {"analysis": str(analysis.get("analysis", "")), "citations": res["citations"][:6]},
    }


# ── 판례 검색 (case_chunks 미적재 → 폴백) ──────────────────────
@safe_node
async def case_search(state: ReportState) -> dict[str, Any]:
    ci = state.get("case_info", {})
    dx = state.get("diagnosis") or {}
    dx_name = dx.get("diagnosis") or ci.get("diagnosis") or ""
    res = await hybrid.search(dx_name, namespaces=["case"], top_k=4)
    refs = res["ranked_chunks"]
    if not refs:
        return {"legal_references": [], "errors": _err(state, "case_data_missing")}
    return {"legal_references": refs}


# ── 분기: 후유장해 검토 필요 시 장해 서브그래프로 ──────────────
def route_after_case(state: ReportState) -> str:
    """진단이 requires_disability_review면 장해 노드로, 아니면 보험금 계산 직행."""
    if (state.get("diagnosis") or {}).get("requires_disability_review"):
        return "disability"
    return "payment_calc"


# ── 장해 분류·지급률 추출 (RAG + LLM, 숫자는 약관에서만) ────────
@safe_node
async def disability_rag(state: ReportState) -> dict[str, Any]:
    ci = state.get("case_info", {})
    dx = state.get("diagnosis") or {}
    dx_name = dx.get("diagnosis") or ci.get("diagnosis") or ""
    icd = " ".join(dx.get("icd_codes") or [])
    query = f"{dx_name} {icd} 후유장해 장해분류표 지급률".strip()
    res = await hybrid.search(
        query, namespaces=["terms"], top_k=8,
        insurer=ci.get("insurer"), product=ci.get("product_name"),
    )
    ranked = res.get("ranked_chunks", [])
    # 장해분류표(schedule) 청크 선별 — chunk_type 우선, 없으면 헤더 휴리스틱
    sched = [c for c in ranked if c.get("chunk_type") == "schedule"]
    if not sched:
        sched = [c for c in ranked if "장해의 분류" in (c.get("text") or "")]

    caveat = "가입금액 미보유로 절대 보험금 불가·약관표 위치정렬 한계 — 지급률은 추정"
    existing = state.get("retrieved_clauses", [])
    if not sched:
        return {
            "disability_analysis": {
                "items": [], "combined_rate": 0.0, "rule_notes": [],
                "citations": [], "confidence": "low", "caveat": "장해분류표 미검색",
            },
            "retrieved_clauses": existing,
            "errors": _err(state, "disability_schedule_missing"),
        }

    sched_text = "\n".join((c.get("text") or "") for c in sched)
    ctx = "\n---\n".join(f"{c['source_ref']}\n{(c.get('text') or '')[:800]}" for c in sched[:6])
    raw = await ai_client.chat_json(
        [
            {"role": "system", "content": (
                "너는 보험 약관 장해분류표 분석가다. 제공된 [약관 장해분류표 원문]에서만 근거를 찾아 "
                "사고를 분류하고 지급률을 추출한다. 표에 없는 지급률은 절대 만들지 마라. JSON만 출력한다."
            )},
            {"role": "user", "content": (
                f"[사고/진단]\n{dx_name} / ICD {icd}\n\n[약관 장해분류표 원문]\n{ctx}\n\n"
                'JSON 키: items(배열, 각 원소 = injury(str), '
                'body_region(눈·귀·코·씹기말하기·척추·체간골·팔·다리·손가락·발가락·흉복부장기·신경계정신 중 하나), '
                'category_label(원문 항목 텍스트 그대로 복사), rate(number 지급률 %), '
                'rate_quote(rate 숫자가 등장한 원문 구절 그대로 복사), temporary(bool 한시장해), '
                'temporary_years(number 또는 null), citation(위 원문 source_ref 중 하나)), '
                'uncertain(bool), notes(str).\n'
                '규칙: rate는 원문에 실제 등장하는 숫자만. category_label·rate_quote는 요약 말고 복사. '
                '적합 항목 없으면 items=[] uncertain=true. 추측으로 숫자 만들지 마라.'
            )},
        ]
    )
    raw = raw if isinstance(raw, dict) else {}
    uncertain = bool(raw.get("uncertain"))

    items: list[dict[str, Any]] = []
    notes: list[str] = []
    for it in raw.get("items", []) if isinstance(raw.get("items"), list) else []:
        if not isinstance(it, dict):
            continue
        try:
            rate_f = float(it.get("rate"))
        except (TypeError, ValueError):
            continue
        quote = str(it.get("rate_quote") or "")
        # 결정론 백스톱: 지급률 숫자가 인용 원문에 실제로 존재해야 인정
        verified = bool(quote) and (str(int(rate_f)) in sched_text)
        injury = str(it.get("injury") or "")
        if not verified:
            notes.append(f"미검증 지급률 제외: {injury} {rate_f}%")
        items.append({
            "injury": injury,
            "body_region": str(it.get("body_region") or "기타"),
            "category_label": str(it.get("category_label") or ""),
            "rate": rate_f,
            "rate_quote": quote,
            "temporary": bool(it.get("temporary")),
            "temporary_years": it.get("temporary_years"),
            "citation": str(it.get("citation") or ""),
            "verified": verified,
        })

    verified_n = sum(1 for i in items if i["verified"])
    confidence = "high" if (items and verified_n == len(items) and not uncertain) else (
        "medium" if verified_n else "low"
    )
    citations = list(dict.fromkeys(i["citation"] for i in items if i["citation"])) or res.get("citations", [])[:6]

    seen = {c.get("source_ref") for c in existing}
    merged = existing + [c for c in sched if c.get("source_ref") not in seen]
    return {
        "disability_analysis": {
            "items": items, "rule_notes": notes, "citations": citations,
            "confidence": confidence, "caveat": caveat,
        },
        "retrieved_clauses": merged,
    }


# ── 장해지급률 결정론 합산 (LLM 없음) ──────────────────────────
@safe_node
async def disability_calc(state: ReportState) -> dict[str, Any]:
    da = state.get("disability_analysis", {})
    verified = [i for i in da.get("items", []) if i.get("verified")]
    result = combine_disability_rate(verified)
    return {
        "disability_analysis": {
            **da,
            "combined_rate": result["combined_rate"],
            "normalized_items": result["normalized_items"],
            "rule_notes": list(da.get("rule_notes", [])) + result["rule_notes"],
        }
    }


# ── 보험금 계산 (추정 범위, 단정 금지) ─────────────────────────
@safe_node
async def payment_calc(state: ReportState) -> dict[str, Any]:
    offered = state.get("case_info", {}).get("offered_amount") or 0
    base = max(offered, 0)
    # 가입금액 미보유 → 절대 보험금은 못 냄. 장해지급률이 있으면 상단 배수를 지급률에 비례해
    # 확장(0%→×1.0, 100%→×1.8)하고, 없으면 기존 ±범위(×1.8)로 폴백. 모두 '추정'.
    rate = float((state.get("disability_analysis") or {}).get("combined_rate") or 0.0)
    factor_hi = 1.0 + min(rate, 100.0) / 100.0 * 0.8 if rate > 0 else 1.8
    lo = int(base * 1.0)
    hi = int(base * factor_hi) if base else 0
    return {"estimated_range": {"min": lo, "max": hi}}


# ── 리포트 통합 (8섹션 + issues, 생성 가드레일) ────────────────
@safe_node
async def report_compose(state: ReportState) -> dict[str, Any]:
    ci = state.get("case_info", {})
    ca = state.get("coverage_analysis", {})
    dx = state.get("diagnosis") or {}
    dx_name = dx.get("diagnosis") or ci.get("diagnosis") or ""
    da = state.get("disability_analysis") or {}
    disability_line = (
        f"추정 합산 장해지급률 {da.get('combined_rate', 0)}% "
        f"(신뢰도 {da.get('confidence', '-')}, 근거 {', '.join(da.get('citations', []))}) — "
        f"규칙 {'; '.join(da.get('rule_notes', [])) or '-'}. ※ {da.get('caveat', '')}"
        if da.get("items")
        else "해당 없음(후유장해 미검토)"
    )
    body = await ai_client.chat(
        [
            {"role": "system", "content": "너는 보험 손해사정 리포트 작성자다. 사실 주장에는 약관 조항 인용을 포함하고, 금액은 단정하지 말고 범위로 쓴다."},
            {
                "role": "user",
                "content": (
                    f"사고: {dx_name} / 질문: {ci.get('question')}\n"
                    f"적용 특약: {state.get('applicable_coverages')}\n"
                    f"누락 가능: {state.get('missing_coverages')}\n"
                    f"분석: {ca.get('analysis')}\n"
                    f"추정범위: {state.get('estimated_range')}\n"
                    f"장해지급률: {disability_line}\n"
                    f"인용: {ca.get('citations')}\n\n"
                    "위 내용으로 손해사정 리포트 본문을 작성하라(사건요약/적용특약/분쟁포인트/추가확인 필요)."
                ),
            },
        ]
    )
    body = guardrail.guard_generation(body)

    issues = await ai_client.chat_json(
        [
            {"role": "system", "content": "보험 리포트의 핵심 쟁점을 JSON 배열로 추출한다. JSON만."},
            {
                "role": "user",
                "content": (
                    f"적용특약 {state.get('applicable_coverages')}, 누락 {state.get('missing_coverages')}, "
                    f"분석 {ca.get('analysis')}.\n"
                    '형식: {"issues":[{"title":str,"description":str,"ai_status":"CONFIRMED|TRUSTED|INFO","tags":[str]}]}'
                ),
            },
        ]
    )
    issue_list = issues.get("issues", []) if isinstance(issues, dict) else []

    sections = {
        "1_사건요약": dx_name,
        "2_적용특약": ", ".join(state.get("applicable_coverages", [])),
        "3_누락가능특약": ", ".join(state.get("missing_coverages", [])),
        "4_약관근거": ", ".join(ca.get("citations", [])),
        "4b_판례근거": "; ".join(
            f"{c.get('article_number') or ''} {c.get('product_name') or ''}".strip()
            for c in state.get("legal_references", [])[:5]
        ) or "관련 판례 없음",
        "5_추정보상범위": str(state.get("estimated_range", {})),
        "5b_장해지급률": disability_line,
        "6_본문": body,
        "7_추가확인필요": "; ".join(state.get("errors", [])) or "없음",
    }
    report_md = "\n\n".join(f"## {k}\n{v}" for k, v in sections.items())
    return {"sections": sections, "issues": issue_list, "report": report_md}


# ── 출력 가드레일 (고지문 + LLM Judge) ─────────────────────────
@safe_node
async def output_guardrail(state: ReportState) -> dict[str, Any]:
    g = await guardrail.guard_output(
        state.get("report", ""), run_judge=True, chunks=state.get("retrieved_clauses", [])
    )
    return {"report": g.final_text, "judge_failures": g.judge_failures}


# ── 영구 저장 (report_drafts/reports/report_issues) ────────────
@safe_node
async def persist(state: ReportState) -> dict[str, Any]:
    # 약관 인용 + 판례 근거를 합쳐 basis_terms_precedents 구성
    terms_cites = state.get("coverage_analysis", {}).get("citations", [])
    case_refs = [c.get("source_ref") for c in state.get("legal_references", []) if c.get("source_ref")]
    da_cites = (state.get("disability_analysis") or {}).get("citations", [])
    basis = terms_cites + case_refs + da_cites

    draft = {
        "sections": state.get("sections", {}),
        "estimated_range": state.get("estimated_range", {}),
        "disclaimer": guardrail.DISCLAIMER,
        "judge_failures": state.get("judge_failures", []),
        "issues": state.get("issues", []),
        "applicable_guarantees": state.get("applicable_coverages", []),
        "omitted_special_contract": state.get("missing_coverages", []),
        "basis_terms_precedents": basis,
        "legal_references": case_refs,   # 판례·분쟁조정 근거(별도 보존)
        "disability": state.get("disability_analysis", {}),   # 장해지급률·근거(P1)
        "errors": state.get("errors", []),
    }
    rid = uuid.UUID(state["report_id"])
    er = state.get("estimated_range", {})
    pool = db.get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            await c.execute(
                """INSERT INTO report_drafts (report_id, draft, status)
                   VALUES ($1, $2::jsonb, 'draft')
                   ON CONFLICT (report_id) DO UPDATE SET draft = EXCLUDED.draft, status = 'draft'""",
                rid, json.dumps(draft, ensure_ascii=False),
            )
            await c.execute(
                """UPDATE reports SET
                     applicable_guarantees = $2, omitted_special_contract = $3,
                     basis_terms_precedents = $4, claimed_min_amount = $5, claimed_max_amount = $6,
                     status = 'AWAITING_ADOPTION', updated_at = now()
                   WHERE id = $1""",
                rid,
                state.get("applicable_coverages", []),
                state.get("missing_coverages", []),
                basis,
                er.get("min"), er.get("max"),
            )
            await c.execute("DELETE FROM report_issues WHERE report_id = $1", rid)
            for it in state.get("issues", []):
                await c.execute(
                    """INSERT INTO report_issues (id, report_id, title, description, ai_status, tags)
                       VALUES ($1,$2,$3,$4,$5,$6)""",
                    uuid.uuid4(), rid,
                    str(it.get("title", ""))[:200], str(it.get("description", "")),
                    (it.get("ai_status") or "INFO"),
                    [str(t) for t in (it.get("tags") or [])],
                )
    return {"errors": state.get("errors", [])}
