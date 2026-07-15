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
from core.logging import get_logger
from report_worker.disability_rules import combine_disability_rate

from ..rag import hybrid
from ..state import ReportState

logger = get_logger(__name__)


def _err(state: ReportState, msg: str) -> list[str]:
    return [*state.get("errors", []), msg]


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
            out.append(
                str(x.get("name") or x.get("title") or x.get("특약") or next(iter(x.values()), ""))
            )
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
            "SELECT insurer_name, product_name, coverages, coverage_details, enrolled_at "
            "FROM user_insurances "
            "WHERE user_id = (SELECT user_id FROM reports WHERE id = $1) LIMIT 1",
            uuid.UUID(state["report_id"]),
        )

    errors = list(state.get("errors", []))
    if not ocr:
        errors.append("ocr_result_missing")
    entities = {}
    if ocr and ocr["entities"]:
        entities = (
            ocr["entities"] if isinstance(ocr["entities"], dict) else json.loads(ocr["entities"])
        )

    case_info = {
        "accident_type": (rep["accident_type"] if rep else None)
        or (claim["accident_type"] if claim else None),
        "diagnosis": (claim["diagnosis"] if claim else None) or (rep["treatment"] if rep else None),
        "offered_amount": (rep["offered_amount"] if rep else None),
        "question": (rep["question"] if rep else None),
        "description": (claim["description"] if claim else None),
        "insurer": ins["insurer_name"] if ins else None,
        "product_name": ins["product_name"] if ins else None,
        "enrolled_at": ins["enrolled_at"]
        if ins
        else None,  # 가입일(date|None) — 표준표 버전 매칭용
    }
    coverage_details = []
    if ins and ins["coverage_details"]:
        cd = ins["coverage_details"]
        coverage_details = cd if isinstance(cd, list) else json.loads(cd)
    return {
        "case_info": case_info,
        "masked_text": (ocr["masked_text"] if ocr else ""),
        "entities": entities,
        "subscribed_coverages": list(ins["coverages"]) if ins and ins["coverages"] else [],
        "coverage_details": coverage_details,
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
            {
                "role": "system",
                "content": "너는 보험 손해사정 진단 분석가다. 의료문서에서 정보를 추출해 JSON만 출력한다.",
            },
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
            "diagnosis": {
                "diagnosis": state.get("case_info", {}).get("diagnosis"),
                "accident_type": "other",
                "requires_disability_review": False,
            },
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
                    insurer,
                    product,
                )
            else:
                n = await c.fetchval(
                    "SELECT count(*) FROM policy_chunks WHERE insurer = $1", insurer
                )
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
    query = f"{dx_name} {icd} {ci.get('question', '')}".strip()
    res = await hybrid.search(
        query,
        namespaces=["terms"],
        top_k=8,
        insurer=ci.get("insurer"),
        product=ci.get("product_name"),
    )
    chunks = res["ranked_chunks"]
    if not chunks:
        return {"retrieved_clauses": [], "errors": _err(state, "rag_empty")}

    ctx = "\n---\n".join(f"{c['source_ref']} {c['text'][:300]}" for c in chunks[:6])
    analysis = await ai_client.chat_json(
        [
            {
                "role": "system",
                "content": "너는 보험 약관 분석가다. 가입 특약과 약관 조항을 대조해 JSON만 출력한다.",
            },
            {
                "role": "user",
                "content": (
                    f"[가입 특약]\n{state.get('subscribed_coverages', [])}\n\n"
                    f"[사고]\n{dx_name}\n\n[약관 조항]\n{ctx}\n\n"
                    "JSON 키: applicable(적용 가능 특약 list), missing(청구 누락 가능 특약 list), "
                    "analysis(면책·감액 등 분석 str)."
                ),
            },
        ]
    )
    analysis = analysis if isinstance(analysis, dict) else {}
    return {
        "retrieved_clauses": chunks,
        "applicable_coverages": _as_str_list(analysis.get("applicable")),
        "missing_coverages": _as_str_list(analysis.get("missing")),
        "coverage_analysis": {
            "analysis": str(analysis.get("analysis", "")),
            "citations": res["citations"][:6],
        },
    }


# ── 판례 검색 (enrich → 사건단위 dedup → LLM 관련성 필터) ──────
_CASE_TOP_N = 4  # 최종 유지 판례 수
_CASE_CANDIDATE_K = 12  # dedup·필터 전 후보 풀


async def _enrich_cases(chunk_ids: list[str]) -> dict[str, dict[str, Any]]:
    """chunk_id → case_chunks 메타(사건명·법원·선고일·출처). 표시·dedup용 역추적."""
    if not chunk_ids:
        return {}
    pool = db.get_pool()
    async with pool.acquire() as c:
        rows = await c.fetch(
            "SELECT chunk_id, source_type, institution, case_no, case_title, decision_date "
            "FROM case_chunks WHERE chunk_id = ANY($1)",
            chunk_ids,
        )
    return {r["chunk_id"]: dict(r) for r in rows}


async def _filter_relevant_cases(dx_name: str, cands: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """LLM으로 진단과 무관한 판례를 걸러낸다(구조화 메타로는 필터 불가 — accident_type 미채움).

    실패/빈 응답 시 입력을 그대로 통과시킨다(degrade — 필터는 품질 개선용, 필수 아님).
    """
    if not cands:
        return cands
    listing = "\n".join(
        f"{i}) {c.get('case_title') or '(제목없음)'} — {(c.get('text') or '')[:120]}"
        for i, c in enumerate(cands)
    )
    res = await ai_client.chat_json(
        [
            {
                "role": "system",
                "content": "너는 보험 손해사정 판례 선별가다. 사고와 쟁점이 관련된 판례만 고른다. JSON만 출력한다.",
            },
            {
                "role": "user",
                "content": (
                    f"[사고/진단]\n{dx_name}\n\n[판례 후보]\n{listing}\n\n"
                    "위 사고의 보험금 쟁점(보장·면책·후유장해·수술/입원)과 실제로 관련된 후보의 "
                    '번호만 고르라. 무관한 사망·타부위·타담보는 제외. JSON: {"relevant":[번호...]}'
                ),
            },
        ]
    )
    idxs = res.get("relevant") if isinstance(res, dict) else None
    if not isinstance(idxs, list):
        return cands  # 판정 실패 → 통과(degrade)
    keep = {int(i) for i in idxs if isinstance(i, (int, float)) or str(i).isdigit()}
    filtered = [c for i, c in enumerate(cands) if i in keep]
    return filtered or cands  # 전부 탈락하면 보수적으로 원본 유지


@safe_node
async def case_search(state: ReportState) -> dict[str, Any]:
    ci = state.get("case_info", {})
    dx = state.get("diagnosis") or {}
    dx_name = dx.get("diagnosis") or ci.get("diagnosis") or ""
    res = await hybrid.search(dx_name, namespaces=["case"], top_k=_CASE_CANDIDATE_K)
    refs = res["ranked_chunks"]
    if not refs:
        return {"legal_references": [], "errors": _err(state, "case_data_missing")}

    # 1) 메타 역추적(사건명·법원·선고일)으로 각 청크를 보강한다.
    meta = await _enrich_cases([r.get("chunk_id") for r in refs if r.get("chunk_id")])
    for r in refs:
        m = meta.get(r.get("chunk_id"), {})
        r["case_title"] = m.get("case_title")
        r["case_no"] = m.get("case_no")
        r["institution"] = m.get("institution")
        r["decision_date"] = str(m["decision_date"]) if m.get("decision_date") else None
        r["source_type"] = m.get("source_type")

    # 2) 사건 단위 dedup — 같은 사건의 여러 청크 중 최상위 순위만(입력이 이미 점수순).
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for r in refs:
        key = r.get("case_no") or r.get("case_title") or r.get("chunk_id")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    # 3) LLM 관련성 필터(무관 판례 제거) 후 상위 N만.
    relevant = await _filter_relevant_cases(dx_name, deduped)
    kept = relevant[:_CASE_TOP_N]
    errors = state.get("errors", [])
    if len(deduped) > len(relevant):
        errors = _err(state, f"case_filtered:{len(deduped)}→{len(relevant)}")
    return {"legal_references": kept, "errors": errors}


# ── 분기: 후유장해 검토 필요 시 장해 서브그래프로 ──────────────
def route_after_case(state: ReportState) -> str:
    """진단이 requires_disability_review면 장해 노드로, 아니면 보험금 계산 직행."""
    if (state.get("diagnosis") or {}).get("requires_disability_review"):
        return "disability"
    return "payment_calc"


# 폴백 시 캐비앗·신뢰도 캡 상수(약관 미확보 → 표준표 추정임을 명시).
_TERMS_CAVEAT = "가입금액 미보유로 절대 보험금 불가·약관표 위치정렬 한계 — 지급률은 추정"
_STANDARD_CAVEAT = "표준 장해분류표 기준(가입 약관 미확보) — 개별 약관 확인 필요"
_STANDARD_NO_DATE_CAVEAT = "가입일 미상 — 현행판 기준"


def _select_schedule(ranked: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """장해분류표(schedule) 청크 선별 — chunk_type 우선, 없으면 헤더 휴리스틱."""
    sched = [c for c in ranked if c.get("chunk_type") == "schedule"]
    if not sched:
        sched = [c for c in ranked if "장해의 분류" in (c.get("text") or "")]
    return sched


async def _extract_schedule_items(
    dx_name: str, icd: str, sched: list[dict[str, Any]], fallback_citations: list[str]
) -> dict[str, Any]:
    """장해분류표 원문에서 LLM 추출 + 결정론 백스톱.

    terms(가입 약관)·level(표준표) 어느 소스든 동일한 추출·검증 흐름을 태운다.

    Returns:
        {"items": list, "notes": list[str], "confidence": str, "citations": list[str]}.
    """
    sched_text = "\n".join((c.get("text") or "") for c in sched)
    ctx = "\n---\n".join(f"{c['source_ref']}\n{(c.get('text') or '')[:800]}" for c in sched[:6])
    raw = await ai_client.chat_json(
        [
            {
                "role": "system",
                "content": (
                    "너는 보험 약관 장해분류표 분석가다. 제공된 [약관 장해분류표 원문]에서만 근거를 찾아 "
                    "사고를 분류하고 지급률을 추출한다. 표에 없는 지급률은 절대 만들지 마라. JSON만 출력한다."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"[사고/진단]\n{dx_name} / ICD {icd}\n\n[약관 장해분류표 원문]\n{ctx}\n\n"
                    "JSON 키: items(배열, 각 원소 = injury(str), "
                    "body_region(눈·귀·코·씹기말하기·척추·체간골·팔·다리·손가락·발가락·흉복부장기·신경계정신 중 하나), "
                    "category_label(원문 항목 텍스트 그대로 복사), rate(number 지급률 %), "
                    "rate_quote(rate 숫자가 등장한 원문 구절 그대로 복사), temporary(bool 한시장해), "
                    "temporary_years(number 또는 null), citation(위 원문 source_ref 중 하나)), "
                    "uncertain(bool), notes(str).\n"
                    "규칙: rate는 원문에 실제 등장하는 숫자만. category_label·rate_quote는 요약 말고 복사. "
                    "적합 항목 없으면 items=[] uncertain=true. 추측으로 숫자 만들지 마라."
                ),
            },
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
        items.append(
            {
                "injury": injury,
                "body_region": str(it.get("body_region") or "기타"),
                "category_label": str(it.get("category_label") or ""),
                "rate": rate_f,
                "rate_quote": quote,
                "temporary": bool(it.get("temporary")),
                "temporary_years": it.get("temporary_years"),
                "citation": str(it.get("citation") or ""),
                "verified": verified,
            }
        )

    verified_n = sum(1 for i in items if i["verified"])
    confidence = (
        "high"
        if (items and verified_n == len(items) and not uncertain)
        else ("medium" if verified_n else "low")
    )
    citations = (
        list(dict.fromkeys(i["citation"] for i in items if i["citation"])) or fallback_citations[:6]
    )
    return {"items": items, "notes": notes, "confidence": confidence, "citations": citations}


# ── 장해 분류·지급률 추출 (RAG + LLM, 숫자는 약관에서만) ────────
@safe_node
async def disability_rag(state: ReportState) -> dict[str, Any]:
    ci = state.get("case_info", {})
    dx = state.get("diagnosis") or {}
    dx_name = dx.get("diagnosis") or ci.get("diagnosis") or ""
    icd = " ".join(dx.get("icd_codes") or [])
    query = f"{dx_name} {icd} 후유장해 장해분류표 지급률".strip()
    res = await hybrid.search(
        query,
        namespaces=["terms"],
        top_k=8,
        insurer=ci.get("insurer"),
        product=ci.get("product_name"),
    )
    sched = _select_schedule(res.get("ranked_chunks", []))
    existing = state.get("retrieved_clauses", [])

    # 폴백: 가입 약관에 장해분류표가 없으면 금감원 표준 장해분류표(level)로 재검색.
    # 계약일(enrolled_at)이 속하는 버전만 반환된다(None이면 현행판).
    enrolled_at = ci.get("enrolled_at")
    is_fallback = False
    if not sched:
        res = await hybrid.search(query, namespaces=["level"], top_k=8, contract_date=enrolled_at)
        # level 청크는 전부 스케줄 행 그룹이므로 chunk_type 선별 없이 그대로 사용.
        sched = res.get("ranked_chunks", [])
        is_fallback = bool(sched)

    if not sched:
        # terms·level 둘 다 비면 기존 동작 유지(빈 결과 + 마커).
        return {
            "disability_analysis": {
                "items": [],
                "combined_rate": 0.0,
                "rule_notes": [],
                "citations": [],
                "confidence": "low",
                "caveat": "장해분류표 미검색",
            },
            "retrieved_clauses": existing,
            "errors": _err(state, "disability_schedule_missing"),
        }

    extracted = await _extract_schedule_items(dx_name, icd, sched, res.get("citations", []))
    confidence = extracted["confidence"]

    if is_fallback:
        caveat = _STANDARD_CAVEAT
        if enrolled_at is None:
            caveat = f"{caveat} · {_STANDARD_NO_DATE_CAVEAT}"
        # 표준표는 개별 약관과 다를 수 있으므로 신뢰도를 medium으로 캡한다.
        if confidence == "high":
            confidence = "medium"
    else:
        caveat = _TERMS_CAVEAT

    seen = {c.get("source_ref") for c in existing}
    merged = existing + [c for c in sched if c.get("source_ref") not in seen]
    out: dict[str, Any] = {
        "disability_analysis": {
            "items": extracted["items"],
            "rule_notes": extracted["notes"],
            "citations": extracted["citations"],
            "confidence": confidence,
            "caveat": caveat,
        },
        "retrieved_clauses": merged,
    }
    if is_fallback:
        # 운영 추적용 마커(표준표 폴백이 실제로 탔음을 남긴다).
        out["errors"] = _err(state, "disability_fallback_standard_schedule")
    return out


# ── 장해 분류 적정성 검증 (LLM 비판 — 과대분류 방지) ───────────
@safe_node
async def disability_verify(state: ReportState) -> dict[str, Any]:
    """추출된 장해 항목이 진단에 비해 과대분류인지 LLM으로 비판·교정한다.

    숫자 검증(disability_rag)은 "지급률이 원문에 있나"만 보고 "이 진단이 그 등급에 맞나"는
    못 본다(사용자 지적). 이 노드가 그 공백을 메운다: 과대면 같은 표의 낮은 등급 지급률로
    하향하고(rate_original 보존), 신뢰도를 low로 강등, 사유를 남긴다. 실패 시 무변경(degrade).
    """
    da = state.get("disability_analysis", {})
    items = da.get("items", [])
    targets = [i for i in items if i.get("verified")]
    if not targets:
        return {}

    ci = state.get("case_info", {})
    dx = state.get("diagnosis") or {}
    dx_name = dx.get("diagnosis") or ci.get("diagnosis") or ""
    surgery = bool(dx.get("surgery"))
    listing = "\n".join(
        f"{idx}) 손상={i.get('injury')} / 선택등급='{i.get('category_label')}' / 지급률={i.get('rate')}%"
        for idx, i in enumerate(targets)
    )
    res = await ai_client.chat_json(
        [
            {
                "role": "system",
                "content": (
                    "너는 보험 손해사정 장해평가 감수자다. 장해분류표에서 '완전히 잃음/완전강직'은 "
                    "절단·인공관절·고정 수준의 중증에만 쓴다. 단순 파열·염좌·재건술 후 회복 가능한 "
                    "손상을 최상위 등급에 넣는 건 과대분류다. JSON만 출력한다."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"[진단]\n{dx_name} (수술={surgery})\n\n[추출된 장해 항목]\n{listing}\n\n"
                    "각 항목에 대해 선택등급이 손상 정도에 적정한지 판정하라. 과대면 같은 부위의 "
                    "더 낮은 등급 지급률(예: 완전상실 30%→기능장해 10~20%)을 제시하라.\n"
                    'JSON: {"reviews":[{"idx":번호,"appropriate":bool,"suggested_rate":숫자 또는 null,'
                    '"reason":str}]}'
                ),
            },
        ]
    )
    reviews = res.get("reviews") if isinstance(res, dict) else None
    if not isinstance(reviews, list):
        return {}  # 판정 실패 → 무변경(degrade)

    by_idx = {int(r["idx"]): r for r in reviews if isinstance(r, dict) and "idx" in r}
    disputed = False
    for idx, item in enumerate(targets):
        rv = by_idx.get(idx)
        if not rv:
            continue
        appropriate = bool(rv.get("appropriate", True))
        item["review"] = {"appropriate": appropriate, "reason": str(rv.get("reason") or "")}
        if appropriate:
            continue
        disputed = True
        sug = rv.get("suggested_rate")
        try:
            sug_f = float(sug)
        except (TypeError, ValueError):
            sug_f = None
        if sug_f is not None and 0 <= sug_f < float(item.get("rate") or 0):
            item["rate_original"] = item.get("rate")
            item["rate"] = sug_f  # 하향 교정 — disability_calc가 이 값으로 합산
            item["disputed"] = True

    if not disputed:
        return {"disability_analysis": {**da, "items": items}}

    caveat = da.get("caveat", "")
    new_da = {
        **da,
        "items": items,
        "confidence": "low",  # 분류 분쟁 → 신뢰도 강등
        "caveat": f"{caveat} · 분류 과대평가 소지로 하향 교정됨(자동검증, 손해사정사 확인 필요)",
    }
    return {
        "disability_analysis": new_da,
        "errors": _err(state, "disability_classification_disputed"),
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


# ── 보험금 계산 (특약별 항목 합산, 단정 금지) ──────────────────
# 정액/변동 특약 유형을 이름 키워드로 추론(가입금액은 coverage_details.amount).
_PER_DIEM_KW = ("입원일당", "입원비")
_DISABILITY_KW = ("후유장해",)
_SURGERY_KW = ("수술비", "수술특약")
_FRACTURE_KW = ("골절",)
# KCD 골절 코드 접두(신체부위별 S_2). 인대·염좌(S83 등)는 여기 안 들어와 골절진단비 제외됨.
_FRACTURE_ICD = ("S02", "S12", "S22", "S32", "S42", "S52", "S62", "S72", "S82", "S92")


def _is_fracture(dx_name: str, icd: str) -> bool:
    if "골절" in dx_name:
        return True
    codes = icd.replace(" ", "").upper()
    return any(c in codes for c in _FRACTURE_ICD)


@safe_node
async def payment_calc(state: ReportState) -> dict[str, Any]:
    """특약별 지급액을 항목화해 합산한다(정액 특약 + 후유장해).

    가입금액(coverage_details.amount)이 있으면 실제 곱셈으로 산출한다:
      - 입원일당 = 일당 x 입원일수   - 수술비 = 정액(수술 시)
      - 골절진단비 = 정액(골절 진단 시)  - 후유장해 = 담보금액 x 장해율
    범위는 min=정액 특약(조건 명확)만, max=후유장해까지 포함(지급률 불확실)로 잡는다.
    coverage_details 가 없으면(구 시드) 기존 배수 어림으로 폴백한다.
    """
    ci = state.get("case_info", {})
    dx = state.get("diagnosis") or {}
    entities = state.get("entities", {})
    dx_name = dx.get("diagnosis") or ci.get("diagnosis") or ""
    icd = " ".join(dx.get("icd_codes") or []) or str(entities.get("icd") or "")
    days = int(entities.get("admission_days") or 0)
    surgery = bool(dx.get("surgery") or entities.get("surgery"))
    fracture = _is_fracture(dx_name, icd)
    da = state.get("disability_analysis") or {}
    rate = float(da.get("combined_rate") or 0.0)
    # disability_verify가 과대분류를 하향 교정했으면 지급액 근거에 그 사실을 명시한다.
    disputed = any(i.get("disputed") for i in da.get("items", []))

    details = [
        d for d in state.get("coverage_details", []) if isinstance(d, dict) and d.get("name")
    ]
    if not details:  # 가입금액 미보유 → 기존 배수 어림(하위호환)
        offered = max(ci.get("offered_amount") or 0, 0)
        factor_hi = 1.0 + min(rate, 100.0) / 100.0 * 0.8 if rate > 0 else 1.8
        return {
            "estimated_range": {"min": offered, "max": int(offered * factor_hi)},
            "payment_breakdown": [],
            "payment_excluded": [],
        }

    fixed_items: list[dict[str, Any]] = []  # 조건 명확한 정액(입원일당·수술비·골절)
    variable_items: list[dict[str, Any]] = []  # 후유장해(지급률 불확실)
    excluded: list[dict[str, Any]] = []

    for d in details:
        name = str(d["name"])
        amt = float(d.get("amount") or 0)
        if any(k in name for k in _DISABILITY_KW):
            if rate > 0:
                variable_items.append(
                    {
                        "name": name,
                        "payout": int(amt * rate / 100.0),
                        "basis": f"담보 {amt:,.0f}원 × 장해율 {rate}%"
                        + ("(과대분류 하향 교정됨)" if disputed else ""),
                    }
                )
            else:
                excluded.append({"name": name, "reason": "후유장해 미검토/지급률 0"})
        elif any(k in name for k in _PER_DIEM_KW):
            if days > 0:
                fixed_items.append(
                    {
                        "name": name,
                        "payout": int(amt * days),
                        "basis": f"{amt:,.0f}원/일 × 입원 {days}일",
                    }
                )
            else:
                excluded.append({"name": name, "reason": "입원일수 0"})
        elif any(k in name for k in _SURGERY_KW):
            if surgery:
                fixed_items.append(
                    {"name": name, "payout": int(amt), "basis": f"수술 정액 {amt:,.0f}원"}
                )
            else:
                excluded.append({"name": name, "reason": "수술 없음"})
        elif any(k in name for k in _FRACTURE_KW):
            if fracture:
                fixed_items.append(
                    {"name": name, "payout": int(amt), "basis": f"골절진단 정액 {amt:,.0f}원"}
                )
            else:
                excluded.append({"name": name, "reason": "골절 진단 아님(인대 손상)"})
        else:
            excluded.append({"name": name, "reason": "지급 조건 매칭 안 됨"})

    fixed_sum = sum(i["payout"] for i in fixed_items)
    var_sum = sum(i["payout"] for i in variable_items)
    return {
        "estimated_range": {"min": fixed_sum, "max": fixed_sum + var_sum},
        "payment_breakdown": fixed_items + variable_items,
        "payment_excluded": excluded,
    }


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
            {
                "role": "system",
                "content": "너는 보험 손해사정 리포트 작성자다. 사실 주장에는 약관 조항 인용을 포함하고, 금액은 단정하지 말고 범위로 쓴다.",
            },
            {
                "role": "user",
                "content": (
                    f"사고: {dx_name} / 질문: {ci.get('question')}\n"
                    f"적용 특약: {state.get('applicable_coverages')}\n"
                    f"누락 가능: {state.get('missing_coverages')}\n"
                    f"분석: {ca.get('analysis')}\n"
                    f"추정범위: {state.get('estimated_range')}\n"
                    f"특약별 산출: {state.get('payment_breakdown')}\n"
                    f"지급제외: {state.get('payment_excluded')}\n"
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
            {
                "role": "system",
                "content": "보험 리포트의 핵심 쟁점을 JSON 배열로 추출한다. JSON만.",
            },
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
            f"{c.get('case_title') or c.get('case_no') or '판례'}"
            f"{' (' + c['institution'] + ')' if c.get('institution') else ''}"
            f"{' ' + c['decision_date'] if c.get('decision_date') else ''}".strip()
            for c in state.get("legal_references", [])[:5]
        )
        or "관련 판례 없음",
        "5_추정보상범위": str(state.get("estimated_range", {})),
        "5c_보상항목내역": "; ".join(
            f"{i['name']} {i['payout']:,}원 ({i['basis']})"
            for i in state.get("payment_breakdown", [])
        )
        or "산출 항목 없음(가입금액 미보유)",
        "5d_지급제외": "; ".join(
            f"{e['name']}({e['reason']})" for e in state.get("payment_excluded", [])
        )
        or "없음",
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
    case_refs = [
        c.get("source_ref") for c in state.get("legal_references", []) if c.get("source_ref")
    ]
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
        "legal_references": case_refs,  # 판례·분쟁조정 근거(별도 보존)
        "disability": state.get("disability_analysis", {}),  # 장해지급률·근거(P1)
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
                rid,
                json.dumps(draft, ensure_ascii=False),
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
                er.get("min"),
                er.get("max"),
            )
            await c.execute("DELETE FROM report_issues WHERE report_id = $1", rid)
            for it in state.get("issues", []):
                await c.execute(
                    """INSERT INTO report_issues (id, report_id, title, description, ai_status, tags)
                       VALUES ($1,$2,$3,$4,$5,$6)""",
                    uuid.uuid4(),
                    rid,
                    str(it.get("title", ""))[:200],
                    str(it.get("description", "")),
                    (it.get("ai_status") or "INFO"),
                    [str(t) for t in (it.get("tags") or [])],
                )
    return {"errors": state.get("errors", [])}


# ── 차단 경로 저장 (초안 없음, reports.status만 갱신) ───────────
@safe_node
async def persist_blocked(state: ReportState) -> dict[str, Any]:
    """입력 가드레일 차단 시 reports.status만 'BLOCKED'로 갱신한다(초안 없음).

    차단은 내용이 없으므로 report_drafts는 만들지 않고, 사유는 로그로만 남긴다(state.errors의
    'input_blocked:...' 항목). 이 노드가 없으면 status가 영원히 갱신되지 않아 Spring/사용자
    쪽에서 리포트가 처리중으로 보인다.

    TODO(spring-contract): reports.status enum에 'BLOCKED'를 추가해 정렬해야 한다
    (현재 계약: AWAITING_INSPECTION|AWAITING_ADOPTION|...). BLOCKED는 여기서 신설한 값이다.
    """
    reasons = [str(e) for e in state.get("errors", []) if str(e).startswith("input_blocked")]
    # 사유는 초안이 아니라 로그로만 보존한다(초안 = 내용물이므로 차단 시 미생성).
    logger.info(
        "report blocked by input guardrail", report_id=state.get("report_id"), reasons=reasons
    )

    rid = uuid.UUID(state["report_id"])
    pool = db.get_pool()
    async with pool.acquire() as c:
        await c.execute(
            "UPDATE reports SET status = 'BLOCKED', updated_at = now() WHERE id = $1",
            rid,
        )
    return {"errors": state.get("errors", [])}
