"""리포트 수치(draft.metrics)와 신뢰도 등급 — 순수 계산(LLM·DB·I/O 없음).

`collect_metrics` 노드가 그래프 끝(output_guardrail 뒤, persist 앞)에서 부른다. 결과는
`report_drafts.draft["metrics"]`로 영구 보존되고, 등급(HIGH·MEDIUM·LOW)은
`reports.confidence_level`로 나간다(프론트가 추정 보상 범위 옆 신뢰도 게이지로 보여준다).
스키마와 등급 규칙은 `src/report_worker/README.md`의 "리포트 수치" 절이 단일 출처다. 키를 바꾸거나
빼면 `METRICS_VERSION`을 올리고 README를 같이 고친다.

PII는 담지 않는다. 분류값(doc_type·마스킹 라벨·노드명)·건수·글자 수·정규화한 조항 번호뿐이다.
예외는 `verification.disability.reclassified[].injury` 하나다. 마스킹된 문서에서 LLM이 뽑은
손상명(건강정보, 식별자 아님)이고 `draft["disability"]`에 이미 같은 값이 있다.

Judge 근거 조립(`judge_evidence`)도 여기 둔다. output_guardrail이 Judge에 넘기는 근거와 수치의
근거 건수가 같은 목록에서 나와야 "Judge가 근거 일부만 봤다"(`evidence_chunks` < 근거 수)를
셀 수 있다.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from core.config import settings
from guardrail import JUDGE_UNAVAILABLE, count_masked_pii
from report_worker.state import ReportState

METRICS_VERSION = 1

# reports.confidence_level(varchar(10))에 쓰는 값. backend·프론트가 대문자로 읽는다.
CONFIDENCE_HIGH = "HIGH"
CONFIDENCE_MEDIUM = "MEDIUM"
CONFIDENCE_LOW = "LOW"
CONFIDENCE_LEVELS = frozenset({CONFIDENCE_HIGH, CONFIDENCE_MEDIUM, CONFIDENCE_LOW})

# Judge 근거 종류. 이 순서가 Judge에 넣는 순서다. guard_output은 앞에서부터
# JUDGE_MAX_EVIDENCE_CHUNKS건만 넣으므로 넘치면 판례부터 빠진다.
EVIDENCE_KINDS = ("terms", "schedule", "case")

_SCORE_DIGITS = 3  # 신뢰도 min·avg 소수 자리
_COVERAGE_DIGITS = 2  # Judge 검증 범위 비율 소수 자리

# 조항 인용. "제12조"·"제 12 조 의 2"를 "제12조"·"제12조의2"로 정규화한다.
_CLAUSE_RE = re.compile(r"제\s*(\d+)\s*조(?:\s*의\s*(\d+))?")
# 법령 조문 판정 1 — 법령 이름이 인용 바로 앞에 붙어 있다(사이엔 공백·닫는 괄호 」』)·"상"만).
# 상법 제651조 · 민법상 제750조 · 보험업법 시행령 제5조 · 동법 제7조 · 「…법률」 제6조.
# 글자 하나만 보면 "보험금 수령 제5조"·"가입 연령 제3조"·"대법원은 제12조"의 약관 인용이 법령으로
# 빠진다. 그래서 맨 "령"은 넣지 않고(시행령·대통령령·총리령·부령만), "방법"(청구 방법 제9조)은
# 법령 이름에서 뺀다. 다만 "방법"으로 끝나는 실제 법령(감염병예방법·결핵예방법·소방법)은
# [예소]방법으로 되살린다. 인용 앞 _STATUTE_LOOKBEHIND글자 안에서만 찾는다.
_STATUTE_NAME_RE = re.compile(
    r"(?:(?<!방)법|[예소]방법|법률|시행령|대통령령|총리령|부령|시행규칙|규칙)상?[」』)]?\s*$"
)
_STATUTE_LOOKBEHIND = 20
# 법령 조문 판정 2 — 바로 앞 인용이 법령이고 사이가 접속어·구두점·항/호 번호뿐이면 같은 법령의
# 열거로 본다: "상법 제651조 및 제652조", "제651조 제1항, 제652조", "제651조 내지 제655조".
_STATUTE_ENUMERATION_RE = re.compile(r"(?:\s*(?:제\s*\d+\s*[항호]|및|와|과|또는|내지|[,·ㆍ~]))*\s*")
_BODY_SECTION = "6_본문"  # LLM이 쓴 본문. 다른 섹션은 근거 목록을 옮겨 적은 것이라 세지 않는다
_FUNNEL_KEYS = ("candidates", "deduped", "relevant", "kept")

# load_context가 mask_pii로 가린 자유입력(case_info). OCR masked_text와 따로 센다.
_FREE_TEXT_FIELDS = ("question", "description", "additional_information")

# errors 표식(agents.py가 남긴다).
_RAG_EMPTY = "rag_empty"
_POLICY_NOT_IN_DB = "policy_not_in_db"
_PII_MASK_FAILED = "pii_mask_failed:"
_DIAGNOSIS_LLM_FAILED = "diagnosis_llm_failed"
_SCHEDULE_MISSING = "disability_schedule_missing"
_SCHEDULE_FALLBACK = "disability_fallback_standard_schedule"

# 등급 사유 — 화면에 그대로 나갈 수 있는 문구다(PII·내부 표식 없음).
REASON_RAG_EMPTY = "약관 근거를 찾지 못함"
REASON_POLICY_NOT_IN_DB = "가입 상품 약관이 DB에 없어 일반 약관으로 분석"
REASON_NO_RANGE = "보상 범위 산출 불가(제시액·가입금액 없음)"
REASON_NO_BREAKDOWN = "가입금액 미보유 — 제시액 기준 어림 범위"
REASON_JUDGE_NOT_RUN = "인용 검증 미수행"
REASON_JUDGE_FLAGGED = "근거 미확인 주장 {count}건"
REASON_JUDGE_PARTIAL = "인용 검증 범위 일부({percent}%)"
REASON_UNGROUNDED = "근거에 없는 조항 인용 {count}건"
REASON_DISABILITY = "후유장해 지급률 추정 불확실"
REASON_LOW_OCR = "문서 인식 신뢰도 낮음"
REASON_PII_EXCLUDED = "개인정보 보호로 일부 입력 제외"
REASON_DIAGNOSIS_FAILED = "진단 정보 자동 추출 실패"
REASON_NODE_FAILED = "일부 분석 단계 실패"


# ── Judge 근거 ─────────────────────────────────────────────────
def judge_evidence(state: ReportState) -> list[dict[str, Any]]:
    """Judge 근거 목록 — 약관 → 장해분류표 → 판례 순, 같은 청크는 한 번만.

    약관·장해분류표는 `retrieved_clauses`(coverage_analysis 약관에 disability_rag가 장해분류표를
    덧붙인 병합본)에서, 판례는 `legal_references`에서 온다. 장해분류표는 표준표(namespace `level`)
    또는 약관 안의 장해분류표(chunk_type `schedule`)다.

    Args:
        state: 그래프 상태.

    Returns:
        Judge에 넘길 청크 dict 목록. 근거가 없으면 빈 목록이다.
    """
    grouped = _evidence_by_kind(state)
    return [chunk for kind in EVIDENCE_KINDS for chunk in grouped[kind]]


def _evidence_by_kind(state: ReportState) -> dict[str, list[dict[str, Any]]]:
    """근거를 종류별로 나눠 중복을 뺀다. 먼저 나온 종류·순서가 남는다."""
    clauses = _dicts(state.get("retrieved_clauses"))
    candidates = {
        "terms": [chunk for chunk in clauses if not _is_schedule(chunk)],
        "schedule": [chunk for chunk in clauses if _is_schedule(chunk)],
        "case": _dicts(state.get("legal_references")),
    }
    seen: set[tuple[str, str]] = set()
    grouped: dict[str, list[dict[str, Any]]] = {kind: [] for kind in EVIDENCE_KINDS}
    for kind in EVIDENCE_KINDS:
        for chunk in candidates[kind]:
            key = _evidence_key(chunk)
            if key in seen:
                continue
            if key is not None:
                seen.add(key)
            grouped[kind].append(chunk)
    return grouped


def _is_schedule(chunk: Mapping[str, Any]) -> bool:
    return chunk.get("namespace") == "level" or chunk.get("chunk_type") == "schedule"


def _evidence_key(chunk: Mapping[str, Any]) -> tuple[str, str] | None:
    """중복 판정 키 — chunk_id(없으면 표시용 source_ref). 둘 다 없으면 None(빼지 않는다).

    chunk_id는 테이블마다 따로 매긴 PK(TEXT)라 namespace를 붙여 다른 테이블 청크끼리 겹치지
    않게 한다.
    """
    ident = chunk.get("chunk_id") or chunk.get("source_ref")
    if not ident:
        return None
    return str(chunk.get("namespace") or ""), str(ident)


# ── 수치 ───────────────────────────────────────────────────────
def build_report_metrics(state: ReportState) -> dict[str, Any]:
    """리포트 수치 문서(`version` 1)를 만든다. `confidence`(등급·사유)까지 채운다.

    state의 어떤 키가 없거나 모양이 어긋나도 그 항목을 0·빈 값·null로 두고 계속한다. 앞 노드가
    실패해 키가 빠진 리포트도 수치는 남겨야 하기 때문이다.

    Args:
        state: output_guardrail까지 지난 그래프 상태.

    Returns:
        `input`·`retrieval`·`verification`·`pipeline`·`confidence` 절을 담은 dict. 구조는
        README "리포트 수치" 절과 같다. JSON으로 직렬화할 수 있는 값만 담는다.
    """
    errors = [str(error) for error in state.get("errors") or []]
    evidence = _evidence_by_kind(state)
    metrics: dict[str, Any] = {
        "version": METRICS_VERSION,
        "input": _input_metrics(state),
        "retrieval": _retrieval_metrics(state, errors, evidence),
        "verification": _verification_metrics(state, evidence),
        "pipeline": _pipeline_metrics(state),
    }
    level, reasons = grade_confidence(state, metrics)
    metrics["confidence"] = {"level": level, "reasons": reasons}
    return metrics


def _input_metrics(state: ReportState) -> dict[str, Any]:
    documents = _dicts(state.get("input_documents"))
    case_info = _mapping(state.get("case_info"))
    document_pii = count_masked_pii(_text(state.get("masked_text")))
    free_text_pii = _sum_counts(
        count_masked_pii(_text(case_info.get(field))) for field in _FREE_TEXT_FIELDS
    )
    by_label = _sum_counts([document_pii, free_text_pii])
    return {
        "document_count": len(documents),
        "doc_types": [str(document.get("doc_type") or "other") for document in documents],
        "ocr_confidence": _min_avg(document.get("ocr_confidence") for document in documents),
        "doc_type_confidence": _min_avg(
            document.get("doc_type_confidence") for document in documents
        ),
        "pii_masked": {
            "total": sum(by_label.values()),
            "by_label": by_label,
            "documents": sum(document_pii.values()),
            "free_text": sum(free_text_pii.values()),
        },
    }


def _retrieval_metrics(
    state: ReportState, errors: list[str], evidence: Mapping[str, list[dict[str, Any]]]
) -> dict[str, Any]:
    return {
        "policy_matched": not any(error.startswith(_POLICY_NOT_IN_DB) for error in errors),
        "evidence": {kind: len(evidence[kind]) for kind in EVIDENCE_KINDS},
        "case_funnel": _case_funnel(state.get("case_funnel")),
        "disability_schedule_source": _schedule_source(state, errors),
    }


def _case_funnel(funnel: object) -> dict[str, int] | None:
    """판례 깔때기 네 값(음수 아닌 정수). case_search가 끝나지 않아 없으면 None."""
    if not isinstance(funnel, Mapping):
        return None
    return {key: _count(funnel.get(key)) for key in _FUNNEL_KEYS}


def _schedule_source(state: ReportState, errors: list[str]) -> str | None:
    """장해분류표 출처 — 가입 약관(terms)·표준표 폴백(standard). 없으면 None."""
    if _SCHEDULE_FALLBACK in errors:
        return "standard"
    if not _mapping(state.get("disability_analysis")) or _SCHEDULE_MISSING in errors:
        return None
    return "terms"


def _verification_metrics(
    state: ReportState, evidence: Mapping[str, list[dict[str, Any]]]
) -> dict[str, Any]:
    body = _text(_mapping(state.get("sections")).get(_BODY_SECTION)) or ""
    chunks = [chunk for kind in EVIDENCE_KINDS for chunk in evidence[kind]]
    return {
        "disability": _disability_metrics(state.get("disability_analysis")),
        "clause_citations": clause_citation_stats(body, chunks),
        "judge": _judge_metrics(state),
    }


def _disability_metrics(analysis: object) -> dict[str, Any] | None:
    da = _mapping(analysis)
    if not da:
        return None
    items = _dicts(da.get("items"))
    return {
        "extracted": len(items),
        "verified": sum(1 for item in items if item.get("verified")),
        "combined_rate": finite_number(da.get("combined_rate")) or 0.0,
        "confidence": _text(da.get("confidence")),
        "pending_reeval": bool(da.get("pending_reeval")),
        # disability_verify가 과대분류로 보고 낮춘 항목(rate_original → rate)
        "reclassified": [
            {
                "injury": _text(item.get("injury")) or "",
                "from": finite_number(item.get("rate_original")),
                "to": finite_number(item.get("rate")),
            }
            for item in items
            if item.get("disputed")
        ],
    }


def clause_citation_stats(body: str, evidence: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """본문의 약관 조항 인용이 근거에 실재하는지 센다(정규식, LLM 없음).

    `제N조`·`제N조의M`을 정규화해 중복을 뺀다. 법령 조문은 `statute_mentions`로만 센다 — 법령
    이름이 인용 바로 앞에 붙어 있거나(상법 제651조), 법령 조문에 열거로 이어진 경우다(상법 제651조
    및 제652조). 근거에 있는 조항 = 근거 청크의 `article_number`와 `text`에 나오는 조항 전부다(근거
    쪽은 법령 여부를 가리지 않는다). 조항 번호가 근거에 있는지만 본다 — 인용한 내용이 맞는지는
    Judge 몫이다.

    Args:
        body: 리포트 본문(`sections["6_본문"]`).
        evidence: Judge 근거 청크(`judge_evidence`).

    Returns:
        `cited`(약관 인용 수)·`grounded`(그중 근거에 있는 수)·`ungrounded`(근거에 없는 조항, 본문에
        처음 나온 순서)·`statute_mentions`(법령 조문 수). 수는 모두 정규화한 조항 기준이다.
    """
    cited: list[str] = []
    statutes: set[str] = set()
    statute_end: int | None = None  # 바로 앞 인용이 법령 조문이면 그 끝 위치(열거 판정용)
    for match in _CLAUSE_RE.finditer(body):
        clause = _normalize_clause(match)
        if _is_statute(body, match.start(), statute_end):
            statutes.add(clause)
            statute_end = match.end()
            continue
        statute_end = None
        if clause not in cited:
            cited.append(clause)
    known = _evidence_clauses(evidence)
    ungrounded = [clause for clause in cited if clause not in known]
    return {
        "cited": len(cited),
        "grounded": len(cited) - len(ungrounded),
        "ungrounded": ungrounded,
        "statute_mentions": len(statutes),
    }


def _normalize_clause(match: re.Match[str]) -> str:
    article, branch = match.group(1), match.group(2)
    return f"제{int(article)}조의{int(branch)}" if branch else f"제{int(article)}조"


def _is_statute(text: str, start: int, previous_statute_end: int | None) -> bool:
    """`start`에서 시작하는 조항 인용이 법령 조문인가(이름이 바로 앞 또는 법령 조문의 열거).

    `pos`·`endpos`로 찾으므로 슬라이스와 달리 `(?<!방)`이 창 밖 글자도 본다.
    """
    lookbehind = max(0, start - _STATUTE_LOOKBEHIND)
    if _STATUTE_NAME_RE.search(text, lookbehind, start) is not None:
        return True
    return (
        previous_statute_end is not None
        and _STATUTE_ENUMERATION_RE.fullmatch(text, previous_statute_end, start) is not None
    )


def _evidence_clauses(evidence: Sequence[Mapping[str, Any]]) -> set[str]:
    known: set[str] = set()
    for chunk in evidence:
        for field in ("article_number", "text"):
            text = _text(chunk.get(field)) or ""
            known.update(_normalize_clause(match) for match in _CLAUSE_RE.finditer(text))
    return known


def _judge_metrics(state: ReportState) -> dict[str, Any]:
    stats = _mapping(state.get("judge_stats"))
    failures = [str(failure) for failure in state.get("judge_failures") or []]
    checked = _count(stats.get("checked_chars"))
    total = _count(stats.get("total_chars"))
    return {
        "ran": checked > 0,  # 판정을 받았을 때만 1 이상이다(guard_output)
        "unavailable": JUDGE_UNAVAILABLE in failures,
        "flagged": sum(1 for failure in failures if failure != JUDGE_UNAVAILABLE),
        "checked_chars": checked,
        "total_chars": total,
        "coverage": round(checked / total, _COVERAGE_DIGITS) if total else 0.0,
        "evidence_chunks": _count(stats.get("evidence_chunks")),
    }


def _pipeline_metrics(state: ReportState) -> dict[str, Any]:
    """노드 소요 시간. collect_metrics 자신과 persist는 아직 끝나지 않아 들어가지 않는다."""
    nodes = [
        {
            "node": str(entry.get("node") or ""),
            "ms": _count(entry.get("ms")),
            "status": str(entry.get("status") or ""),
        }
        for entry in _dicts(state.get("node_trace"))
    ]
    return {
        "nodes": nodes,
        "node_count": len(nodes),
        "failed_nodes": [
            str(entry.get("node")) for entry in nodes if entry.get("status") == "failed"
        ],
        "total_ms": sum(_count(entry.get("ms")) for entry in nodes),
    }


# ── 신뢰도 등급 ────────────────────────────────────────────────
def grade_confidence(state: ReportState, metrics: Mapping[str, Any]) -> tuple[str, list[str]]:
    """추정 보상 범위의 신뢰도 등급과 사유를 정한다.

    HIGH에서 시작해 걸리는 조건마다 상한을 낮추고 사유를 모은다(README "리포트 수치"의 등급
    규칙 표). LOW 조건이 하나라도 있으면 LOW, 아니면 MEDIUM 조건이 있으면 MEDIUM, 둘 다 없으면
    HIGH다. 사유는 화면에 그대로 보여도 되는 문구다(PII·내부 표식 없음). LOW 사유가 먼저 온다.

    Args:
        state: 그래프 상태(errors·estimated_range·payment_breakdown·diagnosis·disability_analysis).
        metrics: `build_report_metrics`가 만든 수치(`input`·`verification`·`pipeline` 절).

    Returns:
        (`HIGH`|`MEDIUM`|`LOW`, 사유 목록).
    """
    errors = [str(error) for error in state.get("errors") or []]
    low = _low_reasons(state, errors)
    medium = _medium_reasons(state, metrics, errors)
    if low:
        level = CONFIDENCE_LOW
    elif medium:
        level = CONFIDENCE_MEDIUM
    else:
        level = CONFIDENCE_HIGH
    return level, [*low, *medium]


def _low_reasons(state: ReportState, errors: list[str]) -> list[str]:
    reasons: list[str] = []
    if _RAG_EMPTY in errors:
        reasons.append(REASON_RAG_EMPTY)
    if any(error.startswith(_POLICY_NOT_IN_DB) for error in errors):
        reasons.append(REASON_POLICY_NOT_IN_DB)
    # 범위가 없으면(payment_calc 실패 포함) 산출하지 못한 것으로 본다.
    range_max = finite_number(_mapping(state.get("estimated_range")).get("max"))
    if range_max is None or range_max <= 0:
        reasons.append(REASON_NO_RANGE)
    return reasons


def _medium_reasons(state: ReportState, metrics: Mapping[str, Any], errors: list[str]) -> list[str]:
    judge = metrics["verification"]["judge"]
    ungrounded = metrics["verification"]["clause_citations"]["ungrounded"]
    ocr_confidence = metrics["input"]["ocr_confidence"]
    reasons: list[str] = []
    if not state.get("payment_breakdown"):
        reasons.append(REASON_NO_BREAKDOWN)
    # 판정 불가(JUDGE_UNAVAILABLE)와 근거가 없어 부르지 않은 경우 둘 다 검증하지 않은 것이다.
    if not judge["ran"]:
        reasons.append(REASON_JUDGE_NOT_RUN)
    if judge["flagged"]:
        reasons.append(REASON_JUDGE_FLAGGED.format(count=judge["flagged"]))
    # 반올림한 coverage(0.995 → 1.0)가 아니라 글자 수로 본다. %는 내림이라 일부만 봤으면
    # 100%로 보이지 않는다.
    if judge["ran"] and judge["checked_chars"] < judge["total_chars"]:
        percent = judge["checked_chars"] * 100 // judge["total_chars"]
        reasons.append(REASON_JUDGE_PARTIAL.format(percent=percent))
    if ungrounded:
        reasons.append(REASON_UNGROUNDED.format(count=len(ungrounded)))
    if _disability_uncertain(state, errors):
        reasons.append(REASON_DISABILITY)
    # 표시값(소수 3자리)과 같은 값으로 판정한다. real(float4) 왕복 오차(0.9 → 0.8999999)도
    # 이렇게 흡수된다.
    if ocr_confidence is not None and ocr_confidence["min"] < settings.ocr_low_confidence_threshold:
        reasons.append(REASON_LOW_OCR)
    if any(error.startswith(_PII_MASK_FAILED) for error in errors):
        reasons.append(REASON_PII_EXCLUDED)
    if _DIAGNOSIS_LLM_FAILED in errors:
        reasons.append(REASON_DIAGNOSIS_FAILED)
    if metrics["pipeline"]["failed_nodes"]:
        reasons.append(REASON_NODE_FAILED)
    return reasons


def _disability_uncertain(state: ReportState, errors: list[str]) -> bool:
    """후유장해 경로인데 지급률 추정이 불확실한가.

    장해 경로 = route_after_case와 같은 판정(진단의 requires_disability_review), 또는 장해 분석이
    있음. 불확실 = 분석 없음(노드 실패)·장해분류표 미검색·재평가 대상·confidence가 high 아님·
    하향 교정.
    """
    analysis = _mapping(state.get("disability_analysis"))
    on_path = bool(_mapping(state.get("diagnosis")).get("requires_disability_review"))
    if not (on_path or analysis):
        return False
    if not analysis:
        return True
    return (
        _SCHEDULE_MISSING in errors
        or bool(analysis.get("pending_reeval"))
        or analysis.get("confidence") != "high"
        or any(item.get("disputed") for item in _dicts(analysis.get("items")))
    )


# ── 값 정규화 ──────────────────────────────────────────────────
def finite_number(value: object) -> float | None:
    """유한한 숫자면 float, 아니면 None(bool·문자열·NaN·무한대·float로 못 바꾸는 큰 정수·None).

    DB·LLM에서 온 값을 수치로 옮길 때 쓴다. 모양이 어긋난 값 하나로 계산 전체가 실패하지 않게 하고,
    NaN·무한대가 초안 JSON에 들어가지 않게 한다(PG jsonb가 거부해 persist 전체가 실패한다).

    Args:
        value: 아무 값.

    Returns:
        float 또는 None.
    """
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    try:
        number = float(value)
    except OverflowError:  # 10**400 같은 정수
        return None
    return number if math.isfinite(number) else None


def _min_avg(values: Iterable[object]) -> dict[str, float] | None:
    """신뢰도 min·avg(소수 3자리). 유한한 숫자만 [0, 1]로 잘라 쓰고, 남는 값이 없으면 None."""
    present = [score for score in map(_unit_interval, values) if score is not None]
    if not present:
        return None
    return {
        "min": round(min(present), _SCORE_DIGITS),
        "avg": round(sum(present) / len(present), _SCORE_DIGITS),
    }


def _unit_interval(value: object) -> float | None:
    number = finite_number(value)
    return None if number is None else min(max(number, 0.0), 1.0)


def _sum_counts(counts: Iterable[Mapping[str, int]]) -> dict[str, int]:
    total: dict[str, int] = {}
    for count in counts:
        for label, value in count.items():
            total[label] = total.get(label, 0) + value
    return total


def _count(value: object) -> int:
    """음수가 아닌 정수로 바꾼다(글자 수·청크 수·ms·깔때기 수).

    정수는 그대로(음수는 0), 유한한 소수는 반올림, 그 밖(bool·문자열·NaN·무한대·None)은 0이다.
    """
    if isinstance(value, int) and not isinstance(value, bool):
        return max(value, 0)
    number = finite_number(value)
    return 0 if number is None else max(round(number), 0)


def _text(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _dicts(value: object) -> list[dict[str, Any]]:
    """dict 목록만 남긴다 — 모양이 어긋난 항목 하나가 Judge·수치를 깨지 않게."""
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
