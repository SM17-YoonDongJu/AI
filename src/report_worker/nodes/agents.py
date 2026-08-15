"""순차 노드 (async, 실제 의존: db·rag·ai_client·guardrail).

각 노드는 ReportState 부분 dict만 반환(LangGraph가 머지). 노드 내부 실패는 errors에
기록하고 부분결과로 진행한다(전체 실패 회피, 이슈 #11 방침).
"""

from __future__ import annotations

import datetime
import functools
import json
import re
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import guardrail
from core import ai_client, crypto, db
from core.exceptions import PiiCryptoError
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
            # 예외 메시지는 담지 않는다(타입명만) — errors는 report_compose의
            # "7_추가확인필요" 섹션에 그대로 join돼 최종 리포트에 노출된다.
            # 원문 파싱 실패 등의 예외 메시지엔 PII 조각이 섞일 수 있다(§9).
            return {"errors": _err(state, f"{fn.__name__}_failed:{type(e).__name__}")}

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
# 청구(claim)에 속한 **처리 완료된** 문서 전부. OCR이 실패한 문서는 애초에 행이 없어
# 여기 안 나온다(ocr_worker.repository의 fan-in 조회와 같은 전제). 정렬 관례도 그쪽
# `_SELECT_CLAIM_DOCUMENTS_SQL`과 맞춘다 — doc_index는 nullable(단독 문서 호환)이라
# NULLS LAST로 뒤로 밀고, 그 안에서는 저장 순서(created_at)로 안정 정렬한다.
# 스키마 미한정(`FROM ocr_results`)은 이 모듈의 기존 관례(search_path 의존)를 따른다.
_SELECT_CLAIM_DOCUMENTS_SQL = (
    "SELECT doc_type, masked_text, entities FROM ocr_results "
    "WHERE claim_id = $1 ORDER BY doc_index NULLS LAST, created_at"
)
_SELECT_OCR_RESULT_SQL = "SELECT masked_text, entities FROM ocr_results WHERE id = $1"

# 문서 경계 표식 — 병합된 텍스트는 그대로 LLM 프롬프트로 들어간다(diagnosis·report_compose).
# 어느 내용이 어느 문서에서 왔는지 표시해야 진단명을 증권에서 뽑거나 특약을 진단서에서
# 뽑는 식의 혼선이 없다. 구분자는 ocr_worker의 `_TABLE_PAGE_SEPARATOR`(빈 줄 + ---)와
# 같은 결로 맞춘다.
_DOC_HEADER = "--- 문서 {index}: {label} ---"
_DOC_SEPARATOR = "\n\n"
# DocType 값 → 한국어 라벨. 프롬프트가 전부 한국어라 enum 값(policy)보다 라벨(보험증권)이
# 모델에 더 잘 읽힌다. 미지의 값은 그대로 노출한다(계약이 늘어도 죽지 않게).
_DOC_TYPE_LABELS = {
    "diagnosis": "진단서",
    "policy": "보험증권",
    "payout_notice": "지급결과안내문",
    "claim": "청구서",
    "hospitalization_cert": "입퇴원확인서",
    "medical_receipt": "진료비계산서·영수증",
    "other": "기타문서",
}


def _parse_entities(raw: Any) -> dict[str, Any]:
    """`ocr_results.entities`(jsonb) 컬럼 값을 dict로 정규화한다.

    asyncpg는 jsonb 코덱 등록 여부에 따라 dict 또는 JSON 문자열을 준다 — 양쪽을 흡수한다.

    Args:
        raw: 컬럼 원값(dict | str | None).

    Returns:
        엔티티 dict. 비었거나 dict가 아니면 빈 dict.
    """
    if not raw:
        return {}
    parsed = raw if isinstance(raw, dict) else json.loads(raw)
    return parsed if isinstance(parsed, dict) else {}


def _merge_claim_texts(docs: Sequence[Any]) -> str:
    """청구 문서들의 `masked_text`를 문서 경계 표식과 함께 이어붙인다.

    Args:
        docs: `_SELECT_CLAIM_DOCUMENTS_SQL` 결과 행들(doc_index 순).

    Returns:
        `--- 문서 1: 보험증권 ---` 헤더가 붙은 문서 본문을 빈 줄로 이은 텍스트.
    """
    parts: list[str] = []
    for index, doc in enumerate(docs, start=1):
        doc_type = str(doc["doc_type"] or "other")
        header = _DOC_HEADER.format(index=index, label=_DOC_TYPE_LABELS.get(doc_type, doc_type))
        parts.append(f"{header}\n{doc['masked_text'] or ''}")
    return _DOC_SEPARATOR.join(parts)


def _merge_claim_entities(docs: Sequence[Any]) -> dict[str, Any]:
    """청구 문서들의 `entities`를 **평평하게** 하나로 합친다.

    네임스페이스로 나누지 않는 이유: 소비처(`payment_calc`)가 `entities.get("icd")`,
    `entities.get("admission_days")`, `entities.get("surgery")`처럼 평평하게 읽고,
    이 키들은 애초에 doc_type별로 서로 겹치지 않는다(`ocr_worker.extract`: icd는
    진단서, admission_days/surgery는 입퇴원확인서). 즉 doc_type이 이미 키 이름에
    녹아 있어 네임스페이스를 추가하면 소비처를 전부 고쳐야 하는 값만큼의 이득이 없다.
    겹치는 키(`insurer`=증권·지급안내문, `payout_amount`=지급안내문·청구서,
    `table_markdown`=영수증 다건)는 report_worker가 읽지 않으므로 뒤 문서가 덮어써도
    무해하다 — 덮어쓰기 순서는 doc_index(문서 순)다.

    다만 `None`으로는 덮어쓰지 않는다. `extract`는 추출 실패 필드를 키 삭제가 아니라
    `None`으로 남기므로, 진단서 2장 중 뒤 장에서 icd 추출이 실패하면 앞 장의 값이
    조용히 지워질 수 있다.

    Args:
        docs: `_SELECT_CLAIM_DOCUMENTS_SQL` 결과 행들(doc_index 순).

    Returns:
        병합된 엔티티 dict.
    """
    merged: dict[str, Any] = {}
    for doc in docs:
        for key, value in _parse_entities(doc["entities"]).items():
            if value is None and key in merged:
                continue  # 뒤 문서의 추출 실패가 앞 문서의 성공값을 지우지 않게
            merged[key] = value
    return merged


def _decrypt_pii_column(row: Any, table: str, column: str, dek: bytes) -> str | None:
    """행이 있으면 그 행의 PII 컬럼을 복호화해 돌려준다(행 자체가 없으면 None).

    load_context가 읽는 세 행(reports·user_claims·user_insurances)은 모두 없을 수 있어
    `... if row else None`이 매번 반복된다. AAD 규약("{table}:{column}")과 그 반복을
    한곳에 모은다. 암호화 미배포 컬럼(text)은 maybe_decrypt가 그대로 통과시킨다.
    """
    if row is None:
        return None
    return crypto.maybe_decrypt(row[column], table, column, dek)


def _extract_claim_diagnosis(claim: Any, dek: bytes) -> str | None:
    """`user_claims.details`(sealed `ClaimDetails`)에서 진단명을 뽑는다.

    Jackson `@JsonTypeInfo`로 사고유형(`accident_type`)별 7개 구현체(의료비·교통사고·
    후유장해·암진단·화재·배상책임·기타) 중 하나로 나뉘지만, `diagnosis: string[]`는
    전 타입 공통 필드라 타입 분기 없이 그대로 뽑을 수 있다(Notion `ClaimDetails` 구조
    문서 참고). `hospitalizations`나 `medical_indemnity` 전용 필드(치료유형·수술일 등)는
    아직 이 값을 쓰는 downstream이 없어 뽑지 않는다 — 쓰는 곳이 생기면 그때 추가한다.

    백엔드 PR #236(이슈 #235)에서 이 컬럼이 `jsonb`→`bytea`로 바뀌었다 — `ClaimDetails`를
    JSON 문자열로 직렬화한 뒤 통째로 한 번 암호화한다(AAD=`user_claims:details`). 그래서
    `maybe_decrypt`로 먼저 복호화해 JSON 문자열을 얻은 다음 파싱한다.

    Args:
        claim: `user_claims` 행(`details` 컬럼 포함) 또는 행 자체가 없으면 None.
        dek: 32바이트 평문 DEK.

    Returns:
        진단명을 ", "로 합친 문자열. 행이 없거나 `details`가 비었거나 복호화 후 JSON
        형식이 예상과 다르면 None — 호출부가 `reports.treatment` 폴백으로 넘어간다.

    Raises:
        PiiCryptoError: 복호화 실패(태그 불일치 등). 이건 데이터 품질 문제가 아니라
            보안 문제라 여기서 삼키지 않고 호출부(load_context)의 기존 PiiCryptoError
            처리(차단 경로)에 맡긴다 — JSON 구조 파싱 실패만 이 함수에서 흡수한다.
    """
    if claim is None or not claim["details"]:
        return None
    raw = crypto.maybe_decrypt(claim["details"], "user_claims", "details", dek)
    if not raw:
        return None
    try:
        parsed = raw if isinstance(raw, dict) else json.loads(raw)
        names = [str(d) for d in parsed.get("diagnosis") or [] if d]
    except (TypeError, ValueError, AttributeError):
        logger.warning("claim details json parse failed")
        return None
    return ", ".join(names) or None


def _decrypt_enrolled_at(value: Any, dek: bytes) -> datetime.date | None:
    """`user_insurances.enrolled_at`을 복호화해 date로 되돌린다.

    이 컬럼만 원래 타입이 date라 별도 처리가 필요하다. 암호화 미배포 상태에선 asyncpg가
    date를 그대로 주므로 통과시키고(maybe_decrypt가 text를 통과시키는 것과 같은 역할),
    배포 후엔 bytea가 와서 복호화 결과가 문자열이 된다. 그런데 downstream
    (`disability_rag` → `hybrid.search(contract_date=...)`)의 계약은 `date | None`이라
    문자열을 그대로 흘리면 SQL 바인딩에서 깨진다.

    Args:
        value: asyncpg가 돌려준 컬럼 값(date=평문, bytes=암호화됨, None=NULL).
        dek: 32바이트 평문 DEK.

    Returns:
        파싱한 date. NULL이거나 ISO-8601로 읽히지 않으면 None — 표준 장해분류표를 현행판으로
        검색하고 리포트에 '계약일 불명' 캐비앗이 붙는다(disability_rag). 계약일 하나 때문에
        리포트 전체를 죽이지 않는다.
    """
    if value is None or isinstance(value, datetime.date):
        return value
    plain = crypto.maybe_decrypt(value, "user_insurances", "enrolled_at", dek)
    if plain is None:
        return None
    try:
        return datetime.date.fromisoformat(plain)
    except ValueError:
        # 값 자체는 로그에 남기지 않는다(암호화 대상 컬럼 = PII 취급).
        logger.warning("enrolled_at parse failed", column="user_insurances.enrolled_at")
        return None


def _extract_coverages(ins: Any, dek: bytes) -> list[str]:
    """`user_insurances.coverages`에서 특약명 목록을 복호화·파싱한다.

    백엔드 PR #222에서 이 컬럼이 `text[]`→`bytea`로 바뀌었다 — 배열 전체를 JSON
    문자열로 직렬화한 뒤 통째로 한 번 암호화한다(AAD=`user_insurances:coverages`).
    원소별 봉투가 아니다 — 예전엔 원소별로 잘못 가정해서 `bytea`를 바이트 단위로
    순회하며 `TypeError`가 나던 버그가 있었다.

    Args:
        ins: `user_insurances` 행(`coverages` 컬럼 포함) 또는 행 자체가 없으면 None.
        dek: 32바이트 평문 DEK.

    Returns:
        특약명 리스트. 행이 없거나 `coverages`가 비었거나 복호화 후 JSON 형식이
        예상과 다르면 빈 리스트 — 이건 데이터 품질 문제라 리포트 전체를 막지 않는다.

    Raises:
        PiiCryptoError: 복호화 실패(태그 불일치 등). 데이터 품질 문제가 아니라 보안
            문제라 여기서 삼키지 않고 호출부(load_context)의 차단 경로에 맡긴다.
    """
    if ins is None or not ins["coverages"]:
        return []
    raw = crypto.maybe_decrypt(ins["coverages"], "user_insurances", "coverages", dek)
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        return [str(c) for c in parsed if c]
    except (TypeError, ValueError):
        logger.warning("coverages json parse failed")
        return []


@safe_node
async def load_context(state: ReportState) -> dict[str, Any]:
    # PII 컬럼(description·question 등)은 백엔드가 AES-256-GCM 봉투(bytea)로 넣어 둔다.
    # DEK는 프로세스당 1회 확보 후 캐시라 노드마다 받아도 비용이 없다 — 한 번 받아 재사용한다.
    #
    # DEK 미확보·복호화 실패(PiiCryptoError)는 safe_node의 범용 캐치에 맡기지 않고 여기서
    # 직접 "input_blocked:..."로 반환한다 — route_after_input이 그 접두사를 보고 기존
    # 차단 경로(persist_blocked → status='BLOCKED')로 보낸다. 그냥 흘려보내면(safe_node가
    # "load_context_failed:..."로 삼킴) case_info가 텅 빈 채 그래프가 끝까지 돌고,
    # guard_input은 빈 masked_text를 막지 않아 빈 리포트가 정상 상태로 저장된다.
    try:
        dek = await crypto.get_pii_dek()
    except PiiCryptoError as e:
        logger.error("pii dek unavailable", report_id=state.get("report_id"), error=str(e))
        return {"errors": _err(state, f"input_blocked:pii_dek_unavailable:{type(e).__name__}")}
    pool = db.get_pool()
    claim_id = state.get("claim_id")
    async with pool.acquire() as c:
        # 청구(claim) 단위 리포트면 대표 문서 하나가 아니라 청구의 **전 문서**를 본다.
        # ReportJob.ocr_result_id/doc_type은 계약상 단일값이라 ocr_worker가 대표 문서
        # (증권 우선) 하나만 실어 보내는데, 그것만 읽으면 진단서·영수증 내용이 리포트에
        # 아예 반영되지 않는다(ocr_worker `_publish_claim_report` 주석의 "정공법"이 이것).
        docs: list[Any] = []
        if claim_id:
            docs = list(await c.fetch(_SELECT_CLAIM_DOCUMENTS_SQL, claim_id))
        # claim에 안 묶인 리포트(기존 경로) + 청구 조회가 예외적으로 0건인 경우의 안전망.
        ocr = None
        if not docs:
            if claim_id:
                # fan-in은 문서 행이 전부 저장된 뒤에야 발행하므로 여기 걸리면 데이터가
                # 어긋난 것이다(claim_id 미기록 등) — 리포트는 대표 문서로 계속 만들되
                # 원인 추적용 신호를 남긴다.
                logger.warning(
                    "claim documents not found, falling back to representative document",
                    report_id=state.get("report_id"),
                    claim_id=claim_id,
                )
            ocr = await c.fetchrow(_SELECT_OCR_RESULT_SQL, uuid.UUID(state["ocr_result_id"]))
        rep = await c.fetchrow(
            "SELECT accident_type, treatment, offered_amount, question, claim_id "
            "FROM reports WHERE id = $1",
            uuid.UUID(state["report_id"]),
        )
        claim = None
        if rep and rep["claim_id"]:
            claim = await c.fetchrow(
                "SELECT accident_date, accident_type, offered_amount, description, "
                "additional_information, details FROM user_claims WHERE id = $1",
                rep["claim_id"],
            )
        ins = await c.fetchrow(
            "SELECT insurer_name, product_name, coverages, enrolled_at "
            "FROM user_insurances "
            "WHERE user_id = (SELECT user_id FROM reports WHERE id = $1) LIMIT 1",
            uuid.UUID(state["report_id"]),
        )

    errors = list(state.get("errors", []))
    if docs:
        masked_text = _merge_claim_texts(docs)
        entities = _merge_claim_entities(docs)
        logger.info(
            "claim documents loaded",
            report_id=state.get("report_id"),
            claim_id=claim_id,
            doc_count=len(docs),
            # 어떤 유형이 실제로 리포트에 반영됐는지 남긴다(전부 DocType enum 값 = 비-PII).
            doc_types=[str(d["doc_type"]) for d in docs],
        )
    elif ocr:
        # 단건 경로는 기존 동작을 그대로 둔다 — 문서 헤더도 붙이지 않는다(프롬프트 무변경).
        masked_text = ocr["masked_text"] or ""
        entities = _parse_entities(ocr["entities"])
    else:
        # 청구 문서 0건 + 대표 문서 행도 없음 = 컨텍스트 없음(worker가 하드 실패로 승격).
        masked_text = ""
        entities = {}
        errors.append("ocr_result_missing")

    try:
        case_info = {
            "accident_type": (rep["accident_type"] if rep else None)
            or (claim["accident_type"] if claim else None),
            # 진단명은 user_claims.details의 diagnosis[]를 우선 쓰고, 없거나
            # 파싱이 안 되면 reports.treatment로 폴백한다(_extract_claim_diagnosis 참고).
            "diagnosis": _extract_claim_diagnosis(claim, dek)
            or (rep["treatment"] if rep else None),
            "offered_amount": (rep["offered_amount"] if rep else None),
            "question": _decrypt_pii_column(rep, "reports", "question", dek),
            "description": _decrypt_pii_column(claim, "user_claims", "description", dek),
            "additional_information": _decrypt_pii_column(
                claim, "user_claims", "additional_information", dek
            ),
            "insurer": _decrypt_pii_column(ins, "user_insurances", "insurer_name", dek),
            "product_name": _decrypt_pii_column(ins, "user_insurances", "product_name", dek),
            # 가입일(date|None) — 표준표 버전 매칭용
            "enrolled_at": _decrypt_enrolled_at(ins["enrolled_at"], dek) if ins else None,
        }
        subscribed_coverages = _extract_coverages(ins, dek)
    except (PiiCryptoError, TypeError) as e:
        # TypeError도 여기서 같이 잡는다 — 예상 타입(bytes/str/None)이 아닌 값이 오면
        # 스키마가 또 어긋났다는 신호다(coverages를 text[]로 잘못 가정했던 게 실제
        # 사례). safe_node의 범용 캐치에 맡기면 "load_context_failed:..."로만 남아
        # 차단 경로를 안 타고, 빈 컨텍스트가 정상 리포트로 저장되는 문제가 재발한다.
        logger.error("pii decrypt failed", report_id=state.get("report_id"), error=str(e))
        errors.append(f"input_blocked:pii_decrypt_failed:{type(e).__name__}")
        return {"errors": errors}
    # user_insurances에는 특약별 가입금액 컬럼이 없다(실제 스키마는 coverages text[] 하나뿐) —
    # coverage_details는 채우지 않고, payment_calc가 배수 어림 폴백으로 지급액을 산출한다.
    return {
        "case_info": case_info,
        "masked_text": masked_text,
        "entities": entities,
        "subscribed_coverages": subscribed_coverages,
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
# 장해검토 대상인데 확정 장해 항목이 없을 때(급성 골절 등) — 0으로 침묵하지 않고 재평가를 안내.
_PENDING_CAVEAT = "후유장해 미확정 — 치료·골유합 경과 후 재평가 시 추가 보상 가능"
_STANDARD_CAVEAT = "표준 장해분류표 기준(가입 약관 미확보) — 개별 약관 확인 필요"
_STANDARD_NO_DATE_CAVEAT = "가입일 미상 — 현행판 기준"


def _select_schedule(ranked: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """장해분류표(schedule) 청크 선별 — chunk_type 우선, 없으면 헤더 휴리스틱."""
    sched = [c for c in ranked if c.get("chunk_type") == "schedule"]
    if not sched:
        sched = [c for c in ranked if "장해의 분류" in (c.get("text") or "")]
    return sched


def _verify_rate_quote(quote: str, rate_f: float, sched_norm: str) -> bool:
    """결정론 백스톱: 인용 구절이 원문에 실재하고, 그 구절 안에 지급률 숫자가 있어야 인정.

    공백 무시 비교로 원문의 개행·정렬 차이를 흡수한다. 지급률은 소수를 보존한다 —
    int로 뭉개면 12.5%가 "12"가 되어 원문 전체의 "제12조"·"12개월" 따위에 오매칭된다.
    """
    quote_norm = re.sub(r"\s+", "", quote)
    if not quote_norm or quote_norm not in sched_norm:
        return False
    return f"{rate_f:g}" in quote_norm


async def _extract_schedule_items(
    dx_name: str, icd: str, sched: list[dict[str, Any]], fallback_citations: list[str]
) -> dict[str, Any]:
    """장해분류표 원문에서 LLM 추출 + 결정론 백스톱.

    terms(가입 약관)·level(표준표) 어느 소스든 동일한 추출·검증 흐름을 태운다.

    Returns:
        {"items": list, "notes": list[str], "confidence": str, "citations": list[str]}.
    """
    sched_text = "\n".join((c.get("text") or "") for c in sched)
    sched_norm = re.sub(r"\s+", "", sched_text)
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
                    "body_region(눈·귀·코·씹기말하기·외모·척추·체간골·팔·다리·손가락·발가락·흉복부장기·신경계정신 중 하나), "
                    'laterality("left"|"right"|"none" — 좌·우가 구분되는 부위(눈·귀·팔·다리·손가락·발가락)만 '
                    '원문·진단에서 확인된 쪽을 적고, 그 외 부위이거나 미상이면 "none"), '
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
        verified = _verify_rate_quote(quote, rate_f, sched_norm)
        injury = str(it.get("injury") or "")
        if not verified:
            notes.append(f"미검증 지급률 제외: {injury} {rate_f}%")
        laterality = str(it.get("laterality") or "none").strip().lower()
        if laterality not in ("left", "right"):
            laterality = "none"
        items.append(
            {
                "injury": injury,
                "body_region": str(it.get("body_region") or "기타"),
                "laterality": laterality,
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

    # 장해표는 찾았는데 확정 항목이 0개 = '아직 미확정'(급성 골절 등). 이 노드는 장해검토가
    # 필요하다고 판정돼야 진입하므로(route_after_case), 조용히 0으로 두면 "경과 후 재평가하면
    # 추가 보상 가능"이라는 정보가 사라진다 — 명시적 캐비앗·마커로 승격한다.
    pending = not extracted["items"]
    if pending:
        caveat = f"{caveat} · {_PENDING_CAVEAT}"

    out: dict[str, Any] = {
        "disability_analysis": {
            "items": extracted["items"],
            "rule_notes": extracted["notes"],
            "citations": extracted["citations"],
            "confidence": confidence,
            "caveat": caveat,
            "pending_reeval": pending,
        },
        "retrieved_clauses": merged,
    }
    markers = []
    if is_fallback:
        # 운영 추적용 마커(표준표 폴백이 실제로 탔음을 남긴다).
        markers.append("disability_fallback_standard_schedule")
    if pending:
        markers.append("disability_pending_reeval")
    if markers:
        out["errors"] = [*state.get("errors", []), *markers]
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
                excluded.append(
                    {
                        "name": name,
                        "reason": "후유장해 재평가 필요(미확정 — 경과 후 추가 보상 가능)"
                        if da.get("pending_reeval")
                        else "후유장해 미검토/지급률 0",
                    }
                )
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
    if da.get("items"):
        disability_line = (
            f"추정 합산 장해지급률 {da.get('combined_rate', 0)}% "
            f"(신뢰도 {da.get('confidence', '-')}, 근거 {', '.join(da.get('citations', []))}) — "
            f"규칙 {'; '.join(da.get('rule_notes', [])) or '-'}. ※ {da.get('caveat', '')}"
        )
    elif da.get("pending_reeval"):
        # 장해검토 대상이나 아직 확정 항목 없음 — '0원'이 아니라 재평가 안내로 전달한다.
        disability_line = f"현재 확정 장해 없음 — 재평가 대상. ※ {da.get('caveat', '')}"
    else:
        disability_line = "해당 없음(후유장해 미검토)"
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
