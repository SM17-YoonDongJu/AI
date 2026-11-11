"""리포트 수치(draft.metrics)·신뢰도 등급 순수 함수 테스트 (I/O 없음).

여기서 고정하는 계약:
  (a) build_report_metrics는 README "리포트 수치" 절의 version 1 스키마를 만든다(골든 픽스처 —
      `_workspace/03_agent_report_metrics.md`의 예시 JSON이 이 출력이다).
  (b) 절마다(input·retrieval·verification·pipeline) 값이 state의 어디서 오고, 키가 없거나 모양이
      어긋나면 무엇이 되는지.
  (c) 조항 인용 실재율: 정규화(공백·의N·앞자리 0)·중복 제거·법령 조문 제외(앞 8글자)·
      근거 출처 2가지(article_number·text).
  (d) grade_confidence: 등급 규칙 표의 행마다 상한과 사유, 사유 누적·순서, OCR 임계값은 설정값.
  (e) judge_evidence: 약관 → 장해분류표 → 판례 순, chunk_id(없으면 source_ref) 기준 중복 제거.
"""

import copy
import json
from typing import Any

import pytest

from core.config import settings
from guardrail import JUDGE_UNAVAILABLE
from report_worker import metrics

# ── 근거 청크 ─────────────────────────────────────────────────
_TERMS: dict[str, Any] = {
    "text": "제3조(보험금의 지급사유) 회사는 제5조에서 정한 경우 입원일당을 지급합니다.",
    "namespace": "terms",
    "source_ref": "[제3조, 행복보험]",
    "chunk_id": "policy-3",
    "article_number": "제3조",
    "chunk_type": "clause",
}
_TERMS_BRANCH: dict[str, Any] = {
    "text": "보험금을 지급하지 않는 사유의 예외를 정한다.",
    "namespace": "terms",
    "source_ref": "[제12조의 2, 행복보험]",
    "chunk_id": "policy-12-2",
    "article_number": "제12조의 2",
    "chunk_type": "clause",
}
_SCHEDULE: dict[str, Any] = {
    "text": "장해의 분류 척추에 약간의 운동장해를 남긴 때 지급률 10%",
    "namespace": "terms",
    "source_ref": "[별표3, 행복보험]",
    "chunk_id": "policy-s3",
    "article_number": "별표3",
    "chunk_type": "schedule",
}
_LEVEL: dict[str, Any] = {
    "text": "척추에 약간의 운동장해를 남긴 때 지급률 10%",
    "namespace": "level",
    "source_ref": "[척추, ]",
    "chunk_id": "schedule-7",
    "article_number": "척추",
    "chunk_type": "schedule",
}
_CASE: dict[str, Any] = {
    "text": "요추 염좌 후유장해 지급 분쟁 사례",
    "namespace": "case",
    "source_ref": "[2020가단123, ]",
    "chunk_id": "case-1",
    "article_number": "판결요지",
    "chunk_type": "case",
}
_CASE_2: dict[str, Any] = {
    "text": "추간판 탈출증 장해 등급 판단 기준",
    "namespace": "case",
    "source_ref": "[2019나456, ]",
    "chunk_id": "case-2",
    "article_number": "판시사항",
    "chunk_type": "case",
}

# ── 골든 픽스처: 청구 3문서 · 장해 경로(하향 교정) · Judge 일부 검증 ─────
_TRACE: list[dict[str, Any]] = [
    {"node": "load_context", "ms": 140, "status": "ok"},
    {"node": "input_guardrail", "ms": 2, "status": "ok"},
    {"node": "diagnosis", "ms": 6100, "status": "ok"},
    {"node": "coverage_parse", "ms": 0, "status": "ok"},
    {"node": "coverage_analysis", "ms": 9800, "status": "ok"},
    {"node": "case_search", "ms": 7300, "status": "ok"},
    {"node": "disability_rag", "ms": 11200, "status": "ok"},
    {"node": "disability_verify", "ms": 5400, "status": "ok"},
    {"node": "disability_calc", "ms": 1, "status": "ok"},
    {"node": "payment_calc", "ms": 0, "status": "ok"},
    {"node": "report_compose", "ms": 24800, "status": "ok"},
    {"node": "output_guardrail", "ms": 8300, "status": "ok"},
]
_BODY = (
    "보험금 지급사유는 약관 제3조에 따르며, 예외는 제 12 조 의 2를 본다. "
    "상법 제651조에 따른 고지의무 위반은 확인되지 않았다. "
    "제99조의 감액 규정은 해당하지 않는다. 제3조 제1항을 다시 확인한다."
)


def _golden_state() -> dict[str, Any]:
    return {
        "doc_type": "policy",
        "input_documents": [
            {"doc_type": "policy", "ocr_confidence": 0.95, "doc_type_confidence": 0.8},
            {"doc_type": "diagnosis", "ocr_confidence": 0.93, "doc_type_confidence": 1.0},
            {"doc_type": "medical_receipt", "ocr_confidence": None, "doc_type_confidence": 0.9},
        ],
        "masked_text": (
            "--- 문서 1: 보험증권 ---\n계약자 [이름] [주민등록번호]\n\n"
            "--- 문서 2: 진단서 ---\n환자 [이름] 연락처 [전화번호]"
        ),
        "case_info": {
            "question": "제 이름은 [이름]입니다. 입원일당을 받을 수 있나요?",
            "description": "연락처 [전화번호]",
            "additional_information": None,  # NULL 또는 마스킹 실패로 비운 필드
        },
        "retrieved_clauses": [_TERMS, _TERMS_BRANCH, _SCHEDULE],
        "legal_references": [_CASE, _CASE_2],
        "case_funnel": {"candidates": 12, "deduped": 9, "relevant": 2, "kept": 2},
        "diagnosis": {"diagnosis": "요추 염좌", "requires_disability_review": True},
        "disability_analysis": {
            "items": [
                {"injury": "요추 운동장해", "rate": 10.0, "verified": True},
                {
                    "injury": "추간판 탈출",
                    "rate": 20.0,
                    "rate_original": 30.0,
                    "verified": True,
                    "disputed": True,
                },
            ],
            "combined_rate": 20.0,
            "confidence": "low",
            "pending_reeval": False,
        },
        "estimated_range": {"min": 1_000_000, "max": 1_160_000},
        "payment_breakdown": [],  # 운영 load_context는 가입금액(coverage_details)이 없다
        "sections": {"4_약관근거": "[제3조, 행복보험]", "6_본문": _BODY},
        "judge_failures": ["보험금은 전액 지급된다"],
        "judge_stats": {"checked_chars": 12_000, "total_chars": 12_480, "evidence_chunks": 5},
        "node_trace": copy.deepcopy(_TRACE),
        "errors": ["case_filtered:9→2", "disability_classification_disputed"],
    }


_GOLDEN_METRICS: dict[str, Any] = {
    "version": 1,
    "input": {
        "document_count": 3,
        "doc_types": ["policy", "diagnosis", "medical_receipt"],
        "ocr_confidence": {"min": 0.93, "avg": 0.94},
        "doc_type_confidence": {"min": 0.8, "avg": 0.9},
        "pii_masked": {
            "total": 6,
            "by_label": {"주민등록번호": 1, "전화번호": 2, "이름": 3},
            "documents": 4,
            "free_text": 2,
        },
    },
    "retrieval": {
        "policy_matched": True,
        "evidence": {"terms": 2, "schedule": 1, "case": 2},
        "case_funnel": {"candidates": 12, "deduped": 9, "relevant": 2, "kept": 2},
        "disability_schedule_source": "terms",
    },
    "verification": {
        "disability": {
            "extracted": 2,
            "verified": 2,
            "combined_rate": 20.0,
            "confidence": "low",
            "pending_reeval": False,
            "reclassified": [{"injury": "추간판 탈출", "from": 30.0, "to": 20.0}],
        },
        "clause_citations": {
            "cited": 3,
            "grounded": 2,
            "ungrounded": ["제99조"],
            "statute_mentions": 1,
        },
        "judge": {
            "ran": True,
            "unavailable": False,
            "flagged": 1,
            "checked_chars": 12_000,
            "total_chars": 12_480,
            "coverage": 0.96,
            "evidence_chunks": 5,
        },
    },
    "pipeline": {
        "nodes": _TRACE,
        "node_count": 12,
        "failed_nodes": [],
        "total_ms": 73_043,
    },
    "confidence": {
        "level": "MEDIUM",
        "reasons": [
            "가입금액 미보유 — 제시액 기준 어림 범위",
            "근거 미확인 주장 1건",
            "인용 검증 범위 일부(96%)",
            "근거에 없는 조항 인용 1건",
            "후유장해 지급률 추정 불확실",
        ],
    },
}


# ── HIGH 기준 상태: 등급 규칙에 하나도 걸리지 않는다 ─────────────
def _high_state() -> dict[str, Any]:
    return {
        "input_documents": [
            {"doc_type": "diagnosis", "ocr_confidence": 0.97, "doc_type_confidence": 0.9}
        ],
        "retrieved_clauses": [_TERMS],
        "legal_references": [_CASE],
        "estimated_range": {"min": 300_000, "max": 900_000},
        "payment_breakdown": [
            {"name": "상해입원일당", "payout": 300_000, "basis": "30,000원/일 x 입원 10일"}
        ],
        "diagnosis": {"diagnosis": "요추 염좌", "requires_disability_review": False},
        "sections": {"6_본문": "약관 제3조에 따라 입원일당 지급 대상이다."},
        "judge_failures": [],
        "judge_stats": {"checked_chars": 900, "total_chars": 900, "evidence_chunks": 2},
        "node_trace": [{"node": "load_context", "ms": 10, "status": "ok"}],
        "errors": [],
    }


def _grade(state: dict[str, Any]) -> tuple[str, list[str]]:
    return metrics.grade_confidence(state, metrics.build_report_metrics(state))


@pytest.fixture(autouse=True)
def ocr_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    """OCR 저신뢰도 임계값을 기본값(0.90)으로 고정한다(셸의 OCR_LOW_CONFIDENCE_THRESHOLD 무관)."""
    monkeypatch.setattr(settings, "ocr_low_confidence_threshold", 0.90)


# ── (a) 스키마(골든) ──────────────────────────────────────────
def test_build_report_metrics_matches_version_1_schema() -> None:
    # Arrange
    state = _golden_state()

    # Act
    result = metrics.build_report_metrics(state)

    # Assert
    assert result == _GOLDEN_METRICS


def test_build_report_metrics_survives_empty_state() -> None:
    # Arrange: 앞 노드가 전부 실패해 키가 하나도 없다

    # Act
    result = metrics.build_report_metrics({})

    # Assert: 예외 없이 0·빈 값·null로 채우고, 범위가 없으니 LOW다
    assert result["input"]["document_count"] == 0
    assert result["retrieval"]["case_funnel"] is None
    assert result["verification"]["disability"] is None
    assert result["pipeline"] == {"nodes": [], "node_count": 0, "failed_nodes": [], "total_ms": 0}
    assert result["confidence"]["level"] == metrics.CONFIDENCE_LOW


# ── (b) input ─────────────────────────────────────────────────
def test_input_without_documents_has_null_confidence() -> None:
    # Arrange
    state: dict[str, Any] = {"input_documents": [], "masked_text": ""}

    # Act
    result = metrics.build_report_metrics(state)["input"]

    # Assert
    assert result == {
        "document_count": 0,
        "doc_types": [],
        "ocr_confidence": None,
        "doc_type_confidence": None,
        "pii_masked": {"total": 0, "by_label": {}, "documents": 0, "free_text": 0},
    }


def test_input_confidence_ignores_values_that_are_not_finite_numbers() -> None:
    # Arrange: None·NaN·bool·문자열은 신뢰도 값이 아니다
    state = {
        "input_documents": [
            {"doc_type": "diagnosis", "ocr_confidence": 0.91234, "doc_type_confidence": None},
            {"doc_type": "claim", "ocr_confidence": float("nan"), "doc_type_confidence": True},
            {"doc_type": "other", "ocr_confidence": "0.5", "doc_type_confidence": None},
        ]
    }

    # Act
    result = metrics.build_report_metrics(state)["input"]

    # Assert
    assert result["ocr_confidence"] == {"min": 0.912, "avg": 0.912}
    assert result["doc_type_confidence"] is None


def test_input_pii_counts_split_documents_and_free_text() -> None:
    # Arrange: 마스킹 실패로 비운 자유입력(None)은 셀 텍스트가 없다. 세 필드를 모두 센다
    state = {
        "masked_text": "환자 [이름] 계좌 [계좌번호] 보호자 [이름]",
        "case_info": {
            "question": "[이메일]로 연락 주세요",
            "description": None,
            "additional_information": "보호자 연락처 [전화번호]",
        },
    }

    # Act
    result = metrics.build_report_metrics(state)["input"]["pii_masked"]

    # Assert
    assert result == {
        "total": 5,
        "by_label": {"이름": 2, "계좌번호": 1, "이메일": 1, "전화번호": 1},
        "documents": 3,
        "free_text": 2,
    }


# ── (b) retrieval ─────────────────────────────────────────────
def test_retrieval_policy_not_matched_when_runtime_parse_marker_present() -> None:
    # Arrange
    state = {"errors": ["policy_not_in_db:runtime_parse_stub"]}

    # Act
    result = metrics.build_report_metrics(state)["retrieval"]

    # Assert
    assert result["policy_matched"] is False


def test_retrieval_evidence_counts_kinds_after_dedup() -> None:
    # Arrange: 같은 약관 청크가 두 번, 표준표(level)와 약관 안의 장해분류표(schedule), 판례
    state = {
        "retrieved_clauses": [_TERMS, dict(_TERMS), _SCHEDULE, _LEVEL],
        "legal_references": [_CASE],
    }

    # Act
    result = metrics.build_report_metrics(state)["retrieval"]

    # Assert
    assert result["evidence"] == {"terms": 1, "schedule": 2, "case": 1}


def test_retrieval_case_funnel_is_null_when_case_search_did_not_finish() -> None:
    # Arrange: case_search가 예외로 끝나면 깔때기가 없다

    # Act
    result = metrics.build_report_metrics({"errors": ["case_search_failed:RuntimeError"]})

    # Assert
    assert result["retrieval"]["case_funnel"] is None


@pytest.mark.parametrize(
    ("errors", "analysis", "expected"),
    [
        pytest.param([], {"items": [], "confidence": "high"}, "terms", id="terms-schedule"),
        pytest.param(
            ["disability_fallback_standard_schedule"],
            {"items": [], "confidence": "medium"},
            "standard",
            id="standard-fallback",
        ),
        pytest.param(
            ["disability_schedule_missing"],
            {"items": [], "confidence": "low"},
            None,
            id="schedule-missing",
        ),
        pytest.param([], None, None, id="not-disability-path"),
    ],
)
def test_retrieval_disability_schedule_source(
    errors: list[str], analysis: dict[str, Any] | None, expected: str | None
) -> None:
    # Arrange
    state = {"errors": errors, "disability_analysis": analysis}

    # Act
    result = metrics.build_report_metrics(state)["retrieval"]

    # Assert
    assert result["disability_schedule_source"] == expected


# ── (b) verification.disability / judge ───────────────────────
def test_verification_disability_is_null_off_the_disability_path() -> None:
    # Act
    result = metrics.build_report_metrics({"diagnosis": {"requires_disability_review": False}})

    # Assert
    assert result["verification"]["disability"] is None


def test_verification_disability_reports_pending_reevaluation() -> None:
    # Arrange: 장해분류표는 찾았지만 확정 항목이 없다(급성 골절 등)
    state = {"disability_analysis": {"items": [], "confidence": "low", "pending_reeval": True}}

    # Act
    result = metrics.build_report_metrics(state)["verification"]["disability"]

    # Assert
    assert result == {
        "extracted": 0,
        "verified": 0,
        "combined_rate": 0.0,
        "confidence": "low",
        "pending_reeval": True,
        "reclassified": [],
    }


def test_verification_disability_counts_only_verified_items() -> None:
    # Arrange: _extract_schedule_items는 원문에서 지급률을 못 찾은 항목도 verified=False로 남긴다
    state = {
        "disability_analysis": {
            "items": [
                {"injury": "요추 운동장해", "rate": 10.0, "verified": True},
                {"injury": "미검증 항목", "rate": 30.0, "verified": False},
                {"injury": "표식 없음", "rate": 5.0},
            ],
            "combined_rate": 10.0,
            "confidence": "medium",
        }
    }

    # Act
    result = metrics.build_report_metrics(state)["verification"]["disability"]

    # Assert
    assert (result["extracted"], result["verified"]) == (3, 1)


def test_verification_judge_unavailable_is_not_counted_as_flagged() -> None:
    # Arrange: 판정 불가 — guard_output이 범위 0으로 돌려준다
    state = {
        "judge_failures": [JUDGE_UNAVAILABLE],
        "judge_stats": {"checked_chars": 0, "total_chars": 5000, "evidence_chunks": 0},
    }

    # Act
    result = metrics.build_report_metrics(state)["verification"]["judge"]

    # Assert
    assert result == {
        "ran": False,
        "unavailable": True,
        "flagged": 0,
        "checked_chars": 0,
        "total_chars": 5000,
        "coverage": 0.0,
        "evidence_chunks": 0,
    }


def test_verification_judge_without_stats_has_zero_coverage() -> None:
    # Arrange: output_guardrail이 예외로 끝나 judge_stats가 없다

    # Act
    result = metrics.build_report_metrics({})["verification"]["judge"]

    # Assert
    assert result["ran"] is False
    assert result["coverage"] == 0.0


# ── (b) pipeline ──────────────────────────────────────────────
def test_pipeline_lists_failed_nodes_and_sums_ms() -> None:
    # Arrange
    state = {
        "node_trace": [
            {"node": "load_context", "ms": 100, "status": "ok"},
            {"node": "case_search", "ms": 250, "status": "failed"},
            {"node": "payment_calc", "ms": 1, "status": "ok"},
        ]
    }

    # Act
    result = metrics.build_report_metrics(state)["pipeline"]

    # Assert
    assert result["node_count"] == 3
    assert result["failed_nodes"] == ["case_search"]
    assert result["total_ms"] == 351


# ── (b) 직렬화 안전 — 비유한 숫자 하나면 jsonb가 초안 전체를 거부한다 ──────
def test_metrics_normalize_trace_funnel_and_scores() -> None:
    # Arrange: 내부 생산자라 실제로는 오지 않는 값들
    state = {
        "node_trace": [
            {"node": "load_context", "ms": float("inf"), "status": "ok", "extra": object()},
            {"node": "case_search", "ms": -5, "status": "failed"},
            {"node": "payment_calc", "ms": 12.6, "status": "ok"},
        ],
        "case_funnel": {"candidates": float("nan"), "deduped": 2, "relevant": True, "kept": "4"},
        "input_documents": [
            {"doc_type": "policy", "ocr_confidence": 1.7, "doc_type_confidence": 1e308},
            {"doc_type": "claim", "ocr_confidence": -0.2, "doc_type_confidence": 1e308},
        ],
    }

    # Act
    result = metrics.build_report_metrics(state)

    # Assert: ms·깔때기는 음수 아닌 정수, 신뢰도는 [0, 1]로 자른다(1e308 평균도 넘치지 않는다)
    assert result["pipeline"]["nodes"] == [
        {"node": "load_context", "ms": 0, "status": "ok"},
        {"node": "case_search", "ms": 0, "status": "failed"},
        {"node": "payment_calc", "ms": 13, "status": "ok"},
    ]
    assert result["pipeline"]["total_ms"] == 13
    assert result["retrieval"]["case_funnel"] == {
        "candidates": 0,
        "deduped": 2,
        "relevant": 0,
        "kept": 0,
    }
    assert result["input"]["ocr_confidence"] == {"min": 0.0, "avg": 0.5}
    assert result["input"]["doc_type_confidence"] == {"min": 1.0, "avg": 1.0}


def test_metrics_from_hostile_state_are_strict_json() -> None:
    # Arrange: 모든 입력 키에 NaN·무한대·거대한 수·엉뚱한 타입
    nan, inf = float("nan"), float("inf")
    state = {
        "input_documents": [{"doc_type": "policy", "ocr_confidence": nan}, {"ocr_confidence": inf}],
        "case_funnel": {"candidates": inf, "deduped": -inf, "relevant": 10**400, "kept": nan},
        "judge_stats": {"checked_chars": inf, "total_chars": nan, "evidence_chunks": 1e308},
        "judge_failures": [nan],
        "disability_analysis": {
            "items": [{"injury": nan, "rate": inf, "rate_original": nan, "disputed": True}],
            "combined_rate": inf,
        },
        "estimated_range": {"max": nan},
        "node_trace": [{"node": nan, "ms": nan, "status": inf}],
        "sections": {"6_본문": nan},
    }

    # Act
    result = metrics.build_report_metrics(state)

    # Assert: allow_nan=False로도 직렬화된다 — persist의 jsonb가 거부할 값이 없다
    assert json.loads(json.dumps(result, allow_nan=False))["version"] == 1


# ── (c) 조항 인용 실재율 ──────────────────────────────────────
def test_clause_citations_normalize_spacing_branch_and_leading_zero() -> None:
    # Arrange
    body = "제 12 조 의 2와 제12조의2, 제012조 그리고 제12조를 본다."

    # Act
    result = metrics.clause_citation_stats(body, [])

    # Assert: 제12조의2·제12조 두 조항(중복 제거), 근거가 없으니 전부 ungrounded
    assert result == {
        "cited": 2,
        "grounded": 0,
        "ungrounded": ["제12조의2", "제12조"],
        "statute_mentions": 0,
    }


def test_clause_citations_ignore_paragraph_and_item_numbers() -> None:
    # Arrange: 항·호는 조항이 아니다. "제3조의 규정"의 "의"는 가지번호가 아니다
    body = "제3조 제1항 제2호와 제3조의 규정에 따른다."

    # Act
    result = metrics.clause_citation_stats(body, [_TERMS])

    # Assert
    assert result == {"cited": 1, "grounded": 1, "ungrounded": [], "statute_mentions": 0}


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("상법 제651조에 따라 고지의무를 본다.", id="상법"),
        pytest.param("같은 시행령 제5조를 따른다.", id="시행령"),
        pytest.param("같은 시행규칙 제3조를 따른다.", id="시행규칙"),
        pytest.param("민법상 제750조의 책임을 본다.", id="민법상"),
        pytest.param("보험업법 시행령 제5조를 따른다.", id="보험업법-시행령"),
        pytest.param("동법 제7조에 따른다.", id="동법"),
        pytest.param("「약관의 규제에 관한 법률」 제6조에 따라 무효다.", id="낫표-법률"),
        pytest.param("보험업법 제95조의2에 따른 설명의무", id="가지번호"),
        pytest.param("보건복지부령 제5조를 따른다.", id="부령"),
        pytest.param("감염병예방법 제49조에 따른 입원 치료", id="감염병예방법"),
        pytest.param("결핵예방법 제15조의 격리", id="결핵예방법"),
        pytest.param("소방법 제3조 위반 여부", id="소방법"),
    ],
)
def test_clause_citations_count_statutes_separately(body: str) -> None:
    # Act
    result = metrics.clause_citation_stats(body, [])

    # Assert: 법령 이름이 바로 앞에 붙은 조문은 약관 인용이 아니다 — ungrounded로 세지 않는다
    assert result == {"cited": 0, "grounded": 0, "ungrounded": [], "statute_mentions": 1}


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("보험금 수령 제5조에 따라 지급한다.", id="수령-붙음"),
        pytest.param("보험금 수령 시 제5조를 따른다.", id="수령-시"),
        pytest.param("수령인은 제8조에 따른다.", id="수령인은"),
        pytest.param("가입 연령 제3조를 확인한다.", id="연령-붙음"),
        pytest.param("가입 연령 제한은 제4조에 있다.", id="연령-제한은"),
        pytest.param("보험금 청구 방법 제9조를 따른다.", id="방법-붙음"),
        pytest.param("청구 방법은 제9조에 있다.", id="방법은"),
        pytest.param("대법원은 제12조를 근거로 판단했다.", id="대법원은"),
        pytest.param("보험업법상 약관 제3조에 따라 지급한다.", id="보험업법상-약관"),
    ],
)
def test_clause_citations_keep_policy_clauses_near_law_like_words(body: str) -> None:
    # Arrange: 예전 규칙(앞 8글자에 법·령·규칙 한 글자)은 이 약관 인용들을 법령으로 숨겼다

    # Act
    result = metrics.clause_citation_stats(body, [])

    # Assert: 약관 인용으로 센다 — 근거가 없으니 ungrounded에 잡힌다
    assert (result["cited"], len(result["ungrounded"]), result["statute_mentions"]) == (1, 1, 0)


def test_clause_citations_statute_enumeration_inherits_statute() -> None:
    # Arrange: 법령 조문 뒤에 및·쉼표·항 번호·과·ㆍ·내지로 이어진 조문은 같은 법령이다
    body = "상법 제651조 및 제652조, 제653조 제1항과 제654조ㆍ제655조 내지 제657조를 본다."

    # Act
    result = metrics.clause_citation_stats(body, [])

    # Assert
    assert result == {"cited": 0, "grounded": 0, "ungrounded": [], "statute_mentions": 6}


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        pytest.param(
            "상법 제651조 및 약관 제3조를 본다.", (1, 1), id="사이에-다른-말이-있으면-끊긴다"
        ),
        pytest.param("상법 제651조. 제3조에 따르면", (1, 1), id="마침표에서-끊긴다"),
        pytest.param("약관 제3조 및 제4조를 본다.", (2, 0), id="약관-인용은-잇지-않는다"),
    ],
)
def test_clause_citations_enumeration_stops_at_other_words(
    body: str, expected: tuple[int, int]
) -> None:
    # Act
    result = metrics.clause_citation_stats(body, [])

    # Assert: (약관 인용 수, 법령 조문 수)
    assert (result["cited"], result["statute_mentions"]) == expected


def test_clause_citations_grounded_by_article_number_or_evidence_text() -> None:
    # Arrange: 제3조는 근거의 article_number, 제5조는 근거 text 안, 제12조의2는 "제12조의 2"
    body = "제3조, 제5조, 제12조의2, 제40조를 인용한다."

    # Act
    result = metrics.clause_citation_stats(body, [_TERMS, _TERMS_BRANCH])

    # Assert
    assert result == {"cited": 4, "grounded": 3, "ungrounded": ["제40조"], "statute_mentions": 0}


def test_clause_citations_only_count_the_body_section() -> None:
    # Arrange: 4_약관근거는 근거 목록을 옮겨 적은 것이라 세지 않는다
    state = {"sections": {"4_약관근거": "[제77조, 행복보험]", "6_본문": "인용 없음"}}

    # Act
    result = metrics.build_report_metrics(state)["verification"]["clause_citations"]

    # Assert
    assert result == {"cited": 0, "grounded": 0, "ungrounded": [], "statute_mentions": 0}


# ── (d) 신뢰도 등급 ───────────────────────────────────────────
def test_grade_is_high_when_no_rule_applies() -> None:
    # Act
    result = _grade(_high_state())

    # Assert
    assert result == ("HIGH", [])


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        pytest.param(
            {"errors": ["rag_empty"]}, ("LOW", ["약관 근거를 찾지 못함"]), id="LOW-rag-empty"
        ),
        pytest.param(
            {"errors": ["policy_not_in_db:runtime_parse_stub"]},
            ("LOW", ["가입 상품 약관이 DB에 없어 일반 약관으로 분석"]),
            id="LOW-policy-not-in-db",
        ),
        pytest.param(
            {"estimated_range": {"min": 0, "max": 0}},
            ("LOW", ["보상 범위 산출 불가(제시액·가입금액 없음)"]),
            id="LOW-range-max-zero",
        ),
        pytest.param(
            {"estimated_range": {}},
            ("LOW", ["보상 범위 산출 불가(제시액·가입금액 없음)"]),
            id="LOW-range-missing",
        ),
        pytest.param(
            {"payment_breakdown": []},
            ("MEDIUM", ["가입금액 미보유 — 제시액 기준 어림 범위"]),
            id="MEDIUM-no-breakdown",
        ),
        pytest.param(
            {
                "judge_failures": [JUDGE_UNAVAILABLE],
                "judge_stats": {"checked_chars": 0, "total_chars": 900, "evidence_chunks": 0},
            },
            ("MEDIUM", ["인용 검증 미수행"]),
            id="MEDIUM-judge-unavailable",
        ),
        pytest.param(
            {
                "retrieved_clauses": [],
                "legal_references": [],
                "sections": {"6_본문": "입원일당 지급 대상이다."},
                "judge_stats": {"checked_chars": 0, "total_chars": 900, "evidence_chunks": 0},
            },
            ("MEDIUM", ["인용 검증 미수행"]),
            id="MEDIUM-judge-not-run-without-evidence",
        ),
        pytest.param(
            {"judge_failures": ["문장 1", "문장 2"]},
            ("MEDIUM", ["근거 미확인 주장 2건"]),
            id="MEDIUM-judge-flagged",
        ),
        pytest.param(
            {"judge_stats": {"checked_chars": 600, "total_chars": 900, "evidence_chunks": 2}},
            ("MEDIUM", ["인용 검증 범위 일부(66%)"]),
            id="MEDIUM-judge-partial",
        ),
        pytest.param(
            {"sections": {"6_본문": "약관 제3조와 제99조에 따라 지급 대상이다."}},
            ("MEDIUM", ["근거에 없는 조항 인용 1건"]),
            id="MEDIUM-ungrounded-clause",
        ),
        pytest.param(
            {
                "diagnosis": {"requires_disability_review": True},
                "disability_analysis": {"items": [], "confidence": "medium"},
            },
            ("MEDIUM", ["후유장해 지급률 추정 불확실"]),
            id="MEDIUM-disability",
        ),
        pytest.param(
            {
                "input_documents": [
                    {"doc_type": "diagnosis", "ocr_confidence": 0.85, "doc_type_confidence": 0.9}
                ]
            },
            ("MEDIUM", ["문서 인식 신뢰도 낮음"]),
            id="MEDIUM-low-ocr",
        ),
        pytest.param(
            {"errors": ["pii_mask_failed:question"]},
            ("MEDIUM", ["개인정보 보호로 일부 입력 제외"]),
            id="MEDIUM-pii-mask-failed",
        ),
        pytest.param(
            {"errors": ["diagnosis_llm_failed"]},
            ("MEDIUM", ["진단 정보 자동 추출 실패"]),
            id="MEDIUM-diagnosis-llm-failed",
        ),
        pytest.param(
            {"node_trace": [{"node": "case_search", "ms": 5, "status": "failed"}]},
            ("MEDIUM", ["일부 분석 단계 실패"]),
            id="MEDIUM-node-failed",
        ),
    ],
)
def test_grade_rule_rows(overrides: dict[str, Any], expected: tuple[str, list[str]]) -> None:
    # Arrange: HIGH 기준 상태에서 한 가지만 바꾼다
    state = {**_high_state(), **overrides}

    # Act
    result = _grade(state)

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"disability_analysis": None}, id="analysis-missing-node-failed"),
        pytest.param(
            {"errors": ["disability_schedule_missing"]},
            id="schedule-missing",
        ),
        pytest.param(
            {"disability_analysis": {"items": [], "confidence": "high", "pending_reeval": True}},
            id="pending-reevaluation",
        ),
        pytest.param(
            {
                "disability_analysis": {
                    "items": [{"rate": 10.0, "rate_original": 30.0, "disputed": True}],
                    "confidence": "high",
                }
            },
            id="reclassified-down",
        ),
        pytest.param(
            {"disability_analysis": {"items": [], "confidence": "low"}},
            id="confidence-not-high",
        ),
    ],
)
def test_grade_disability_uncertainty_variants(overrides: dict[str, Any]) -> None:
    # Arrange: 장해 경로(requires_disability_review)에서 high 분석을 기준으로 한 가지만 바꾼다
    base = {
        **_high_state(),
        "diagnosis": {"requires_disability_review": True},
        "disability_analysis": {"items": [{"rate": 10.0, "verified": True}], "confidence": "high"},
    }
    state = {**base, **overrides}

    # Act
    result = _grade(state)

    # Assert
    assert result == ("MEDIUM", ["후유장해 지급률 추정 불확실"])


def test_grade_disability_with_high_confidence_stays_high() -> None:
    # Arrange: 가입 약관 장해분류표에서 전부 검증된 항목 — 불확실 사유가 없다
    state = {
        **_high_state(),
        "diagnosis": {"requires_disability_review": True},
        "disability_analysis": {"items": [{"rate": 10.0, "verified": True}], "confidence": "high"},
    }

    # Act
    result = _grade(state)

    # Assert
    assert result == ("HIGH", [])


def test_grade_accumulates_reasons_with_low_reasons_first() -> None:
    # Arrange: LOW 하나 + MEDIUM 둘
    state = {
        **_high_state(),
        "errors": ["pii_mask_failed:description", "rag_empty"],
        "payment_breakdown": [],
    }

    # Act
    result = _grade(state)

    # Assert
    assert result == (
        "LOW",
        [
            "약관 근거를 찾지 못함",
            "가입금액 미보유 — 제시액 기준 어림 범위",
            "개인정보 보호로 일부 입력 제외",
        ],
    )


def test_grade_partial_judge_coverage_is_floored_not_rounded() -> None:
    # Arrange: 11,950/12,000자 — 반올림하면 coverage 1.0이지만 일부만 봤다
    state = {
        **_high_state(),
        "judge_stats": {"checked_chars": 11_950, "total_chars": 12_000, "evidence_chunks": 2},
    }

    # Act
    report_metrics = metrics.build_report_metrics(state)

    # Assert
    assert report_metrics["verification"]["judge"]["coverage"] == 1.0
    assert report_metrics["confidence"] == {
        "level": "MEDIUM",
        "reasons": ["인용 검증 범위 일부(99%)"],
    }


def test_grade_ocr_threshold_compares_the_rounded_value() -> None:
    # Arrange: real(float4)로 저장된 0.90을 읽으면 0.8999999761581421이 된다
    state = {
        **_high_state(),
        "input_documents": [
            {
                "doc_type": "diagnosis",
                "ocr_confidence": 0.8999999761581421,
                "doc_type_confidence": 1,
            }
        ],
    }

    # Act
    result = _grade(state)

    # Assert: 표시값 0.9 = 임계값 → 낮지 않다
    assert result == ("HIGH", [])


def test_grade_ocr_threshold_comes_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: PP-OCRv5 분포에 맞춰 임계값을 올린 배포
    monkeypatch.setattr(settings, "ocr_low_confidence_threshold", 0.98)

    # Act
    result = _grade(_high_state())

    # Assert: 0.97 < 0.98
    assert result == ("MEDIUM", ["문서 인식 신뢰도 낮음"])


# ── (e) Judge 근거 ────────────────────────────────────────────
def test_judge_evidence_orders_terms_then_schedule_then_cases() -> None:
    # Arrange: 병합 순서가 섞여 있다(장해분류표가 먼저 나온 경우)
    state = {
        "retrieved_clauses": [_SCHEDULE, _TERMS, _LEVEL, _TERMS_BRANCH],
        "legal_references": [_CASE],
    }

    # Act
    result = metrics.judge_evidence(state)

    # Assert
    assert result == [_TERMS, _TERMS_BRANCH, _SCHEDULE, _LEVEL, _CASE]


def test_judge_evidence_dedups_by_chunk_id_then_source_ref() -> None:
    # Arrange: chunk_id 중복, chunk_id 없이 source_ref만 같은 청크, 식별자가 아예 없는 청크 2건
    no_chunk_id = {"text": "가", "namespace": "terms", "source_ref": "[제7조, 상품]"}
    anonymous = {"text": "식별자 없음", "namespace": "terms"}
    state = {
        "retrieved_clauses": [
            _TERMS,
            {**_TERMS, "text": "같은 청크(다른 경로로 다시 들어옴)"},
            no_chunk_id,
            dict(no_chunk_id),
            anonymous,
            dict(anonymous),
        ]
    }

    # Act
    result = metrics.judge_evidence(state)

    # Assert: 식별자가 없으면 같은 청크인지 알 수 없어 빼지 않는다
    assert result == [_TERMS, no_chunk_id, anonymous, anonymous]


def test_judge_evidence_keeps_same_chunk_id_from_different_tables() -> None:
    # Arrange: chunk_id는 테이블별 PK라 약관·판례가 같은 값을 가질 수 있다
    case_with_same_id = {**_CASE, "chunk_id": _TERMS["chunk_id"]}
    state = {"retrieved_clauses": [_TERMS], "legal_references": [case_with_same_id]}

    # Act
    result = metrics.judge_evidence(state)

    # Assert
    assert result == [_TERMS, case_with_same_id]


def test_judge_evidence_skips_malformed_entries() -> None:
    # Arrange
    state = {"retrieved_clauses": ["문자열 청크", None, _TERMS], "legal_references": None}

    # Act
    result = metrics.judge_evidence(state)

    # Assert
    assert result == [_TERMS]
