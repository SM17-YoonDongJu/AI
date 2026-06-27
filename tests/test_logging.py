"""core.logging 검증 — PII 마스킹(방어선)·컨텍스트 전파·구성 스모크."""

import structlog

from core.logging import (
    bind_context,
    clear_context,
    configure_logging,
    get_logger,
    log_event,
    mask_pii,
    new_request_context,
)

# --- PII 마스킹 (가장 위험한 로직 — 정확히 고정) --- #


def test_mask_rrn_keeps_front_and_gender_digit():
    assert mask_pii("환자 901010-1234567 입원") == "환자 901010-1****** 입원"


def test_mask_phone_keeps_prefix_and_last4():
    assert mask_pii("연락처 010-1234-5678") == "연락처 010-****-5678"


def test_mask_email_keeps_first_chars_and_domain():
    assert mask_pii("메일 hong@example.com") == "메일 hon***@example.com"


def test_mask_card_keeps_last4():
    assert mask_pii("카드 1234-5678-9012-3456") == "카드 ****-****-****-3456"


def test_mask_preserves_uuid():
    # job_id 등 UUID(16진수+하이픈)는 마스킹되면 안 된다(추적 불가).
    uid = "550e8400-e29b-41d4-a716-446655440000"
    assert mask_pii(f"job {uid}") == f"job {uid}"


def test_mask_leaves_clean_text_unchanged():
    assert mask_pii("상해후유장해 보험금 청구") == "상해후유장해 보험금 청구"


# --- 컨텍스트 전파 --- #


def test_new_request_context_generates_and_binds_trace_id():
    clear_context()
    trace_id = new_request_context(session_id="sess-1")
    assert len(trace_id) == 32  # W3C trace-id (32 hex)
    bound = structlog.contextvars.get_contextvars()
    assert bound["trace_id"] == trace_id
    assert bound["session_id"] == "sess-1"
    assert bound["request_id"].startswith("req-")
    clear_context()


def test_new_request_context_accepts_upstream_trace_id():
    clear_context()
    trace_id = new_request_context(trace_id="abc123", request_id="req-fixed")
    assert trace_id == "abc123"
    assert structlog.contextvars.get_contextvars()["request_id"] == "req-fixed"
    clear_context()


def test_clear_context_removes_bound_fields():
    bind_context(trace_id="t1")
    clear_context()
    assert structlog.contextvars.get_contextvars() == {}


# --- 구성 스모크 --- #


def test_configure_and_log_does_not_raise():
    configure_logging()
    log = get_logger("test")
    log.info("스모크 메시지", foo="bar")
    log_event(log, "rag.search.completed", duration_ms=12)
