"""core.logging 검증 — PII 마스킹(방어선)·컨텍스트 전파·구성 스모크."""

import logging

import pytest
import structlog

from core import logging as core_logging
from core.logging import (
    bind_context,
    bound_context,
    clear_context,
    configure_logging,
    get_logger,
    log_event,
    mask_pii,
    new_request_context,
    timed,
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


@pytest.fixture
def _restore_log_levels():  # type: ignore[no-untyped-def]
    names = ["", "anthropic", "httpx2", "httpcore2"]
    saved = {name: logging.getLogger(name).level for name in names}
    yield
    for name, level in saved.items():
        logging.getLogger(name).setLevel(level)


@pytest.mark.usefixtures("_restore_log_levels")
def test_debug_level_does_not_open_sdk_body_loggers(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 장애 대응 중 LOG_LEVEL=DEBUG로 올린 상황
    monkeypatch.setattr(core_logging.settings, "log_level", "DEBUG")

    # Act
    configure_logging()

    # Assert: 앱 로그는 DEBUG지만, 요청 본문을 찍는 SDK 로거는 INFO 미만으로 내려가지 않는다
    assert logging.getLogger().level == logging.DEBUG
    assert logging.getLogger("anthropic").getEffectiveLevel() == logging.INFO
    assert logging.getLogger("httpx2").getEffectiveLevel() == logging.INFO
    assert logging.getLogger("httpcore2").getEffectiveLevel() == logging.INFO


@pytest.mark.usefixtures("_restore_log_levels")
def test_cap_reapplies_after_sdk_reset_its_logger() -> None:
    # Arrange: SDK가 import 때 ANTHROPIC_LOG=debug로 자기 로거를 DEBUG로 되돌린 상황
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger("anthropic").setLevel(logging.DEBUG)

    # Act
    core_logging.cap_sdk_body_loggers()

    # Assert
    assert logging.getLogger("anthropic").getEffectiveLevel() == logging.INFO


# --- 성능 계측(timed·bound_context) --- #


def test_timed_logs_duration_status_and_block_fields() -> None:
    # Arrange
    logger = get_logger("test")

    # Act
    with structlog.testing.capture_logs() as logs, timed(logger, "x.stage", stage="s1") as extra:
        extra["pages"] = 3

    # Assert: 고정 필드 + 블록이 채운 필드 + 시간·상태가 이벤트 1건에 실린다
    [event] = logs
    assert event["event"] == "x.stage"
    assert event["event_type"] == "x.stage"
    assert (event["stage"], event["pages"], event["status"]) == ("s1", 3, "ok")
    assert isinstance(event["duration_ms"], int)
    assert event["duration_ms"] >= 0
    assert "error_type" not in event


def test_timed_failure_reraises_and_logs_type_only() -> None:
    # Arrange: 예외 메시지에 원문 조각이 섞인 상황
    logger = get_logger("test")

    # Act
    with (
        structlog.testing.capture_logs() as logs,
        pytest.raises(ValueError, match="원문"),
        timed(logger, "x.stage", stage="s1"),
    ):
        raise ValueError("원문 홍길동")

    # Assert: 계측은 예외를 삼키지 않고, 타입만 남긴다(§9)
    [event] = logs
    assert (event["status"], event["error_type"]) == ("failed", "ValueError")
    assert "홍길동" not in str(event)


def test_bound_context_applies_inside_block_only() -> None:
    # Arrange
    clear_context()
    bind_context(job_id="j1")
    logger = get_logger("test")

    # Act
    with structlog.testing.capture_logs(
        processors=[structlog.contextvars.merge_contextvars]
    ) as logs:
        with bound_context(graph_node="diagnosis"):
            logger.info("inside")
        logger.info("outside")
    clear_context()

    # Assert: 블록 안 로그에만 붙고, 작업 단위 컨텍스트(job_id)는 그대로 남는다
    inside, outside = logs
    assert (inside["graph_node"], inside["job_id"]) == ("diagnosis", "j1")
    assert "graph_node" not in outside
    assert outside["job_id"] == "j1"
