"""구조적 로깅 (structlog).

- **PII 로깅 금지**: 주민번호·계좌·연락처 등은 절대 남기지 않는다(CODE_CONVENTIONS §9·§13).
  마스킹된 값과 식별자(UUID·ref)만 로깅한다.
- 상관관계 식별자(`job_id` 등)를 contextvars로 바인딩해 처리 전 구간 로그에 자동 포함한다.
"""

import logging
import sys

import structlog

from core.config import get_settings


def configure_logging() -> None:
    """structlog + 표준 logging을 1회 구성한다. 앱 진입점에서 호출한다."""
    settings = get_settings()
    level = logging.getLevelNamesMapping().get(settings.log_level.upper(), logging.INFO)

    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """모듈용 구조적 로거를 반환한다."""
    return structlog.get_logger(name)


def bind_context(**identifiers: str) -> None:
    """상관관계 식별자를 현재 컨텍스트에 바인딩한다(예: ``bind_context(job_id=...)``).

    PII는 절대 넣지 않는다 — 식별자(UUID·ref)만 허용한다.
    """
    structlog.contextvars.bind_contextvars(**identifiers)


def clear_context() -> None:
    """컨텍스트 바인딩을 비운다(메시지 처리 종료 시 호출)."""
    structlog.contextvars.clear_contextvars()
