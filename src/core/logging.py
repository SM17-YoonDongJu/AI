"""구조적 로깅 (structlog) — 팀 로깅 표준(OTel 시맨틱 컨벤션 정렬) 구현.

모든 로그는 JSON으로 구조화되고(local 환경은 사람이 읽는 콘솔), `trace_id`·`request_id`가
contextvars로 async 경계(`await`·`asyncio.gather`)를 넘어 전파되며, PII는 렌더링 직전
자동 마스킹된다.

핵심 원칙(.claude/CODE_CONVENTIONS.md §9 · Notion 로깅 전략):
- `print` 금지. 구조적 로거만 사용.
- 원문 PII를 로그에 남기지 않는다. `mask_pii`는 사고 방지용 **방어선**이며, 근본 책임은
  호출자가 PII를 애초에 넘기지 않는 것이다(가드레일의 `[주민번호]` 토큰 치환과는 별개).
- 상관관계 식별자(`trace_id`·`request_id`·`session_id`·`job_id`)를 컨텍스트에 바인딩한다.

범위: 이 모듈은 OTel "필드 네이밍"과 contextvars 전파만 제공한다. 실제 OTel SDK/exporter
연동(trace 수집)은 모니터링 인프라 단계에서 추가한다.
"""

import logging
import re
import socket
import sys
import uuid
from typing import Any

import structlog
from structlog.typing import EventDict, WrappedLogger

from core.config import settings

__all__ = [
    "bind_context",
    "clear_context",
    "configure_logging",
    "get_logger",
    "log_event",
    "mask_pii",
    "new_request_context",
]

# --------------------------------------------------------------------------- #
# PII 마스킹 (방어선) — Notion 로깅 전략의 "로깅 금지 정보" 포맷을 따른다.
# 순수 숫자 패턴만 매칭하므로 UUID(16진수+하이픈, 예: job_id)는 보존된다.
# --------------------------------------------------------------------------- #

# 주민등록번호: 901010-1234567 → 901010-1****** (성별 자리 1자리 보존)
_RRN_RE = re.compile(r"\b(\d{6})[-\s]?([1-4])\d{6}\b")
# 카드번호: 1234-5678-9012-3456 → ****-****-****-3456 (뒤 4자리 보존). 전화보다 먼저 적용.
_CARD_RE = re.compile(r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?(\d{4})\b")
# 휴대전화: 010-1234-5678 → 010-****-5678
_PHONE_RE = re.compile(r"\b(01[016789])[-\s]?\d{3,4}[-\s]?(\d{4})\b")
# 이메일: hong@example.com → hon***@example.com (앞 1~3자 보존)
_EMAIL_RE = re.compile(
    r"\b([A-Za-z0-9._%+-]{1,3})[A-Za-z0-9._%+-]*(@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b"
)


def mask_pii(text: str) -> str:
    """문자열에서 정형 PII(주민번호·카드·전화·이메일)를 부분 마스킹한다.

    로그 사고 방지용 방어선이다. 비정형 PII(이름·주소)는 정규식으로 잡지 못하므로,
    근본적으로는 호출자가 마스킹된 텍스트만 로깅해야 한다.

    Args:
        text: 원본 문자열.

    Returns:
        정형 PII가 부분 가려진 문자열.
    """
    text = _RRN_RE.sub(r"\1-\2******", text)
    text = _CARD_RE.sub(r"****-****-****-\1", text)
    text = _PHONE_RE.sub(r"\1-****-\2", text)
    text = _EMAIL_RE.sub(r"\1***\2", text)
    return text


# --------------------------------------------------------------------------- #
# 프로세서 (structlog 파이프라인 단계)
# --------------------------------------------------------------------------- #

# 리소스 필드. configure_logging()에서 settings 기준으로 갱신된다.
_SERVICE: str = settings.service_name
_ENVIRONMENT: str = settings.environment
_INSTANCE_ID: str = settings.instance_id or socket.gethostname()

# 환경별 기본 로그 레벨(log_level 미지정 시). prod는 비즈니스 이벤트(INFO) 보존을 위해 INFO.
_DEFAULT_LEVEL_BY_ENV = {"local": "DEBUG", "dev": "INFO", "prod": "INFO"}
# stdlib 레벨명 → OTel SeverityText.
_SEVERITY_MAP = {"warning": "WARN", "critical": "FATAL"}


def _severity_processor(
    logger: WrappedLogger, method_name: str, event_dict: EventDict
) -> EventDict:
    """`level`(stdlib)을 OTel `severity` 텍스트로 변환한다."""
    level = str(event_dict.pop("level", method_name)).lower()
    event_dict["severity"] = _SEVERITY_MAP.get(level, level.upper())
    return event_dict


def _resource_processor(
    logger: WrappedLogger, method_name: str, event_dict: EventDict
) -> EventDict:
    """출처 식별 필드(service·environment·instance_id)를 부여한다."""
    event_dict.setdefault("service", _SERVICE)
    event_dict.setdefault("environment", _ENVIRONMENT)
    event_dict.setdefault("instance_id", _INSTANCE_ID)
    return event_dict


def _exception_processor(
    logger: WrappedLogger, method_name: str, event_dict: EventDict
) -> EventDict:
    """`exc_info`를 `exception_type`·`exception_message` 필드로 분해한다.

    메시지는 이후 마스킹 프로세서가 PII를 가린다.
    """
    exc_info = event_dict.pop("exc_info", None)
    if not exc_info:
        return event_dict
    if exc_info is True:
        exc = sys.exc_info()
    elif isinstance(exc_info, BaseException):
        exc = (type(exc_info), exc_info, exc_info.__traceback__)
    elif isinstance(exc_info, tuple):
        exc = exc_info
    else:
        return event_dict
    exc_type, exc_value = exc[0], exc[1]
    if exc_type is not None:
        event_dict["exception_type"] = exc_type.__name__
        event_dict["exception_message"] = str(exc_value)
    return event_dict


def _mask_processor(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    """모든 문자열 값에 PII 마스킹을 적용한다(방어선)."""
    for key, value in event_dict.items():
        if isinstance(value, str):
            event_dict[key] = mask_pii(value)
    return event_dict


def _shared_processors() -> list[Any]:
    """structlog·stdlib 로그가 공유하는 프로세서 체인."""
    return [
        structlog.contextvars.merge_contextvars,  # 바인딩된 trace_id 등 주입
        structlog.stdlib.add_log_level,  # "level" 추가
        _severity_processor,  # level → severity(WARN 등)
        structlog.processors.TimeStamper(fmt="%Y-%m-%dT%H:%M:%S.%fZ", utc=True, key="timestamp"),
        structlog.stdlib.add_logger_name,  # "logger" 추가
        _resource_processor,
        _exception_processor,
        _mask_processor,
        structlog.processors.EventRenamer("message"),  # "event" → "message"
    ]


# --------------------------------------------------------------------------- #
# 공개 API
# --------------------------------------------------------------------------- #


def configure_logging() -> None:
    """앱 시작 시 1회 호출. structlog + stdlib 로깅을 팀 표준 포맷으로 구성한다.

    third-party 로그(asyncpg·boto3·httpx·uvicorn)도 동일 포맷으로 라우팅된다.
    환경(local)에서는 사람이 읽는 콘솔, 그 외에는 JSON으로 렌더링한다.
    """
    global _SERVICE, _ENVIRONMENT, _INSTANCE_ID
    _SERVICE = settings.service_name
    _ENVIRONMENT = settings.environment
    _INSTANCE_ID = settings.instance_id or socket.gethostname()

    level_name = (settings.log_level or _DEFAULT_LEVEL_BY_ENV.get(_ENVIRONMENT, "INFO")).upper()
    level = logging.getLevelNamesMapping().get(level_name, logging.INFO)

    shared = _shared_processors()
    renderer: Any = (
        structlog.dev.ConsoleRenderer()
        if _ENVIRONMENT == "local"
        else structlog.processors.JSONRenderer(ensure_ascii=False)  # 한글 보존
    )

    # stdlib 핸들러: 외부 라이브러리 로그도 같은 포맷으로.
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """구조적 로거를 반환한다.

    Args:
        name: 로거 이름(보통 `__name__`). 로그의 `logger` 필드가 된다.

    Returns:
        바인딩 가능한 구조적 로거.
    """
    return structlog.get_logger(name)


def bind_context(**fields: Any) -> None:
    """현재 실행 컨텍스트(contextvars)에 필드를 바인딩한다.

    이후 같은 async 컨텍스트의 모든 로그에 자동 포함된다. 요청/작업 경계에서 사용한다.
    """
    structlog.contextvars.bind_contextvars(**fields)


def clear_context() -> None:
    """바인딩된 컨텍스트 필드를 모두 제거한다(요청/작업 종료 시)."""
    structlog.contextvars.clear_contextvars()


def new_request_context(
    *, trace_id: str | None = None, request_id: str | None = None, **extra: Any
) -> str:
    """요청/작업 진입 시 추적 컨텍스트를 생성·바인딩한다.

    Args:
        trace_id: 상류에서 전파된 추적 ID(W3C). 없으면 생성한다.
        request_id: 단일 서비스 내 요청 ID. 없으면 생성한다.
        **extra: 함께 바인딩할 추가 필드(session_id·job_id·user_id 등).

    Returns:
        사용된(또는 생성된) `trace_id`.
    """
    trace_id = trace_id or uuid.uuid4().hex  # 32 hex = W3C trace-id 형식
    request_id = request_id or f"req-{uuid.uuid4().hex[:12]}"
    bind_context(trace_id=trace_id, request_id=request_id, **extra)
    return trace_id


def log_event(logger: structlog.stdlib.BoundLogger, event_type: str, **fields: Any) -> None:
    """비즈니스 이벤트를 명시적으로 로깅한다(로깅 원칙 6).

    Args:
        logger: `get_logger`로 얻은 로거.
        event_type: 이벤트 종류(예: "rag.search.completed", "report.generated").
        **fields: 이벤트 속성(analysis_id·policy_id·duration_ms 등).
    """
    logger.info(event_type, event_type=event_type, **fields)
