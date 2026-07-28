"""공식 Notion REST 클라이언트(async) + property 파싱 (이슈 #35 P1).

Notion의 데이터베이스를 페이지네이션으로 순회해 원본 페이지 dict를 흘려보내고
(``iter_rows``), 페이지 property를 컬럼 매핑 전의 평탄한 dict로 정규화한다(``parse_props``).

설계 원칙(.claude/CODE_CONVENTIONS.md):
- **async-first**: httpx.AsyncClient만 사용하고 블로킹 sleep 대신 ``asyncio.sleep``으로
  레이트리밋(토큰버킷)·지수 백오프를 구현한다.
- **주입 가능**: HTTP 클라이언트·sleep·clock을 주입해 네트워크 없이 테스트한다.
- **순수 파싱 분리**: ``parse_props``는 네트워크·상태에 의존하지 않는 순수 함수다.
- **PII·만료 URL 미저장**: files property의 ``file.url``은 만료되므로 저장하지 않고
  개수·이름·순서만 추출한다.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime
from typing import Any, Protocol

import httpx

from core.config import Settings, get_settings
from core.exceptions import CorpusSyncError
from core.logging import get_logger

logger = get_logger(__name__)

# Notion REST 엔드포인트 기준(2022-06-28 버전은 /v1/databases/{id}/query 사용).
NOTION_API_BASE = "https://api.notion.com"
# Notion 쿼리 페이지 크기 상한(API 최대치).
QUERY_PAGE_SIZE = 100
# HTTP 요청 타임아웃(초). 카탈로그 동기화는 지연에 관대하다.
NOTION_TIMEOUT_SECONDS = 30.0
# 429/5xx 재시도 횟수와 지수 백오프 파라미터.
DEFAULT_MAX_RETRIES = 5
_BACKOFF_BASE_SECONDS = 1.0
_BACKOFF_MAX_SECONDS = 30.0
# 토큰버킷 최소 레이트(0 division 방지).
_MIN_RATE_PER_SECOND = 0.1
# 재시도 대상 서버 오류 하한.
_SERVER_ERROR_MIN = 500
_TOO_MANY_REQUESTS = 429


class _AsyncHttpClient(Protocol):
    """주입 가능한 최소 HTTP 클라이언트 계약(httpx.AsyncClient가 구조적으로 충족)."""

    async def post(
        self, url: str, *, json: dict[str, Any], headers: dict[str, str]
    ) -> httpx.Response: ...

    async def aclose(self) -> None: ...


class _TokenBucket:
    """async 토큰버킷 레이트리미터(초당 ``rate``개 토큰).

    블로킹 없이 ``asyncio.sleep``으로 대기한다. clock·sleep을 주입해 테스트에서 시간
    진행을 결정적으로 만든다. 락으로 동시 acquire를 직렬화해 전역 레이트를 보장한다.
    """

    def __init__(
        self,
        rate_per_second: float,
        *,
        sleep: Callable[[float], Awaitable[None]],
        clock: Callable[[], float],
        capacity: float | None = None,
    ) -> None:
        self._rate = max(rate_per_second, _MIN_RATE_PER_SECOND)
        self._capacity = capacity if capacity is not None else max(self._rate, 1.0)
        self._tokens = self._capacity
        self._sleep = sleep
        self._clock = clock
        self._last = clock()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """토큰 1개를 소비한다. 부족하면 충전될 만큼만 비동기 대기한다."""
        async with self._lock:
            while True:
                now = self._clock()
                self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                await self._sleep((1.0 - self._tokens) / self._rate)


def _flatten_property(prop: dict[str, Any]) -> Any:
    """Notion property 객체 하나를 타입별로 평탄한 파이썬 값으로 정규화한다.

    title/rich_text→text, select/status→name, date→start(ISO), checkbox→bool,
    number→float, url→str, relation→list[page_id], files→list[{name, order}].
    files의 만료 URL은 저장하지 않는다(개수·이름·순서만).

    Args:
        prop: Notion page property 객체(``{"type": ..., <type>: ...}``).

    Returns:
        정규화된 값(알 수 없는 타입은 ``None``).
    """
    prop_type = prop.get("type")
    if prop_type in ("title", "rich_text"):
        segments = prop.get(prop_type) or []
        text = "".join(segment.get("plain_text", "") for segment in segments)
        return text or None
    if prop_type == "select":
        option = prop.get("select")
        return option.get("name") if option else None
    if prop_type == "status":
        option = prop.get("status")
        return option.get("name") if option else None
    if prop_type == "multi_select":
        return [option.get("name") for option in prop.get("multi_select") or []]
    if prop_type == "date":
        date_value = prop.get("date")
        return date_value.get("start") if date_value else None
    if prop_type == "checkbox":
        return bool(prop.get("checkbox"))
    if prop_type == "number":
        number = prop.get("number")
        return float(number) if number is not None else None
    if prop_type == "url":
        return prop.get("url") or None
    if prop_type == "relation":
        return [relation.get("id") for relation in prop.get("relation") or []]
    if prop_type == "files":
        # file.url은 만료되므로 저장 금지 — 이름·순서만.
        return [
            {"name": attachment.get("name"), "order": index}
            for index, attachment in enumerate(prop.get("files") or [])
        ]
    return None


def parse_props(page: dict[str, Any]) -> dict[str, Any]:
    """Notion page dict를 property 이름→값의 평탄한 dict로 정규화한다(순수 함수).

    페이지 식별자(``id``)와 ``last_edited_time``도 결과에 포함한다(증분 커서·PK 매핑용).
    property 이름과 충돌하지 않도록 이 둘은 property 평탄화 뒤에 덮어써 항상 페이지
    본체 값이 이긴다.

    Args:
        page: Notion query API가 돌려준 page 객체.

    Returns:
        ``{property_name: value, "id": ..., "last_edited_time": ...}``.
    """
    parsed: dict[str, Any] = {}
    for name, prop in (page.get("properties") or {}).items():
        parsed[name] = _flatten_property(prop)
    parsed["id"] = page.get("id")
    parsed["last_edited_time"] = page.get("last_edited_time")
    return parsed


class NotionSource:
    """공식 Notion REST 클라이언트(async·레이트리밋·재시도).

    한 데이터베이스를 페이지네이션으로 순회하며 원본 page dict를 스트리밍한다. 쓰기
    작업은 제공하지 않는다(P1은 읽기 전용).
    """

    def __init__(
        self,
        client: _AsyncHttpClient,
        *,
        token: str,
        api_version: str,
        rate_limit_rps: float,
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": api_version,
            "Content-Type": "application/json",
        }
        self._max_retries = max_retries
        self._sleep = sleep
        self._bucket = _TokenBucket(rate_limit_rps, sleep=sleep, clock=clock)

    async def iter_rows(
        self, data_source_id: str, since: datetime | None = None
    ) -> AsyncIterator[dict[str, Any]]:
        """데이터베이스 행을 페이지네이션으로 순회한다(원본 page dict).

        ``since``가 있으면 ``last_edited_time >= since`` 필터로 증분 조회한다.

        Args:
            data_source_id: Notion 데이터베이스 id(설정에서 주입).
            since: 증분 기준 시각(tz-aware 권장). None이면 전체 조회.

        Yields:
            Notion page 객체(``parse_props``의 입력).

        Raises:
            CorpusSyncError: 재시도 소진 또는 재시도 불가한 4xx 응답.
        """
        cursor: str | None = None
        while True:
            body: dict[str, Any] = {"page_size": QUERY_PAGE_SIZE}
            if cursor is not None:
                body["start_cursor"] = cursor
            if since is not None:
                body["filter"] = {
                    "timestamp": "last_edited_time",
                    "last_edited_time": {"on_or_after": since.isoformat()},
                }
            payload = await self._query(data_source_id, body)
            for row in payload.get("results", []):
                yield row
            if not payload.get("has_more"):
                return
            cursor = payload.get("next_cursor")
            if cursor is None:
                return

    async def _query(self, data_source_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """단일 query 요청(레이트리밋 + 429/5xx 지수 백오프 재시도)."""
        path = f"/v1/databases/{data_source_id}/query"
        for attempt in range(self._max_retries + 1):
            await self._bucket.acquire()
            response = await self._client.post(path, json=body, headers=self._headers)
            status = response.status_code
            if status < 400:
                return response.json()
            if status == _TOO_MANY_REQUESTS or status >= _SERVER_ERROR_MIN:
                if attempt >= self._max_retries:
                    raise CorpusSyncError(f"Notion API 재시도 소진: status={status}")
                delay = self._retry_delay(response, attempt)
                logger.warning(
                    "notion_query_retry", status=status, attempt=attempt, delay_seconds=delay
                )
                await self._sleep(delay)
                continue
            raise CorpusSyncError(f"Notion API 요청 실패: status={status}")
        raise CorpusSyncError("Notion API 재시도 소진")  # 방어적(루프가 먼저 반환/예외)

    def _retry_delay(self, response: httpx.Response, attempt: int) -> float:
        """재시도 대기 시간(초). Retry-After 헤더 우선, 없으면 지수 백오프."""
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
        return min(_BACKOFF_BASE_SECONDS * (2**attempt), _BACKOFF_MAX_SECONDS)

    async def aclose(self) -> None:
        """내부 HTTP 클라이언트를 종료한다(앱/사이클 종료 시)."""
        await self._client.aclose()


def create_notion_source(settings: Settings | None = None) -> NotionSource:
    """설정 기반 기본 NotionSource를 만든다(운영 진입점용).

    Args:
        settings: 주입 설정. None이면 ``get_settings()``.

    Returns:
        api.notion.com에 붙는 httpx.AsyncClient를 감싼 NotionSource.
    """
    settings = settings or get_settings()
    client = httpx.AsyncClient(base_url=NOTION_API_BASE, timeout=NOTION_TIMEOUT_SECONDS)
    return NotionSource(
        client,
        token=settings.notion_token,
        api_version=settings.notion_api_version,
        rate_limit_rps=settings.notion_rate_limit_rps,
    )
