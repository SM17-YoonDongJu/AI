"""notion_source 파싱·클라이언트 테스트 (이슈 #35 P1).

``parse_props``는 네트워크 없이 property 타입별 평탄화를 검증하고, ``NotionSource``는
페이크 httpx 클라이언트로 페이지네이션·증분 필터·재시도를 검증한다.
"""

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from core.exceptions import CorpusSyncError
from corpus_worker.notion_source import NotionSource, parse_props


def _page() -> dict[str, Any]:
    return {
        "id": "page-1",
        "last_edited_time": "2024-05-01T09:30:00.000Z",
        "properties": {
            "이름": {"type": "title", "title": [{"plain_text": "표준약관"}]},
            "메모": {"type": "rich_text", "rich_text": [{"plain_text": "설명"}]},
            "카테고리": {"type": "select", "select": {"name": "terms"}},
            "우선순위": {"type": "select", "select": None},
            "법률검토필요": {"type": "checkbox", "checkbox": True},
            "신뢰도": {"type": "select", "select": {"name": "1"}},
            "시행일": {"type": "date", "date": {"start": "2024-01-01", "end": None}},
            "파일크기KB": {"type": "number", "number": 12.5},
            "출처URL": {"type": "url", "url": "https://example.com"},
            "출처": {"type": "relation", "relation": [{"id": "rel-1"}, {"id": "rel-2"}]},
            "파일": {
                "type": "files",
                "files": [
                    {
                        "name": "second.pdf",
                        "type": "file",
                        "file": {"url": "https://s3/secret?token=x", "expiry_time": "t"},
                    },
                    {
                        "name": "first.pdf",
                        "type": "external",
                        "external": {"url": "https://ext/first"},
                    },
                ],
            },
        },
    }


class FakeClient:
    """httpx.AsyncClient.post 흉내 — 준비된 응답을 순서대로 돌려주고 호출을 기록한다."""

    def __init__(self, responses: list[httpx.Response]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, dict[str, Any], dict[str, str]]] = []
        self.get_calls: list[tuple[str, dict[str, str]]] = []

    async def post(
        self, url: str, *, json: dict[str, Any], headers: dict[str, str]
    ) -> httpx.Response:
        self.calls.append((url, json, headers))
        return self._responses.pop(0)

    async def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.get_calls.append((url, headers))
        return self._responses.pop(0)

    async def aclose(self) -> None:
        return None


async def _noop_sleep(_seconds: float) -> None:
    return None


# --------------------------------------------------------------------------- #
# parse_props (순수)
# --------------------------------------------------------------------------- #


def test_parse_props_flattens_scalar_types() -> None:
    # Arrange / Act
    parsed = parse_props(_page())

    # Assert: 타입별 평탄화 계약
    assert parsed["이름"] == "표준약관"
    assert parsed["메모"] == "설명"
    assert parsed["카테고리"] == "terms"
    assert parsed["우선순위"] is None  # select None → None
    assert parsed["법률검토필요"] is True
    assert parsed["신뢰도"] == "1"
    assert parsed["시행일"] == "2024-01-01"
    assert parsed["파일크기KB"] == 12.5
    assert parsed["출처URL"] == "https://example.com"


def test_parse_props_extracts_page_identity() -> None:
    # Arrange / Act
    parsed = parse_props(_page())

    # Assert
    assert parsed["id"] == "page-1"
    assert parsed["last_edited_time"] == "2024-05-01T09:30:00.000Z"


def test_parse_props_relation_returns_page_ids() -> None:
    # Arrange / Act
    parsed = parse_props(_page())

    # Assert
    assert parsed["출처"] == ["rel-1", "rel-2"]


def test_parse_props_files_keep_name_order_not_url() -> None:
    # Arrange / Act
    files = parse_props(_page())["파일"]

    # Assert: 개수·이름·순서만, 만료 URL은 저장하지 않음
    assert files == [
        {"name": "second.pdf", "order": 0},
        {"name": "first.pdf", "order": 1},
    ]
    for entry in files:
        assert set(entry.keys()) == {"name", "order"}


# --------------------------------------------------------------------------- #
# NotionSource (페이크 클라이언트)
# --------------------------------------------------------------------------- #


async def test_iter_rows_paginates_until_has_more_false() -> None:
    # Arrange
    resp1 = httpx.Response(
        200, json={"results": [{"id": "a"}], "has_more": True, "next_cursor": "c1"}
    )
    resp2 = httpx.Response(
        200, json={"results": [{"id": "b"}], "has_more": False, "next_cursor": None}
    )
    client = FakeClient([resp1, resp2])
    source = NotionSource(client, token="tok", api_version="2022-06-28", rate_limit_rps=1000.0)

    # Act
    rows = [row async for row in source.iter_rows("ds-1")]

    # Assert
    assert [row["id"] for row in rows] == ["a", "b"]
    assert client.calls[0][2]["Authorization"] == "Bearer tok"
    assert client.calls[0][2]["Notion-Version"] == "2022-06-28"
    assert client.calls[1][1]["start_cursor"] == "c1"  # 2번째 요청은 커서 이어받음


async def test_iter_rows_applies_since_filter() -> None:
    # Arrange
    resp = httpx.Response(200, json={"results": [], "has_more": False})
    client = FakeClient([resp])
    source = NotionSource(client, token="t", api_version="v", rate_limit_rps=1000.0)
    since = datetime(2024, 1, 1, tzinfo=UTC)

    # Act
    _ = [row async for row in source.iter_rows("ds-1", since=since)]

    # Assert
    body_filter = client.calls[0][1]["filter"]
    assert body_filter["timestamp"] == "last_edited_time"
    assert body_filter["last_edited_time"]["on_or_after"].startswith("2024-01-01")


async def test_query_retries_on_429_then_succeeds() -> None:
    # Arrange
    resp_429 = httpx.Response(429, json={}, headers={"Retry-After": "0"})
    resp_ok = httpx.Response(200, json={"results": [{"id": "a"}], "has_more": False})
    client = FakeClient([resp_429, resp_ok])
    source = NotionSource(
        client, token="t", api_version="v", rate_limit_rps=1000.0, sleep=_noop_sleep
    )

    # Act
    rows = [row async for row in source.iter_rows("ds-1")]

    # Assert
    assert [row["id"] for row in rows] == ["a"]
    assert len(client.calls) == 2  # 429 → 재시도 → 성공


async def test_query_raises_on_non_retryable_4xx() -> None:
    # Arrange
    client = FakeClient([httpx.Response(401, json={"message": "unauthorized"})])
    source = NotionSource(client, token="t", api_version="v", rate_limit_rps=1000.0)

    # Act / Assert
    with pytest.raises(CorpusSyncError):
        _ = [row async for row in source.iter_rows("ds-1")]


async def test_query_raises_after_retry_exhaustion() -> None:
    # Arrange: 5xx가 재시도 한도(max_retries=1)를 넘어 계속됨
    responses = [httpx.Response(503, json={}) for _ in range(3)]
    client = FakeClient(responses)
    source = NotionSource(
        client,
        token="t",
        api_version="v",
        rate_limit_rps=1000.0,
        max_retries=1,
        sleep=_noop_sleep,
    )

    # Act / Assert
    with pytest.raises(CorpusSyncError):
        _ = [row async for row in source.iter_rows("ds-1")]


# --------------------------------------------------------------------------- #
# file_urls (업로드 직전 신선 URL 재조회)
# --------------------------------------------------------------------------- #


def _page_with_files(files: list[dict[str, Any]]) -> dict[str, Any]:
    return {"id": "page-1", "properties": {"파일": {"type": "files", "files": files}}}


async def test_file_urls_returns_urls_in_part_order() -> None:
    # Arrange: file(Notion-hosted 만료 URL)·external 혼재, 배열 순서 = part_order
    files = [
        {"name": "part0.pdf", "type": "file", "file": {"url": "https://s3/0?token=a"}},
        {"name": "part1.pdf", "type": "external", "external": {"url": "https://ext/1"}},
    ]
    client = FakeClient([httpx.Response(200, json=_page_with_files(files))])
    source = NotionSource(client, token="t", api_version="v", rate_limit_rps=1000.0)

    # Act
    urls = await source.file_urls("page-1")

    # Assert: order 순서대로 신선 URL 반환 + GET 페이지 경로
    assert urls == ["https://s3/0?token=a", "https://ext/1"]
    assert client.get_calls[0][0] == "/v1/pages/page-1"
    assert client.get_calls[0][1]["Authorization"] == "Bearer t"


async def test_file_urls_empty_when_no_attachments() -> None:
    # Arrange
    client = FakeClient([httpx.Response(200, json=_page_with_files([]))])
    source = NotionSource(client, token="t", api_version="v", rate_limit_rps=1000.0)

    # Act / Assert
    assert await source.file_urls("page-1") == []


async def test_file_urls_raises_when_attachment_url_missing() -> None:
    # Arrange: url 없는 첨부 → part_order 정합을 깨지 않게 실패로 처리
    files = [{"name": "x.pdf", "type": "file", "file": {}}]
    client = FakeClient([httpx.Response(200, json=_page_with_files(files))])
    source = NotionSource(client, token="t", api_version="v", rate_limit_rps=1000.0)

    # Act / Assert
    with pytest.raises(CorpusSyncError):
        await source.file_urls("page-1")


async def test_file_urls_retries_on_429_then_succeeds() -> None:
    # Arrange: GET도 429 재시도 경로(_send_with_retry)를 공유한다
    files = [{"name": "p.pdf", "type": "external", "external": {"url": "https://ext/p"}}]
    responses = [
        httpx.Response(429, json={}, headers={"Retry-After": "0"}),
        httpx.Response(200, json=_page_with_files(files)),
    ]
    client = FakeClient(responses)
    source = NotionSource(
        client, token="t", api_version="v", rate_limit_rps=1000.0, sleep=_noop_sleep
    )

    # Act
    urls = await source.file_urls("page-1")

    # Assert
    assert urls == ["https://ext/p"]
    assert len(client.get_calls) == 2
