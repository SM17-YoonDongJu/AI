"""downloader.download_to_temp 테스트 (이슈 #35 P2).

페이크 스트리밍 클라이언트로 네트워크 없이 검증한다: 청크 경계와 무관하게 SHA256·크기가
정확한지, 실패 시 부분 임시파일이 잔류하지 않는지(로컬 미잔류).
"""

import hashlib
import os
from collections.abc import AsyncIterator

import httpx
import pytest

from corpus_worker.downloader import download_to_temp


class FakeStreamResponse:
    """httpx 스트리밍 응답 흉내 — 준비된 청크를 순서대로 흘려보낸다."""

    def __init__(self, chunks: list[bytes], status: int = 200) -> None:
        self._chunks = chunks
        self._status = status

    def raise_for_status(self) -> None:
        if self._status >= 400:
            raise httpx.HTTPError(f"simulated status {self._status}")

    async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk


class _FakeStreamCM:
    """stream() 결과 async 컨텍스트 매니저."""

    def __init__(self, response: FakeStreamResponse) -> None:
        self._response = response

    async def __aenter__(self) -> FakeStreamResponse:
        return self._response

    async def __aexit__(self, *exc: object) -> bool:
        return False


class FakeStreamingClient:
    """httpx.AsyncClient.stream 흉내 — 준비된 청크를 돌려주고 호출을 기록한다."""

    def __init__(self, chunks: list[bytes], status: int = 200) -> None:
        self._chunks = chunks
        self._status = status
        self.calls: list[tuple[str, str]] = []

    def stream(self, method: str, url: str) -> _FakeStreamCM:
        self.calls.append((method, url))
        return _FakeStreamCM(FakeStreamResponse(self._chunks, self._status))


def _chunked(data: bytes, size: int) -> list[bytes]:
    return [data[index : index + size] for index in range(0, len(data), size)]


async def test_download_computes_sha256_and_size(tmp_path) -> None:
    # Arrange: 임의 청크 경계로 쪼갠 고정 바이트 스트림
    data = b"insurance-terms-payload-" * 500
    client = FakeStreamingClient(_chunked(data, 7))

    # Act
    result = await download_to_temp("https://x/f.pdf", str(tmp_path), client=client, suffix=".pdf")

    # Assert: 청크 경계와 무관하게 해시·크기 정확
    assert result.byte_size == len(data)
    assert result.sha256 == hashlib.sha256(data).hexdigest()
    assert result.temp_path.endswith(".pdf")  # suffix가 임시파일명에 반영됨
    with open(result.temp_path, "rb") as file:
        assert file.read() == data
    assert client.calls == [("GET", "https://x/f.pdf")]
    os.remove(result.temp_path)


async def test_download_handles_empty_stream(tmp_path) -> None:
    # Arrange
    client = FakeStreamingClient([])

    # Act
    result = await download_to_temp(
        "https://x/empty.pdf", str(tmp_path), client=client, suffix=".pdf"
    )

    # Assert
    assert result.byte_size == 0
    assert result.sha256 == hashlib.sha256(b"").hexdigest()
    os.remove(result.temp_path)


async def test_download_removes_partial_temp_on_failure(tmp_path) -> None:
    # Arrange: 5xx → raise_for_status가 예외
    client = FakeStreamingClient([b"partial"], status=500)

    # Act / Assert
    with pytest.raises(httpx.HTTPError):
        await download_to_temp("https://x/f.pdf", str(tmp_path), client=client, suffix=".pdf")
    # 부분 임시파일이 로컬에 남지 않아야 한다
    assert os.listdir(tmp_path) == []
