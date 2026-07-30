"""Notion 첨부 스트리밍 다운로드 (이슈 #35 P2).

업로드 직전에 받은 **신선 URL**에서 httpx 스트리밍 GET으로 임시파일에 청크 단위로 기록하며
SHA256을 증분 계산한다. 87MB급 대용량 약관도 메모리에 상주시키지 않는다(§7 async-first·
§13 로컬 미잔류). 임시파일은 호출자(pipeline)가 업로드 후 즉시 삭제한다.

httpx.AsyncClient가 구조적으로 충족하는 최소 스트리밍 계약(``_StreamingClient``)만 의존하므로
테스트에서 페이크 스트림으로 대체할 수 있다.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import Any, NamedTuple, Protocol

from core.logging import get_logger

logger = get_logger(__name__)

# 스트리밍 청크 크기(64KiB). 메모리 상주량과 syscall 횟수의 절충.
_CHUNK_SIZE = 64 * 1024
# 임시파일 접미사(약관은 전부 PDF — corpus_key도 .pdf).
_TEMP_SUFFIX = ".pdf"


class DownloadResult(NamedTuple):
    """다운로드 산출물. 튜플 언패킹·속성 접근 모두 지원한다."""

    temp_path: str
    sha256: str
    byte_size: int


class _StreamResponse(Protocol):
    """스트리밍 응답 최소 계약(httpx.Response가 구조적으로 충족)."""

    def raise_for_status(self) -> Any: ...

    def aiter_bytes(self, chunk_size: int | None = ...) -> AsyncIterator[bytes]: ...


class _StreamingClient(Protocol):
    """스트리밍 GET 클라이언트 최소 계약(httpx.AsyncClient가 구조적으로 충족)."""

    def stream(self, method: str, url: str) -> AbstractAsyncContextManager[_StreamResponse]: ...


async def download_to_temp(url: str, tmp_dir: str, *, client: _StreamingClient) -> DownloadResult:
    """URL을 스트리밍으로 임시파일에 내려받고 SHA256·크기를 함께 계산한다.

    네트워크 수신은 async로 양보하고, 각 청크의 디스크 기록·해시 갱신은 작은(64KiB) 단위라
    이벤트 루프를 사실상 막지 않는다. 실패 시 부분 파일을 지우고 예외를 전파한다.

    Args:
        url: 신선한(만료 전) 다운로드 URL.
        tmp_dir: 임시파일을 둘 디렉터리(설정 ``corpus_download_tmp_dir``).
        client: 스트리밍 GET 클라이언트(주입 — 테스트 페이크).

    Returns:
        ``DownloadResult(temp_path, sha256, byte_size)``.

    Raises:
        httpx.HTTPError: 다운로드 실패(상태 코드·연결·스트림 오류).
        OSError: 임시 디렉터리 생성·기록 실패.
    """
    os.makedirs(tmp_dir, exist_ok=True)
    # 원시 fd에 os.write로 직접 쓴다 — 느린 부분(네트워크 수신)은 async로 양보하고,
    # 64KiB 버퍼드 write·해시 갱신은 이벤트 루프를 사실상 막지 않는다.
    fd, temp_path = tempfile.mkstemp(suffix=_TEMP_SUFFIX, dir=tmp_dir)
    hasher = hashlib.sha256()
    byte_size = 0
    completed = False
    try:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes(_CHUNK_SIZE):
                os.write(fd, chunk)
                hasher.update(chunk)
                byte_size += len(chunk)
        completed = True
    finally:
        os.close(fd)
        if not completed:
            _safe_remove(temp_path)  # 부분 파일이 로컬에 잔류하지 않게 정리
    logger.info("corpus_download_done", byte_size=byte_size)
    return DownloadResult(temp_path=temp_path, sha256=hasher.hexdigest(), byte_size=byte_size)


def _safe_remove(path: str) -> None:
    """임시파일을 조용히 지운다(이미 없거나 지울 수 없어도 삼킨다)."""
    try:
        os.remove(path)
    except OSError as exc:
        logger.warning("corpus_temp_remove_failed", error=str(exc))
