"""약관 문서 1건 스테이징 파이프라인 (이슈 #35 P2).

우선순위 큐에서 claim한 문서 하나를 파트 순서대로 처리한다: 신선 URL 조회 → 스트리밍
다운로드(+SHA256) → 내용주소 키로 dedup 업로드 → 파트/문서 상태 전이. 청킹·임베딩·OCR은
범위 밖이다(Notion 첨부 → S3까지).

멱등·재개 규약:
- **파트 단위 재개**: 이미 ``uploaded``(sha256 보유)인 파트는 건너뛴다 — 재claim 시
  중단 지점부터 이어서 처리한다.
- **내용주소 dedup**: 같은 내용은 같은 키(``corpus/{category}/{sha256}.pdf``)라, HeadObject로
  존재를 확인해 이미 있으면 업로드를 생략한다.
- **로컬 미잔류**: 임시파일은 파트 처리 직후(성공·실패 불문) 삭제한다(§13).

I/O 경계(notion_source·download·s3·pool)를 주입 가능하게 두어 네트워크·S3·DB 없이
단위 테스트한다. 실패는 구체 예외만 포착해 ``mark_document_failed``로 반영한다(§8).
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

import asyncpg
import httpx

from core.config import Settings
from core.exceptions import CorpusStagingError, CorpusSyncError
from core.logging import bind_context, clear_context, get_logger
from corpus_worker import filetype
from corpus_worker.downloader import DownloadResult
from corpus_worker.repository import (
    ClaimedDoc,
    PartRow,
    mark_document_done,
    mark_document_failed,
    mark_part_uploaded,
)
from corpus_worker.s3 import corpus_key

logger = get_logger(__name__)

# 파트 처리 중 예상되는 구체 실패군 — 이 묶음만 포착해 문서를 실패 처리하고 재시도에 맡긴다.
# (boto3 예외는 s3 경계가 CorpusStagingError로 감싸므로 여기서 botocore를 import하지 않는다.)
_STAGING_FAILURES = (
    httpx.HTTPError,
    OSError,
    asyncpg.PostgresError,
    CorpusSyncError,
    CorpusStagingError,
)

# (url, 임시파일 접미사) → 다운로드 산출물. 임시 디렉터리·httpx 클라이언트는 배선 시 바인딩한다.
type DownloadFn = Callable[[str, str], Awaitable[DownloadResult]]


class _UrlSource(Protocol):
    """신선 URL 조회 계약(NotionSource가 구조적으로 충족)."""

    async def file_urls(self, page_id: str) -> list[str]: ...


class _S3Boundary(Protocol):
    """S3 스테이징 계약(CorpusS3가 구조적으로 충족)."""

    async def head_exists(self, key: str) -> bool: ...

    async def put_file(self, temp_path: str, key: str, *, content_type: str, sse: str) -> None: ...


@dataclass(slots=True)
class ProcessDeps:
    """문서 처리에 필요한 I/O 경계 묶음(전부 주입 — 테스트 페이크)."""

    pool: asyncpg.Pool
    notion_source: _UrlSource
    download: DownloadFn
    s3: _S3Boundary
    settings: Settings


async def process_document(deps: ProcessDeps, doc: ClaimedDoc, parts: list[PartRow]) -> None:
    """문서 1건을 파트 순서대로 스테이징하고 상태를 전이한다.

    성공하면 ``done``으로, 구체 실패면 ``mark_document_failed``(attempts++)로 반영한다 —
    상한 미만이면 ``pending``으로 되돌아가 다음 claim에서 재개된다. 예외를 삼키지 않되,
    최상위 드레인 루프가 개별 문서 실패로 멈추지 않게 여기서 격리한다(§8).

    Args:
        deps: 주입된 I/O 경계 묶음.
        doc: claim한 문서(``in_progress``).
        parts: 문서 파트 목록(part_order 순서).
    """
    bind_context(corpus_page_id=doc.notion_page_id)
    try:
        await _stage_parts(deps, doc, parts)
        await mark_document_done(deps.pool, doc.notion_page_id)
        logger.info("corpus_document_done", parts=len(parts))
    except _STAGING_FAILURES as exc:
        reason = type(exc).__name__  # 사유는 예외 종류만 — URL/토큰 등 PII·시크릿 미기록(§13)
        logger.warning("corpus_document_failed", reason=reason)
        await mark_document_failed(
            deps.pool, doc.notion_page_id, reason, deps.settings.corpus_max_attempts
        )
    finally:
        clear_context()


async def _stage_parts(deps: ProcessDeps, doc: ClaimedDoc, parts: list[PartRow]) -> None:
    """미완료 파트를 순서대로 스테이징한다(신선 URL은 문서당 1회만 조회).

    Notion-hosted URL은 만료되므로, 이 문서에 올릴 파트가 하나라도 있을 때 ``file_urls``를
    **한 번만** 호출해 리스트를 재사용한다(파트마다 재조회 금지).
    """
    urls: list[str] | None = None
    for part in parts:
        if part.status == "uploaded" and part.sha256:
            continue  # 멱등 재개: 이미 업로드된 파트는 건너뛴다
        if urls is None:
            urls = await deps.notion_source.file_urls(doc.notion_page_id)
        url = _part_url(urls, part.part_order, doc.notion_page_id)
        await _stage_one_part(deps, doc, part, url)


async def _stage_one_part(deps: ProcessDeps, doc: ClaimedDoc, part: PartRow, url: str) -> None:
    """파트 1개를 다운로드→(dedup)업로드→상태전이한다. 임시파일은 반드시 정리한다.

    타입(확장자·ContentType)은 다운로드 전에 ``notion_file_name``에서 판정한다 — 실패 시
    (파일명 없음·미상 확장자) 네트워크 호출 없이 바로 예외를 던진다.
    """
    file_type = filetype.detect(part.notion_file_name)
    result = await deps.download(url, file_type.ext)
    try:
        key = corpus_key(doc.category, result.sha256, file_type.ext, settings=deps.settings)
        if await deps.s3.head_exists(key):
            logger.info("corpus_dedup_hit", key=key)  # 같은 내용 이미 존재 → 업로드 생략
        else:
            await deps.s3.put_file(
                result.temp_path,
                key,
                content_type=file_type.content_type,
                sse=deps.settings.s3_corpus_sse,
            )
        await mark_part_uploaded(deps.pool, part.id, result.sha256, key, result.byte_size)
    finally:
        _remove_temp(result.temp_path)


def _part_url(urls: list[str], part_order: int, page_id: str) -> str:
    """part_order에 대응하는 URL을 안전하게 꺼낸다(Notion 파트 수 불일치 방어).

    Raises:
        CorpusSyncError: part_order가 재조회한 URL 목록 범위를 벗어난 경우.
    """
    if part_order < 0 or part_order >= len(urls):
        raise CorpusSyncError(
            f"파트 순서 범위 초과: page={page_id} order={part_order} urls={len(urls)}"
        )
    return urls[part_order]


def _remove_temp(path: str) -> None:
    """임시파일을 정리한다(이미 없거나 지울 수 없어도 삼킨다 — 로컬 미잔류 보장)."""
    try:
        os.remove(path)
    except OSError as exc:
        logger.warning("corpus_temp_remove_failed", error=str(exc))
