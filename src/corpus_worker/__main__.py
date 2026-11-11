"""Corpus Worker 진입점 (이슈 #35 P2) — ``python -m corpus_worker``.

OCR 워커처럼 계속 떠 있는 **상시 데몬**이다. 두 태스크를 동시에 돈다:

1. **sync**: ``notion_sync_interval_seconds``마다 카탈로그·약관 미러를 증분 동기화하고,
   ``corpus_full_reconcile_every_cycles`` 사이클마다 full(삭제 반영) 동기화한다(P1 재사용).
2. **drain**: 우선순위 큐에서 문서를 claim해 파트를 S3로 스테이징한다.
   ``corpus_max_concurrent_uploads``개까지 세마포어로 동시 처리한다.

수명 순서(진입 시 확보, 종료 시 역순 정리):
  db_pool → run_migrations → reclaim_stale(1회) → NotionSource·httpx·CorpusS3 →
  gather(sync, drain). SIGTERM/SIGINT에 신규 claim을 멈추고 진행 중 문서를 마저 끝낸 뒤
  풀·httpx·S3를 정리한다(ocr_worker.__main__ 우아한 종료 패턴).
"""

from __future__ import annotations

import asyncio
import signal

import asyncpg
import httpx

from core.config import Settings, get_settings
from core.db import db_pool, run_migrations
from core.logging import configure_logging, get_logger
from corpus_worker.downloader import DownloadResult, download_to_temp
from corpus_worker.notion_source import NotionSource, create_notion_source
from corpus_worker.pipeline import ProcessDeps, process_document
from corpus_worker.repository import (
    ClaimedDoc,
    claim_next_document,
    list_parts,
    mark_document_failed,
    reclaim_stale,
)
from corpus_worker.s3 import CorpusS3
from corpus_worker.sync import run_sync_cycle

logger = get_logger(__name__)

# 마이그레이션 SQL 디렉터리. corpus_owner 전용 서브디렉터리만 가리킨다 — migrations/
# 전체를 돌리면 ai_owner 소유 오브젝트(ocr_results 등)를 건드리다 권한 에러로 기동이
# 막힌다(실측, #48~#50). ai_owner 쪽은 src/ocr_worker/__main__.py가 migrations/ai를 돈다.
_MIGRATIONS_DIR = "migrations/corpus"
# 다운로드 HTTP 타임아웃(초). 스트리밍이라 청크 read 단위 타임아웃이다(총량 아님).
_DOWNLOAD_TIMEOUT_SECONDS = 60.0


async def _run() -> None:
    """자원을 배선하고 sync·drain 태스크를 종료 신호까지 돈다."""
    settings = get_settings()
    stopping = asyncio.Event()
    _install_signal_handlers(stopping)

    async with db_pool(settings) as pool:
        applied = await run_migrations(pool, _MIGRATIONS_DIR)
        logger.info("migrations applied", files=applied)
        reclaimed = await reclaim_stale(pool, settings.corpus_stale_reclaim_seconds)
        logger.info("corpus_startup_reclaim", reclaimed=reclaimed)

        notion_source = create_notion_source(settings)
        http_client = httpx.AsyncClient(timeout=_DOWNLOAD_TIMEOUT_SECONDS)
        s3 = CorpusS3(settings=settings)

        async def download(url: str, suffix: str) -> DownloadResult:
            return await download_to_temp(
                url, settings.corpus_download_tmp_dir, client=http_client, suffix=suffix
            )

        deps = ProcessDeps(
            pool=pool,
            notion_source=notion_source,
            download=download,
            s3=s3,
            settings=settings,
        )
        try:
            logger.info("corpus worker starting", categories=settings.corpus_categories)
            await asyncio.gather(
                _run_sync(notion_source, settings, stopping, pool),
                _run_drain(deps, settings, stopping),
            )
        finally:
            await http_client.aclose()
            await notion_source.aclose()
            await s3.aclose()


async def _run_sync(
    notion_source: NotionSource,
    settings: Settings,
    stopping: asyncio.Event,
    pool: asyncpg.Pool,
) -> None:
    """주기 동기화 루프. N 사이클마다 full(삭제 반영)로 돈다."""
    cycle = 0
    while not stopping.is_set():
        cycle += 1
        every = settings.corpus_full_reconcile_every_cycles
        full = every > 0 and cycle % every == 0  # 0이면 full 비활성(0 나눗셈 방어)
        try:
            await run_sync_cycle(pool, notion_source, settings, full=full)
        except Exception:  # 최상위 격리: 한 사이클 실패가 워커를 멈추지 않게(§8)
            logger.exception("corpus_sync_cycle_error", cycle=cycle, full=full)
        await _sleep_or_stop(stopping, settings.notion_sync_interval_seconds)


async def _run_drain(deps: ProcessDeps, settings: Settings, stopping: asyncio.Event) -> None:
    """우선순위 큐 드레인 루프(세마포어로 동시 업로드 수 제한).

    claim한 문서마다 태스크를 띄우고 세마포어로 동시성을 ``corpus_max_concurrent_uploads``로
    묶는다. 종료 신호를 받으면 신규 claim을 멈추고 진행 중 태스크를 마저 기다린다.
    """
    semaphore = asyncio.Semaphore(settings.corpus_max_concurrent_uploads)
    inflight: set[asyncio.Task[None]] = set()
    try:
        while not stopping.is_set():
            await semaphore.acquire()
            if stopping.is_set():
                semaphore.release()
                break
            try:
                doc = await claim_next_document(deps.pool)
            except Exception:  # 최상위 격리: 일시적 claim 오류로 상시 데몬이 죽지 않게(§8 top-loop)
                logger.exception("corpus_claim_error")
                semaphore.release()
                await _sleep_or_stop(stopping, settings.corpus_poll_interval_seconds)
                continue
            if doc is None:
                semaphore.release()
                await _sleep_or_stop(stopping, settings.corpus_poll_interval_seconds)
                continue
            task = asyncio.create_task(_process_and_release(deps, doc, semaphore))
            inflight.add(task)
            task.add_done_callback(inflight.discard)
    finally:
        if inflight:
            logger.info("corpus_drain_awaiting_inflight", count=len(inflight))
            await asyncio.gather(*inflight, return_exceptions=True)


async def _process_and_release(
    deps: ProcessDeps, doc: ClaimedDoc, semaphore: asyncio.Semaphore
) -> None:
    """문서 1건을 처리하고 세마포어를 반납한다(개별 실패는 격리)."""
    try:
        parts = await list_parts(deps.pool, doc.notion_page_id)
        await process_document(deps, doc, parts)
    except Exception:  # 최상위 격리: 개별 문서 실패로 드레인이 멈추지 않게(§8)
        logger.exception("corpus_process_doc_error")
        # claim 뒤 list_parts 등에서 실패하면 문서가 in_progress에 갇힌다 — 재시도 상태로
        # 되돌려 startup reclaim(TTL)까지 기다리지 않게 한다(process_document 자체 실패는
        # 이미 mark 처리되므로, 여기 도달하는 건 그 밖의 경로다).
        try:
            await mark_document_failed(
                deps.pool,
                doc.notion_page_id,
                "process_error",
                deps.settings.corpus_max_attempts,
            )
        except Exception:
            logger.exception("corpus_mark_failed_error")
    finally:
        semaphore.release()


async def _sleep_or_stop(stopping: asyncio.Event, seconds: float) -> None:
    """종료 신호가 오면 즉시 깨어나는 취소 가능 대기(폴링·사이클 간격용)."""
    try:
        await asyncio.wait_for(stopping.wait(), timeout=seconds)
    except TimeoutError:
        return  # 정상 간격 경과 — 다음 반복으로


def _install_signal_handlers(stopping: asyncio.Event) -> None:
    """SIGTERM/SIGINT에 종료 이벤트를 세팅한다(미지원 플랫폼은 KeyboardInterrupt 경로)."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stopping.set)
        except NotImplementedError:
            logger.debug("signal handler unsupported", signal=sig)


def main() -> None:
    """프로세스 진입점. 로깅을 구성하고 이벤트 루프를 돌린다."""
    configure_logging()
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:  # 시그널 미지원 플랫폼(Windows 등)의 종료 경로
        logger.info("corpus worker interrupted")


if __name__ == "__main__":
    main()
