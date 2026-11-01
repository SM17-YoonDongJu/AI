"""report-worker 진입점 — report-job SQS 컨슈머.

    python -m report_worker

설정·DB풀·마이그레이션·컨슈머를 배선한다. 소비→검증→ack(DeleteMessage)→실패=삭제 안 함(재전달)
→poison 스킵·우아한 종료는 SqsConsumer가 담당한다. 노드는 db.get_pool()(전역 싱글턴)을 쓰므로
init_pool()로 초기화한다.

수명 순서(진입 시 자원 확보, 종료 시 역순 정리):
  init_pool → run_migrations("migrations/ai") → SqsConsumer(journaled_handler, on_poison).run()
  → close_pool

마이그레이션: GPU EC2의 OCR 워커가 꺼져 있어도 report가 자기 테이블(`ai.report_job_failures`)을
보장하도록 기동 때 `migrations/ai`를 적용한다(OCR과 같은 디렉터리 — 둘이 동시에 기동해도
`run_migrations`의 디렉터리별 advisory lock이 한 번에 하나만 적용한다). 적용이 실패하면 기동도
실패한다(OCR과 같다 — 저널 없이 돌면 poison이 다시 무음 실패가 된다).

실패 저널: 핸들러는 `worker.journaled_handler`다(재전달마다 비종결 행, 종결 시 삭제). 재전달
한도를 다 쓴 poison은 컨슈머가 지우기 직전 `_poison_journal` 훅이 terminal로 확정한다 —
Spring이 그 행을 보고 FAILED를 표시한다.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path

from core.config import get_settings
from core.contracts import ReportJob
from core.db import close_pool, init_pool, run_migrations
from core.logging import configure_logging, get_logger
from core.sqs import SqsConsumer
from report_worker.failure_journal import Executor, mark_report_failure_terminal
from report_worker.worker import journaled_handler

logger = get_logger(__name__)

# 마이그레이션 SQL 디렉터리(작업 디렉터리 기준 상대 — 이미지 WORKDIR /app, 로컬은 레포 루트).
# ai_owner 전용 서브디렉터리만 가리킨다 — migrations/ 전체를 돌리면 corpus_owner 소유
# 오브젝트를 건드리다 권한 에러로 기동이 막힌다(§14, 실측 #48~#50). ocr_worker와 같은 값이다.
_MIGRATIONS_DIR = "migrations/ai"


def _require_migrations_dir() -> None:
    """마이그레이션 디렉터리가 없으면 기동을 멈춘다(동기 파일시스템 확인 — 스레드에서 부른다).

    디렉터리가 없으면 ``run_migrations``는 오류 없이 0개를 적용한다(glob). 그러면 저널 테이블
    보장이 조용히 깨진다(이미지에서 ``COPY migrations/ai``가 빠졌거나 작업 디렉터리가 다름).

    Raises:
        FileNotFoundError: ``_MIGRATIONS_DIR``가 디렉터리가 아닐 때.
    """
    if not Path(_MIGRATIONS_DIR).is_dir():
        raise FileNotFoundError(
            f"마이그레이션 디렉터리가 없다: {_MIGRATIONS_DIR} (cwd={Path.cwd()})"
        )


async def main() -> None:
    """자원을 배선하고 소비 루프를 돈다(종료 신호까지)."""
    configure_logging()
    settings = get_settings()
    await asyncio.to_thread(_require_migrations_dir)
    pool = await init_pool(settings)
    try:
        applied = await run_migrations(pool, _MIGRATIONS_DIR)
        logger.info("migrations applied", files=applied)
        consumer = SqsConsumer(
            settings.sqs_report_job_queue_url,
            ReportJob,
            journaled_handler(pool),
            settings=settings,
            on_poison=_poison_journal(pool),
        )
        logger.info(
            "report-worker starting",
            queue_url=settings.sqs_report_job_queue_url,
            git_commit=settings.git_commit,
        )
        await consumer.run()
    finally:
        await close_pool()


def _poison_journal(pool: Executor) -> Callable[[ReportJob | None, str, int], Awaitable[None]]:
    """poison 메시지를 걷어내기 직전 실패 저널에 확정(terminal) 기록하는 훅을 만든다.

    컨슈머의 poison 가드는 수신 횟수 상한을 넘긴 메시지를 **삭제**한다. 실패한 리포트는
    저장하지 않으므로, 여기서 기록하지 않으면 그 리포트는 Spring에서 영원히 처리 중으로 남는다.

    **저널 기록의 예외는 삼키지 않는다**(OCR `_poison_journal`과 같다) — 컨슈머가 훅의 성공
    여부로 삭제/보류를 정하기 때문이다(`SqsConsumer._run_poison_hook`). 여기서 잡아 로그만
    남기면 컨슈머는 성공으로 오인해 메시지를 지우고, 정확히 이 훅이 막으려던 무음 실패가 다시
    생긴다. 예외를 올리면 컨슈머가 타입만 경고로 남기고(§9) 메시지를 보류한다 — 다음 재전달에서
    다시 시도하며, 큐 보존기간이 상한이라 유계다. 소비 루프는 멈추지 않는다.

    **알려진 한계**: 본문 역직렬화 자체가 실패하면(``job is None``) report_id를 알 수 없어
    ``message_id``로만 ``schema_invalid`` 행을 남긴다. Spring은 그 리포트를 조인할 수 없어
    PROCESSING에 머문다 — 운영 조회와 수동 개입이 복구 수단이다(OCR 저널과 같은 한계).

    Args:
        pool: asyncpg 연결 풀(워커 수명과 같다 — 클로저로 잡아 둔다).

    Returns:
        ``SqsConsumer(on_poison=...)``에 넘길 훅.
    """

    async def on_poison(job: ReportJob | None, message_id: str, receive_count: int) -> None:
        await mark_report_failure_terminal(
            pool, job=job, message_id=message_id, receive_count=receive_count
        )

    return on_poison


if __name__ == "__main__":
    asyncio.run(main())
