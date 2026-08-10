"""report-worker 진입점 — report-job SQS 컨슈머.

    python -m report_worker

설정·DB풀·컨슈머를 배선한다. 소비→검증→ack(DeleteMessage)→실패=삭제 안 함(재전달)→poison
스킵·우아한 종료는 SqsConsumer가 담당한다. 노드는 db.get_pool()(전역 싱글턴)을 쓰므로
init_pool()로 초기화한다.
"""

from __future__ import annotations

import asyncio

from core.config import get_settings
from core.contracts import ReportJob
from core.db import close_pool, init_pool
from core.logging import configure_logging, get_logger
from core.sqs import SqsConsumer
from report_worker.worker import handle_job

logger = get_logger(__name__)


async def main() -> None:
    configure_logging()
    settings = get_settings()
    await init_pool(settings)
    try:
        consumer = SqsConsumer(
            settings.sqs_report_job_queue_url, ReportJob, handle_job, settings=settings
        )
        logger.info("report-worker starting", queue_url=settings.sqs_report_job_queue_url)
        await consumer.run()
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
