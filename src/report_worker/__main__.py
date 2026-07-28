"""report-worker 진입점 — report-job Kafka 컨슈머.

    python -m report_worker

설정·DB풀·컨슈머를 배선한다. 소비→검증→재시도→DLQ(report-job.dlq)→커밋·우아한 종료는
KafkaConsumer가 담당한다. 노드는 db.get_pool()(전역 싱글턴)을 쓰므로 init_pool()로 초기화한다.
"""

from __future__ import annotations

import asyncio

from core.config import get_settings
from core.contracts import REPORT_JOB_TOPIC, ReportJob
from core.db import close_pool, init_pool
from core.kafka import KafkaConsumer
from core.logging import configure_logging, get_logger
from report_worker.worker import handle_job

logger = get_logger(__name__)


async def main() -> None:
    configure_logging()
    settings = get_settings()
    await init_pool(settings)
    try:
        consumer = KafkaConsumer(REPORT_JOB_TOPIC, ReportJob, handle_job, settings=settings)
        logger.info(
            "report-worker starting",
            topic=REPORT_JOB_TOPIC,
            group=settings.kafka_consumer_group,
        )
        await consumer.run()
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
