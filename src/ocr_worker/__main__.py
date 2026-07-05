"""OCR 워커 진입점 (이슈 #15) — ``python -m ocr_worker``.

얇은 부트스트랩만 담당한다: 로깅 구성 → DB 풀 생성 → 마이그레이션 적용 → Kafka
프로듀서 준비 → 파이프라인 배선 → ``ocr-job-queue`` 소비 루프. 처리 로직은
``pipeline.py``, 소비·DLQ·수동 커밋·우아한 종료는 ``core.kafka.consumer``가 맡는다.

수명 순서(진입 시 자원 확보, 종료 시 역순 정리):
  db_pool → run_migrations → KafkaProducer(ReportJob 발행) → KafkaConsumer.run()
``KafkaConsumer``는 자체 DLQ 프로듀서를 열고 SIGTERM/SIGINT에 우아하게 멈춘다.
로컬 PG·RDS에 같은 ``migrations/*.sql``을 적용해 스키마 드리프트를 차단한다(#19).
"""

import asyncio

from core.config import get_settings
from core.contracts import OcrJob
from core.db import db_pool, run_migrations
from core.kafka.consumer import KafkaConsumer
from core.kafka.producer import KafkaProducer
from core.logging import configure_logging, get_logger
from ocr_worker.pipeline import OcrPipeline

logger = get_logger(__name__)

# 마이그레이션 SQL 디렉터리(리포지토리 루트 기준 상대). 진입 시 멱등 DDL을 적용한다.
_MIGRATIONS_DIR = "migrations"


async def _run() -> None:
    """자원을 배선하고 소비 루프를 돈다(종료 신호까지)."""
    settings = get_settings()
    async with db_pool(settings) as pool:
        applied = await run_migrations(pool, _MIGRATIONS_DIR)
        logger.info("migrations applied", files=applied)
        async with KafkaProducer(settings) as producer:
            pipeline = OcrPipeline(pool=pool, producer=producer, settings=settings)
            consumer: KafkaConsumer[OcrJob] = KafkaConsumer(
                topic=settings.kafka_ocr_job_topic,
                schema=OcrJob,
                handler=pipeline.handle,
                settings=settings,
            )
            logger.info("ocr worker starting", topic=settings.kafka_ocr_job_topic)
            await consumer.run()


def main() -> None:
    """프로세스 진입점. 로깅을 구성하고 이벤트 루프를 돌린다."""
    configure_logging()
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:  # 시그널 핸들러 미지원 플랫폼(Windows 등)의 종료 경로
        logger.info("ocr worker interrupted")


if __name__ == "__main__":
    main()
