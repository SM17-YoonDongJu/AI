"""aiokafka 컨슈머 래퍼.

토픽을 소비해 raw 메시지를 pydantic 모델로 즉시 검증하고 핸들러로 넘긴다.
- 역직렬화/검증 실패 → DLQ (파이프라인 미차단)
- 핸들러 일시 실패 → 인프로세스 재시도, 초과 시 DLQ
- 처리 성공 후 **수동 오프셋 커밋**(at-least-once; 핸들러는 멱등이어야 함)
- SIGTERM/SIGINT 우아한 종료
"""

import asyncio
import signal
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord
from pydantic import BaseModel, ValidationError

from core.config import Settings, get_settings
from core.kafka.producer import KafkaProducer
from core.logging import get_logger

logger = get_logger(__name__)

_POLL_TIMEOUT_MS = 1000
_BATCH_MAX_RECORDS = 1
_BACKOFF_BASE = 2
_MAX_BACKOFF_SECONDS = 10


class KafkaConsumer[T: BaseModel]:
    """토픽을 소비해 검증 후 핸들러로 넘기는 컨슈머.

    핸들러는 **멱등**해야 한다(at-least-once 재처리 안전). 검증 실패·재시도 초과
    메시지는 DLQ로 보내 원본을 유실하지 않는다.
    """

    def __init__(
        self,
        topic: str,
        schema: type[T],
        handler: Callable[[T], Awaitable[None]],
        *,
        settings: Settings | None = None,
    ) -> None:
        self._topic = topic
        self._schema = schema
        self._handler = handler
        self._settings = settings or get_settings()
        self._stopping = asyncio.Event()

    async def run(self) -> None:
        """컨슈머를 시작해 종료 신호까지 처리 루프를 돈다."""
        settings = self._settings
        consumer: AIOKafkaConsumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            security_protocol=settings.kafka_security_protocol,
            group_id=settings.kafka_consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._install_signal_handlers()
        await consumer.start()
        logger.info("consumer started", topic=self._topic, group=settings.kafka_consumer_group)
        try:
            async with KafkaProducer(settings) as dlq_producer:
                await self._consume_loop(consumer, dlq_producer)
        finally:
            await consumer.stop()
            logger.info("consumer stopped", topic=self._topic)

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._stopping.set)
            except NotImplementedError:
                # Windows 등 미지원 플랫폼 — KeyboardInterrupt로 종료된다.
                logger.debug("signal handler unsupported", signal=sig)

    async def _consume_loop(self, consumer: AIOKafkaConsumer, dlq: KafkaProducer) -> None:
        while not self._stopping.is_set():
            batches = await consumer.getmany(
                timeout_ms=_POLL_TIMEOUT_MS, max_records=_BATCH_MAX_RECORDS
            )
            for records in batches.values():
                for record in records:
                    await self._process(record, dlq)
                    await consumer.commit()  # 처리 성공 후에만 커밋(at-least-once)

    async def _process(self, record: ConsumerRecord, dlq: KafkaProducer) -> None:
        try:
            model = self._schema.model_validate_json(record.value)
        except ValidationError as exc:
            logger.warning("invalid message → DLQ", topic=self._topic, error=str(exc))
            await self._to_dlq(dlq, record)
            return
        await self._handle_with_retry(model, record, dlq)

    async def _handle_with_retry(
        self, model: T, record: ConsumerRecord, dlq: KafkaProducer
    ) -> None:
        retries = self._settings.kafka_max_retries
        for attempt in range(1, retries + 1):
            try:
                await self._handler(model)
                return
            except Exception:  # 최상위 격리: 유실 없이 재시도·DLQ (CODE_CONVENTIONS §8)
                logger.warning("handler error", topic=self._topic, attempt=attempt, max=retries)
                if attempt < retries:
                    await asyncio.sleep(min(_BACKOFF_BASE**attempt, _MAX_BACKOFF_SECONDS))
        logger.error("handler exhausted retries → DLQ", topic=self._topic)
        await self._to_dlq(dlq, record)

    async def _to_dlq(self, dlq: KafkaProducer, record: ConsumerRecord) -> None:
        dlq_topic = f"{self._topic}{self._settings.kafka_dlq_suffix}"
        await dlq.publish_raw(dlq_topic, record.value, record.key)
