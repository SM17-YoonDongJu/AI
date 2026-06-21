"""aiokafka 프로듀서 래퍼.

결과 이벤트(예: ReportJob)를 토픽에 발행한다. pydantic 모델 → UTF-8 JSON 직렬화,
파티션 키로 멱등·순서를 보장한다(`enable_idempotence`). DLQ용 raw bytes 경로도 제공한다.
직접 클라이언트를 만들지 않고 항상 이 래퍼를 거친다.
"""

from typing import Self

from aiokafka import AIOKafkaProducer
from pydantic import BaseModel

from core.config import Settings, get_settings
from core.logging import get_logger

logger = get_logger(__name__)


class KafkaProducer:
    """수명관리되는 aiokafka 프로듀서. `async with KafkaProducer()`로 사용한다."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._producer: AIOKafkaProducer | None = None

    async def __aenter__(self) -> Self:
        producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            security_protocol=self._settings.kafka_security_protocol,
            acks="all",
            enable_idempotence=True,
        )
        await producer.start()
        self._producer = producer
        logger.info("producer started")
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._producer is not None:
            await self._producer.stop()
            logger.info("producer stopped")

    def _require(self) -> AIOKafkaProducer:
        if self._producer is None:
            raise RuntimeError("producer not started; use 'async with KafkaProducer()'")
        return self._producer

    async def publish(self, topic: str, message: BaseModel, *, key: str) -> None:
        """pydantic 메시지를 토픽에 발행한다(key 기준 파티셔닝)."""
        payload = message.model_dump_json().encode("utf-8")
        await self._require().send_and_wait(topic, value=payload, key=key.encode("utf-8"))
        logger.info("published", topic=topic, key=key)

    async def publish_raw(self, topic: str, value: bytes | None, key: bytes | None) -> None:
        """raw bytes를 그대로 발행한다(DLQ 전달 등 — 원본 유실 방지)."""
        await self._require().send_and_wait(topic, value=value, key=key)
        logger.warning("published raw", topic=topic)
