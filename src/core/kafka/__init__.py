"""aiokafka consumer/producer 래퍼 — 직접 클라이언트 생성 대신 항상 이 래퍼를 쓴다."""

from core.kafka.consumer import KafkaConsumer
from core.kafka.producer import KafkaProducer

__all__ = ["KafkaConsumer", "KafkaProducer"]
