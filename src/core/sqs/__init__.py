"""boto3 기반 SQS consumer/producer 래퍼 — 직접 클라이언트 생성 대신 항상 이 래퍼를 쓴다."""

from core.sqs.consumer import SqsConsumer
from core.sqs.producer import SqsProducer

__all__ = ["SqsConsumer", "SqsProducer"]
