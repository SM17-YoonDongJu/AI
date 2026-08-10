"""core.sqs.producer 발행 계약 테스트.

boto3·실 SQS 없이 페이크 클라이언트를 주입해, 메시지가 래퍼 없는 snake_case JSON 본문으로
지정 큐에 SendMessage 되는지 고정한다(단위 CI 무해).
"""

from typing import Any

from pydantic import BaseModel

from core.config import Settings
from core.sqs.producer import SqsProducer

_QUEUE = "https://sqs.ap-northeast-2.amazonaws.com/123456789012/report-job"


class _Report(BaseModel):
    report_id: str
    n: int


class FakeSqsClient:
    """send_message만 흉내내는 boto3 SQS 클라이언트 대역(동기)."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []  # (QueueUrl, MessageBody)

    def send_message(self, *, QueueUrl: str, MessageBody: str) -> dict[str, Any]:
        self.sent.append((QueueUrl, MessageBody))
        return {"MessageId": "m-1"}


async def test_send_serializes_model_as_json_body() -> None:
    # Arrange
    client = FakeSqsClient()
    producer = SqsProducer(Settings(), client=client)

    # Act
    await producer.send(_QUEUE, _Report(report_id="r1", n=3))

    # Assert: 지정 큐에 래퍼 없는 compact JSON 본문 1건.
    assert len(client.sent) == 1
    queue_url, body = client.sent[0]
    assert queue_url == _QUEUE
    assert body == '{"report_id":"r1","n":3}'


async def test_send_roundtrips_back_into_model() -> None:
    # 발행 본문이 소비측(model_validate_json)에서 그대로 복원되는지 — 계약 왕복 고정.
    client = FakeSqsClient()
    producer = SqsProducer(Settings(), client=client)

    await producer.send(_QUEUE, _Report(report_id="r2", n=9))

    _, body = client.sent[0]
    assert _Report.model_validate_json(body) == _Report(report_id="r2", n=9)
