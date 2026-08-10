"""AWS SQS 프로듀서 래퍼.

결과 이벤트(ReportJob)를 큐에 발행한다. pydantic 모델 → UTF-8 JSON 직렬화 후 SendMessage.
본문은 계약 그대로(래퍼·MessageAttributes 없음, snake_case JSON). Kafka 프로듀서와 달리 유지할
연결 수명이 없어(요청마다 서명되는 boto3 호출) 컨텍스트 매니저를 두지 않는다.

멱등·순서: Standard 큐라 파티션 키·순서 보장이 없다. 멱등은 본문 식별자(ReportJob.report_id)로
다운스트림이 책임진다 — 계약 그대로다(Kafka 시절 파티션 키였던 값도 이제 본문에만 있다).

전송 계층·자격증명은 ``core.sqs.client``가 가둔다. boto3는 동기 SDK라 ``asyncio.to_thread``로
격리한다(§7). 직접 클라이언트를 만들지 않고 항상 이 래퍼를 거친다.
"""

import asyncio
from typing import Any

from pydantic import BaseModel

from core.config import Settings, get_settings
from core.logging import get_logger
from core.sqs.client import get_sqs_client, queue_name

logger = get_logger(__name__)


class SqsProducer:
    """SQS SendMessage 래퍼. 큐 URL을 인자로 받아 pydantic 메시지를 발행한다."""

    def __init__(self, settings: Settings | None = None, *, client: Any | None = None) -> None:
        self._settings = settings or get_settings()
        self._client = client  # None이면 첫 send에서 lazy 생성(boto3)

    async def send(self, queue_url: str, message: BaseModel) -> None:
        """pydantic 메시지를 큐에 발행한다(본문 = UTF-8 JSON, 래퍼·속성 없음)."""
        if self._client is None:
            self._client = get_sqs_client(self._settings)
        body = message.model_dump_json()
        await asyncio.to_thread(self._client.send_message, QueueUrl=queue_url, MessageBody=body)
        logger.info("sqs message sent", queue=queue_name(queue_url))
