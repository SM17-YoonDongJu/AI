"""core.sqs.consumer 수신 규약 테스트.

boto3·실 SQS 없이 페이크 클라이언트를 주입해 컨슈머의 계약만 고정한다(단위 CI 무해):
- 처리 성공 → DeleteMessage(ack) 호출.
- 핸들러 실패 → 삭제하지 않음(visibility timeout 후 재전달에 맡김).
- 스키마 위반 → 핸들러 미호출·삭제 안 함(재전달, poison 가드가 끝내 걷어냄).
- poison(수신 횟수 상한 초과) → 핸들러 미호출·삭제(스킵).
- 삭제 실패는 예외를 올리지 않는다(재전달로 흡수).
- run(): 배치를 처리·ack하고 종료 신호에 우아하게 멈춘다.

``_process``/``_receive``는 무한 소비 루프(run) 없이 계약을 확인하기 위한 단위 테스트 seam이다.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel

from core.config import Settings
from core.sqs.consumer import SqsConsumer

_QUEUE = "https://sqs.ap-northeast-2.amazonaws.com/123456789012/ocr-job-queue"


class _Job(BaseModel):
    job_id: str
    value: int


class FakeSqsClient:
    """receive_message/delete_message만 흉내내는 boto3 SQS 클라이언트 대역(동기)."""

    def __init__(
        self, batches: list[list[dict[str, Any]]] | None = None, stop_event: Any | None = None
    ) -> None:
        self._batches = list(batches or [])
        self._stop_event = stop_event  # 배치 소진 시 set → run() 루프 종료
        self.deleted: list[str] = []  # 삭제된 ReceiptHandle
        self.receive_calls = 0
        self.delete_should_raise = False

    def receive_message(self, **_kwargs: object) -> dict[str, Any]:
        self.receive_calls += 1
        if self._batches:
            return {"Messages": self._batches.pop(0)}
        if self._stop_event is not None:
            self._stop_event.set()
        return {}

    def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> None:
        if self.delete_should_raise:
            raise RuntimeError("delete boom")
        self.deleted.append(ReceiptHandle)


def _message(
    body: str, *, receive_count: int = 1, handle: str = "rh-1", msg_id: str = "m-1"
) -> dict[str, Any]:
    return {
        "MessageId": msg_id,
        "ReceiptHandle": handle,
        "Body": body,
        "Attributes": {"ApproximateReceiveCount": str(receive_count)},
    }


def _consumer(
    handler: Callable[[_Job], Awaitable[None]],
    *,
    settings: Settings | None = None,
    client: FakeSqsClient | None = None,
) -> SqsConsumer[_Job]:
    # 기본 상한을 명시해 receive_count=1이 환경 .env 값에 흔들려 poison으로 오판되지 않게 한다.
    return SqsConsumer(
        _QUEUE,
        _Job,
        handler,
        settings=settings or Settings(sqs_max_receive_count=5),
        client=client or FakeSqsClient(),
    )


async def test_success_deletes_message() -> None:
    # Arrange
    handled: list[_Job] = []

    async def handler(job: _Job) -> None:
        handled.append(job)

    client = FakeSqsClient()
    consumer = _consumer(handler, client=client)

    # Act
    await consumer._process(_message('{"job_id": "j1", "value": 7}'))

    # Assert: 핸들러가 검증된 모델을 받고, 성공했으니 삭제(ack)된다.
    assert [j.value for j in handled] == [7]
    assert client.deleted == ["rh-1"]


async def test_handler_failure_does_not_delete() -> None:
    async def handler(_job: _Job) -> None:
        raise RuntimeError("downstream down")

    client = FakeSqsClient()
    consumer = _consumer(handler, client=client)

    await consumer._process(_message('{"job_id": "j1", "value": 7}'))

    # 삭제하지 않음 → visibility timeout 후 재전달된다(at-least-once).
    assert client.deleted == []


async def test_invalid_body_skips_handler_and_keeps_message() -> None:
    called = False

    async def handler(_job: _Job) -> None:
        nonlocal called
        called = True

    client = FakeSqsClient()
    consumer = _consumer(handler, client=client)

    await consumer._process(_message('{"oops": true}'))  # 필수 필드 누락

    assert called is False
    assert client.deleted == []  # 재전달(스키마 위반은 poison 가드가 끝내 걷어냄)


async def test_poison_message_skipped_after_max_receive_count() -> None:
    called = False

    async def handler(_job: _Job) -> None:
        nonlocal called
        called = True

    client = FakeSqsClient()
    consumer = _consumer(handler, settings=Settings(sqs_max_receive_count=5), client=client)

    # 6번째 수신(상한 5 초과) — 유효 본문이어도 스킵(삭제)하고 핸들러는 안 부른다.
    await consumer._process(_message('{"job_id": "j1", "value": 7}', receive_count=6))

    assert called is False
    assert client.deleted == ["rh-1"]  # 명시적 삭제(스킵)


async def test_poison_boundary_at_threshold_still_processed() -> None:
    # 상한과 같은 횟수(5)는 아직 스킵 아님 — "넘으면"(초과) 스킵이라 경계를 고정한다.
    handled: list[_Job] = []

    async def handler(job: _Job) -> None:
        handled.append(job)

    client = FakeSqsClient()
    consumer = _consumer(handler, settings=Settings(sqs_max_receive_count=5), client=client)

    await consumer._process(_message('{"job_id": "j1", "value": 7}', receive_count=5))

    assert [j.value for j in handled] == [7]
    assert client.deleted == ["rh-1"]


async def test_delete_failure_is_swallowed() -> None:
    async def handler(_job: _Job) -> None:
        return None

    client = FakeSqsClient()
    client.delete_should_raise = True
    consumer = _consumer(handler, client=client)

    # 삭제가 실패해도 예외가 전파되지 않는다(재전달로 흡수, 멱등 재처리).
    await consumer._process(_message('{"job_id": "j1", "value": 7}'))
    assert client.deleted == []


async def test_run_consumes_batch_then_stops() -> None:
    # Arrange: 한 배치를 처리한 뒤, 배치가 소진되면 stopping을 세워 루프를 끝낸다.
    handled: list[_Job] = []

    async def handler(job: _Job) -> None:
        handled.append(job)

    client = FakeSqsClient(batches=[[_message('{"job_id": "j1", "value": 1}')]])
    consumer = _consumer(handler, client=client)
    client._stop_event = consumer._stopping  # 배치 소진 시 종료 신호

    # Act
    await consumer.run()

    # Assert: 배치의 메시지를 처리하고 삭제(ack)한 뒤 종료했다.
    assert [j.value for j in handled] == [1]
    assert client.deleted == ["rh-1"]
