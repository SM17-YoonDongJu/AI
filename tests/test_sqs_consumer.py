"""core.sqs.consumer 수신 규약 테스트.

boto3·실 SQS 없이 페이크 클라이언트를 주입해 컨슈머의 계약만 고정한다(단위 CI 무해):
- 처리 성공 → DeleteMessage(ack) 호출.
- 핸들러 실패 → 삭제하지 않음(visibility timeout 후 재전달에 맡김).
- 핸들러가 ``NonRetryableError`` → 즉시 삭제(ack) + error 로그(결정적 실패 종결).
- 스키마 위반 → 핸들러 미호출·삭제 안 함(재전달, poison 가드가 끝내 걷어냄).
- poison(수신 횟수 상한 초과) → 핸들러 미호출·삭제(스킵).
- poison 훅(``on_poison``) → 걷어내기 **전에** 호출, 성공해야만 삭제(실패 시 보류).
- 삭제 실패는 예외를 올리지 않는다(재전달로 흡수).
- run(): 배치를 처리·ack하고 종료 신호에 우아하게 멈춘다.

``_process``/``_receive``는 무한 소비 루프(run) 없이 계약을 확인하기 위한 단위 테스트 seam이다.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel
from structlog.testing import capture_logs

from core.config import Settings
from core.exceptions import NonRetryableError
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
        # 삭제 시점을 외부 타임라인에 기록하는 훅(poison 훅과의 **순서** 검증용).
        self.on_delete: Callable[[], None] | None = None

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
        if self.on_delete is not None:
            self.on_delete()
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


# ── 결정적 실패(NonRetryableError) → 즉시 ack ────────────────────
class _Terminal(RuntimeError, NonRetryableError):
    """테스트용 결정적 실패 — 도메인 예외 + 마커 믹스인(실제 사용 형태와 동일)."""


async def test_non_retryable_error_deletes_message_immediately() -> None:
    # Arrange: 핸들러가 "재전달해도 같은 결과"라고 단언한다.
    async def handler(_job: _Job) -> None:
        raise _Terminal("마스킹 잔류")

    client = FakeSqsClient()
    consumer = _consumer(handler, client=client)

    # Act
    with capture_logs() as logs:
        await consumer._process(_message('{"job_id": "j1", "value": 7}'))

    # Assert: 재전달로 수신 횟수를 태우지 않고 첫 시도에서 큐를 비운다.
    assert client.deleted == ["rh-1"]
    # 운영 개입 신호(error) — 자동 복구가 없는 확정 실패다.
    terminal = [e for e in logs if e["event"] == "sqs handler terminal failure → ack"]
    assert [e["log_level"] for e in terminal] == ["error"]
    # 예외는 **타입만** 남긴다(§9) — 메시지 문자열엔 무엇이 섞일지 보장할 수 없다.
    assert terminal[0]["error_type"] == "_Terminal"
    assert "마스킹 잔류" not in str(terminal[0])


async def test_plain_error_still_redelivers_after_terminal_branch_added() -> None:
    # NonRetryable 분기가 생겼다고 일반 예외까지 ack되면 일시 장애가 유실로 바뀐다.
    async def handler(_job: _Job) -> None:
        raise RuntimeError("일시적 DB 오류")

    client = FakeSqsClient()
    consumer = _consumer(handler, client=client)

    await consumer._process(_message('{"job_id": "j1", "value": 7}'))

    assert client.deleted == []


# ── poison 훅(걷어내기 전 기록) ──────────────────────────────────
async def _noop_handler(_job: _Job) -> None:
    return None


def _poison_consumer(
    hook: Callable[[_Job | None, str, int], Awaitable[None]],
    client: FakeSqsClient,
    *,
    handler: Callable[[_Job], Awaitable[None]] | None = None,
) -> SqsConsumer[_Job]:
    return SqsConsumer(
        _QUEUE,
        _Job,
        handler or _noop_handler,
        settings=Settings(sqs_max_receive_count=5),
        client=client,
        on_poison=hook,
    )


async def test_poison_hook_runs_before_delete() -> None:
    # Arrange: 순서가 이 기능의 전부다 — 삭제가 먼저면 훅이 실패했을 때 메시지가 이미
    # 사라져 어디에도 흔적이 남지 않는다(무음 유실).
    events: list[str] = []

    async def hook(_job: _Job | None, _message_id: str, _receive_count: int) -> None:
        # 실제 DB 왕복처럼 루프에 양보한다. 이게 없으면 훅을 await하지 않고 task로
        # 흘려보내는 구현(기록 전에 삭제되는 회귀)도 이 테스트를 통과해버린다.
        await asyncio.sleep(0)
        events.append("hook")

    client = FakeSqsClient()
    client.on_delete = lambda: events.append("delete")
    consumer = _poison_consumer(hook, client)

    # Act
    await consumer._process(_message('{"job_id": "j1", "value": 7}', receive_count=6))

    # Assert
    assert events == ["hook", "delete"]
    assert client.deleted == ["rh-1"]


async def test_poison_hook_receives_parsed_model_and_metadata() -> None:
    # 본문은 멀쩡한데 처리가 계속 실패한 poison — job 단위로 기록할 수 있어야 한다.
    seen: list[tuple[_Job | None, str, int]] = []

    async def hook(job: _Job | None, message_id: str, receive_count: int) -> None:
        seen.append((job, message_id, receive_count))

    client = FakeSqsClient()
    consumer = _poison_consumer(hook, client)

    await consumer._process(_message('{"job_id": "j1", "value": 7}', receive_count=6, msg_id="m-9"))

    assert len(seen) == 1
    job, message_id, receive_count = seen[0]
    assert job is not None and job.job_id == "j1" and job.value == 7
    assert (message_id, receive_count) == ("m-9", 6)


async def test_poison_hook_receives_none_for_unparsable_body() -> None:
    # 스키마 자체가 깨진 poison — job_id를 모르니 훅은 message_id로만 기록한다.
    seen: list[tuple[_Job | None, str, int]] = []

    async def hook(job: _Job | None, message_id: str, receive_count: int) -> None:
        seen.append((job, message_id, receive_count))

    client = FakeSqsClient()
    consumer = _poison_consumer(hook, client)

    await consumer._process(_message("{not json", receive_count=6, msg_id="m-8"))

    assert seen == [(None, "m-8", 6)]
    assert client.deleted == ["rh-1"]  # 기록은 남겼으니 걷어낸다


async def test_poison_hook_failure_keeps_message() -> None:
    # Arrange: 기록에 실패하면 삭제를 **보류**한다. "추적 없이 유실"보다 "며칠 더 도는 것"이
    # 낫다(SQS 보존기간이 상한이라 유계다).
    async def hook(_job: _Job | None, _message_id: str, _receive_count: int) -> None:
        raise RuntimeError("저널 DB 다운")

    client = FakeSqsClient()
    consumer = _poison_consumer(hook, client)

    # Act: 훅 예외가 소비 루프로 새지 않는다(§8).
    await consumer._process(_message('{"job_id": "j1", "value": 7}', receive_count=6))

    # Assert
    assert client.deleted == []


async def test_poison_hook_not_called_for_healthy_message() -> None:
    # 정상 처리 경로는 훅과 무관하다 — 매 메시지마다 저널을 두드리면 안 된다.
    calls = 0

    async def hook(_job: _Job | None, _message_id: str, _receive_count: int) -> None:
        nonlocal calls
        calls += 1

    client = FakeSqsClient()
    consumer = _poison_consumer(hook, client)

    await consumer._process(_message('{"job_id": "j1", "value": 7}'))

    assert calls == 0
    assert client.deleted == ["rh-1"]


async def test_poison_hook_not_called_for_terminal_handler_failure() -> None:
    # 결정적 실패는 핸들러가 이미 저널에 남긴다(pipeline.handle) — poison 훅까지 부르면
    # 같은 실패가 두 경로로 기록돼 attempts가 이중 계상된다.
    calls = 0

    async def hook(_job: _Job | None, _message_id: str, _receive_count: int) -> None:
        nonlocal calls
        calls += 1

    async def handler(_job: _Job) -> None:
        raise _Terminal("결정적")

    client = FakeSqsClient()
    consumer = _poison_consumer(hook, client, handler=handler)

    await consumer._process(_message('{"job_id": "j1", "value": 7}'))

    assert calls == 0
    assert client.deleted == ["rh-1"]
