"""AWS SQS 컨슈머 래퍼.

큐를 롱폴링해 raw 메시지를 pydantic 모델로 즉시 검증하고 핸들러로 넘긴다. Kafka 컨슈머와
같은 인터페이스(queue_url·schema·handler)를 유지해 워커 배선·핸들러 로직을 그대로 재사용한다.

수신 규약(at-least-once + 핸들러 멱등):
- **처리 성공 후에만 DeleteMessage**(= ack). 실패 시 삭제하지 않으면 visibility timeout이
  지난 뒤 SQS가 자동 재전달한다(핸들러는 멱등이어야 함). 인프로세스 재시도는 두지 않는다 —
  재시도 책임을 브로커(visibility timeout)에 맡긴다(명세 §3).
- 역직렬화/검증 실패도 삭제하지 않는다 — 재전달되며, 아래 poison 가드가 끝내 걷어낸다.
- **poison 가드(DLQ 대체)**: ``ApproximateReceiveCount``가 ``sqs_max_receive_count``를 넘으면
  더는 못 살릴 메시지로 보고 **명시적 삭제(스킵)** 후 운영 로그(``error``)를 남긴다. DLQ를
  아직 안 붙였으므로(명세 §4) 이 자체 방어가 없으면 poison 메시지가 큐 보존기간(기본 4일)
  내내 재전달 루프를 돈다. 향후 소스 큐에 redrive policy(DLQ)만 붙이면 이 코드는 그대로 호환된다.

우아한 종료: SIGTERM/SIGINT에 멈춘다. 종료 신호는 진행 중인 롱폴링 1주기
(``sqs_wait_time_seconds``)까지는 대기할 수 있으나, 그 사이 강제 종료(SIGKILL)돼도 삭제 전
메시지는 재전달·멱등 재처리되므로 정합성은 깨지지 않는다(효율만 손해).

전송 계층(자격증명·리전·LocalStack 엔드포인트)은 ``core.sqs.client``가 가둔다 — 정적 키 없이
표준 AWS 자격증명 체인(워커 IAM Role)을 쓴다(§13). boto3는 동기 SDK라 수신·삭제 호출을
``asyncio.to_thread``로 이벤트 루프에서 뗀다(§7).
"""

import asyncio
import signal
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel, ValidationError

from core.config import Settings, get_settings
from core.logging import get_logger
from core.sqs.client import get_sqs_client, queue_name

logger = get_logger(__name__)

# receive_message 실패(네트워크·스로틀) 시 재시도 전 짧은 백오프(초) — 루프가 tight-loop로
# CPU를 태우지 않게 한다. 정상 롱폴링은 서버측 대기(WaitTimeSeconds)라 이 값과 무관하다.
_ERROR_BACKOFF_SECONDS = 1.0


class SqsConsumer[T: BaseModel]:
    """SQS 큐를 롱폴링해 검증 후 핸들러로 넘기는 컨슈머.

    핸들러는 **멱등**해야 한다(at-least-once 재전달 안전). 처리 성공 시에만 메시지를
    삭제하며, 일시 실패는 재전달로 흡수하고 못 살리는 poison은 수신 횟수 상한으로 걷어낸다.
    """

    def __init__(
        self,
        queue_url: str,
        schema: type[T],
        handler: Callable[[T], Awaitable[None]],
        *,
        settings: Settings | None = None,
        client: Any | None = None,
    ) -> None:
        self._queue_url = queue_url
        self._schema = schema
        self._handler = handler
        self._settings = settings or get_settings()
        self._client = client  # None이면 run()에서 lazy 생성(boto3)
        self._stopping = asyncio.Event()

    async def run(self) -> None:
        """컨슈머를 시작해 종료 신호까지 롱폴링 루프를 돈다."""
        self._sqs()  # boto3 클라이언트를 시작 시점에 lazy 생성(배선·로그를 앞당긴다)
        self._install_signal_handlers()
        logger.info("sqs consumer started", queue=queue_name(self._queue_url))
        try:
            await self._consume_loop()
        finally:
            logger.info("sqs consumer stopped", queue=queue_name(self._queue_url))

    def _sqs(self) -> Any:
        """캐시된 boto3 SQS 클라이언트를 반환한다(첫 접근 시 lazy 생성).

        주입된 클라이언트(테스트)면 그대로 쓰고, 없으면 ``core.sqs.client``에서 만든다.
        반환 타입이 ``Any``라 호출측 attribute 접근이 None 유니온에 걸리지 않는다.
        """
        if self._client is None:
            self._client = get_sqs_client(self._settings)
        return self._client

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._stopping.set)
            except NotImplementedError:
                # Windows 등 미지원 플랫폼 — KeyboardInterrupt로 종료된다.
                logger.debug("signal handler unsupported", signal=sig)

    async def _consume_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                messages = await self._receive()
            except Exception:  # 최상위 격리: 일시적 수신 오류로 상시 데몬이 죽지 않게(§8)
                logger.warning("sqs receive failed → retry")
                await self._sleep_or_stop(_ERROR_BACKOFF_SECONDS)
                continue
            for message in messages:
                if self._stopping.is_set():
                    break  # 종료 신호 — 남은 메시지는 삭제하지 않아 재전달된다(멱등 안전)
                try:
                    await self._process(message)
                except Exception:  # 개별 메시지 격리: 예기치 못한 오류로 소비 루프가 죽지 않게(§8)
                    # 삭제하지 않으므로 재전달되고, 계속 실패하면 poison 가드가 걷어낸다.
                    logger.warning(
                        "sqs process error → redeliver", message_id=message.get("MessageId")
                    )

    async def _receive(self) -> list[dict[str, Any]]:
        """롱폴링으로 메시지를 최대 ``sqs_max_messages``건 받는다(블로킹 SDK를 스레드 격리).

        ``ApproximateReceiveCount``를 함께 받아 poison 가드가 재전달 횟수를 판정한다.
        """
        settings = self._settings
        response = await asyncio.to_thread(
            self._sqs().receive_message,
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=settings.sqs_max_messages,
            WaitTimeSeconds=settings.sqs_wait_time_seconds,
            VisibilityTimeout=settings.sqs_visibility_timeout,
            AttributeNames=["ApproximateReceiveCount"],
        )
        messages: list[dict[str, Any]] = response.get("Messages", [])
        return messages

    async def _process(self, message: dict[str, Any]) -> None:
        """메시지 1건을 검증·처리하고 성공 시 삭제한다(poison은 상한 초과 시 스킵)."""
        if self._is_poison(message):
            # 못 살리는 메시지(재전달 상한 초과) — 명시적 삭제로 4일짜리 재전달 루프를 끊는다.
            # 유효했지만 다운스트림 장애로 계속 실패한 메시지도 여기서 유실될 수 있다(명세 §4가
            # 감수하는 트레이드오프) — 그래서 error 레벨로 운영 개입 신호를 남긴다.
            logger.error(
                "sqs message exceeded max receive count → skip",
                message_id=message.get("MessageId"),
                receive_count=self._receive_count(message),
                max_receive_count=self._settings.sqs_max_receive_count,
            )
            await self._delete(message)
            return
        try:
            model = self._schema.model_validate_json(message["Body"])
        except ValidationError as exc:
            # 스키마 위반 = 재전달해도 통과 못 함(poison). 삭제하지 않아 재전달되고, 상한을
            # 넘으면 위 poison 가드가 걷어낸다. 원문 값은 남기지 않는다(§9) — 오류 건수만.
            logger.warning(
                "sqs invalid message → redeliver",
                message_id=message.get("MessageId"),
                errors=exc.error_count(),
            )
            return
        if not await self._handle(model):
            return  # 핸들러 실패 — 삭제하지 않아 재전달된다(멱등 재처리)
        await self._delete(message)  # ack

    async def _handle(self, model: T) -> bool:
        """핸들러를 1회 호출한다. 성공하면 ``True``(→ 삭제/ack), 실패면 ``False``(→ 재전달)."""
        try:
            await self._handler(model)
        except Exception:  # 최상위 격리: 유실 없이 재전달에 맡긴다(§8)
            logger.warning("sqs handler error → redeliver", queue=queue_name(self._queue_url))
            return False
        return True

    async def _delete(self, message: dict[str, Any]) -> None:
        """메시지를 삭제한다(= ack). 삭제 실패는 재전달로 흡수되므로 예외를 올리지 않는다."""
        try:
            await asyncio.to_thread(
                self._sqs().delete_message,
                QueueUrl=self._queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )
        except Exception:  # 삭제 실패 → 메시지가 재전달되고 멱등 핸들러가 다시 처리한다(무해)
            logger.warning(
                "sqs delete failed → will redeliver", message_id=message.get("MessageId")
            )

    def _is_poison(self, message: dict[str, Any]) -> bool:
        return self._receive_count(message) > self._settings.sqs_max_receive_count

    @staticmethod
    def _receive_count(message: dict[str, Any]) -> int:
        """``ApproximateReceiveCount``(문자열)를 정수로 읽는다. 없으면 첫 수신(1)으로 본다."""
        return int(message.get("Attributes", {}).get("ApproximateReceiveCount", "1"))

    async def _sleep_or_stop(self, seconds: float) -> None:
        """종료 신호가 오면 즉시 깨어나는 취소 가능 대기(수신 오류 백오프용)."""
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
        except TimeoutError:
            return
