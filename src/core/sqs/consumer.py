"""AWS SQS 컨슈머 래퍼.

큐를 롱폴링해 raw 메시지를 pydantic 모델로 즉시 검증하고 핸들러로 넘긴다. Kafka 컨슈머와
같은 인터페이스(queue_url·schema·handler)를 유지해 워커 배선·핸들러 로직을 그대로 재사용한다.

수신 규약(at-least-once + 핸들러 멱등):
- **처리 성공 후에만 DeleteMessage**(= ack). 실패 시 삭제하지 않으면 visibility timeout이
  지난 뒤 SQS가 자동 재전달한다(핸들러는 멱등이어야 함). 인프로세스 재시도는 두지 않는다 —
  재시도 책임을 브로커(visibility timeout)에 맡긴다(명세 §3).
- 역직렬화/검증 실패도 삭제하지 않는다 — 재전달되며, 아래 poison 가드가 끝내 걷어낸다.
- **결정적 실패는 첫 시도에서 종결**: 핸들러가 ``NonRetryableError``를 던지면 재전달해도
  결과가 같으므로 **즉시 삭제(ack)** 하고 ``error`` 로그를 남긴다. 재전달 낭비를 없애고,
  핸들러가 남긴 실패 기록이 그만큼 빨리 사용자에게 노출된다.
- **poison 가드(DLQ 대체)**: ``ApproximateReceiveCount``가 ``sqs_max_receive_count``를 넘으면
  더는 못 살릴 메시지로 보고 **명시적 삭제(스킵)** 후 운영 로그(``error``)를 남긴다. DLQ를
  아직 안 붙였으므로(명세 §4) 이 자체 방어가 없으면 poison 메시지가 큐 보존기간(기본 4일)
  내내 재전달 루프를 돈다. 향후 소스 큐에 redrive policy(DLQ)만 붙이면 이 코드는 그대로 호환된다.
  ``on_poison`` 훅을 주입하면 걷어내기 **전에** 추적 근거를 남길 기회를 준다(아래 참고).

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
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ValidationError

from core.config import Settings, get_settings
from core.exceptions import NonRetryableError
from core.logging import get_logger
from core.sqs.client import get_sqs_client, queue_name

logger = get_logger(__name__)

# receive_message 실패(네트워크·스로틀) 시 재시도 전 짧은 백오프(초) — 루프가 tight-loop로
# CPU를 태우지 않게 한다. 정상 롱폴링은 서버측 대기(WaitTimeSeconds)라 이 값과 무관하다.
_ERROR_BACKOFF_SECONDS = 1.0


class HandleOutcome(StrEnum):
    """핸들러 1회 호출의 결과 — 컨슈머가 메시지를 어떻게 할지 정하는 3-상태.

    ``DONE``과 ``TERMINAL``은 둘 다 삭제로 이어지지만 **의미가 정반대**라 bool로 합치지
    않는다. 하나는 "처리했다", 다른 하나는 "못 살린다"이고, 로그 레벨(info/error)과
    운영 대응이 갈린다 — 합치면 나중에 둘을 구분해야 할 때(예: DLQ 라우팅, 메트릭)
    호출부에서 다시 유추해야 한다.
    """

    DONE = "done"  # 성공 → 삭제(ack)
    TERMINAL = "terminal"  # 결정적 실패 → 삭제(ack). 재전달해도 결과가 같다.
    RETRY = "retry"  # 일시 실패 → 삭제하지 않음(visibility timeout 후 재전달)


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
        on_poison: Callable[[T | None, str, int], Awaitable[None]] | None = None,
    ) -> None:
        """컨슈머를 구성한다.

        Args:
            queue_url: 소비할 SQS 큐 URL.
            schema: 메시지 Body를 검증할 pydantic 모델.
            handler: 검증된 모델을 처리하는 코루틴(**멱등**이어야 한다).
            settings: 환경설정. ``None``이면 ``get_settings()``.
            client: boto3 SQS 클라이언트(테스트 주입). ``None``이면 ``run()``에서 lazy 생성.
            on_poison: poison 메시지를 걷어내기 **전에** 호출되는 훅. 인자는
                ``(파싱된 모델 또는 None, message_id, receive_count)``. 미주입이면
                기존 동작(로그 후 바로 삭제) 그대로다 — 훅이 필요 없는 소비자
                (report_worker 등)는 영향을 받지 않는다.
        """
        self._queue_url = queue_url
        self._schema = schema
        self._handler = handler
        self._settings = settings or get_settings()
        self._client = client  # None이면 run()에서 lazy 생성(boto3)
        self._on_poison = on_poison
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
            if not await self._run_poison_hook(message):
                return  # 훅 실패 — 삭제하지 않는다(아래 _run_poison_hook 참고)
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
        if await self._handle(model) is HandleOutcome.RETRY:
            return  # 핸들러 실패 — 삭제하지 않아 재전달된다(멱등 재처리)
        await self._delete(message)  # ack — 성공 또는 결정적 실패(종결)

    async def _handle(self, model: T) -> HandleOutcome:
        """핸들러를 1회 호출하고 메시지 처분(``HandleOutcome``)을 정한다.

        ``NonRetryableError``는 "재전달해도 같은 결과"라는 핸들러의 단언이다. 그대로
        재전달하면 수신 횟수 상한까지 같은 실패를 반복하다 poison으로 걷히므로, 여기서
        바로 ack해 큐를 비운다 — 유실이 아니다. 핸들러가 종결 전에 실패 기록을 남기는
        것이 이 분기의 전제이고(``ocr_worker.pipeline.handle``), 기록에 실패하면
        핸들러가 **재시도 가능한 예외로 바꿔 던져** 이 분기를 타지 않게 한다.
        """
        try:
            await self._handler(model)
        except NonRetryableError as exc:
            # 운영 개입 신호(error): 자동 복구가 없는 확정 실패다. 예외는 **타입만**
            # 남긴다 — 메시지 문자열에 무엇이 섞일지 보장할 수 없다(§9).
            logger.error(
                "sqs handler terminal failure → ack",
                queue=queue_name(self._queue_url),
                error_type=type(exc).__name__,
            )
            return HandleOutcome.TERMINAL
        except Exception:  # 최상위 격리: 유실 없이 재전달에 맡긴다(§8)
            logger.warning("sqs handler error → redeliver", queue=queue_name(self._queue_url))
            return HandleOutcome.RETRY
        return HandleOutcome.DONE

    async def _run_poison_hook(self, message: dict[str, Any]) -> bool:
        """걷어내기 직전 ``on_poison``을 호출한다. 삭제해도 되면 ``True``.

        **순서가 핵심이다**: 훅이 성공한 뒤에만 삭제한다. 반대로 하면(삭제 후 훅) 훅이
        실패했을 때 그 메시지는 이미 큐에서 사라져 어디에도 흔적이 남지 않는다 — 사용자
        입장에선 업로드가 조용히 증발하는 무음 실패다. 훅이 실패하면 삭제를 보류해
        다음 재전달에서 다시 시도한다: "추적 없이 유실"보다 "며칠 더 도는 것"이 낫다.
        무한 루프가 아니라 **유계**다 — SQS 큐 보존기간(기본 4일)이 상한이라 그 안에
        훅이 한 번이라도 성공하면 정리되고, 끝내 실패해도 보존기간이 만료시킨다.

        훅 미주입 시엔 아무것도 하지 않고 ``True``(기존 동작 그대로).

        Returns:
            훅이 없거나 성공했으면 ``True``, 훅이 예외를 던졌으면 ``False``(삭제 보류).
        """
        if self._on_poison is None:
            return True
        try:
            await self._on_poison(
                self._try_parse(message),
                str(message.get("MessageId", "")),
                self._receive_count(message),
            )
        except Exception as exc:  # 훅 실패가 소비 루프를 죽이지 않게(§8) — 삭제만 보류한다
            logger.warning(
                "sqs poison hook failed → keep message",
                message_id=message.get("MessageId"),
                error_type=type(exc).__name__,
            )
            return False
        return True

    def _try_parse(self, message: dict[str, Any]) -> T | None:
        """poison 훅에 넘길 모델을 best-effort로 파싱한다(실패하면 ``None``).

        poison에는 두 종류가 섞인다: (a) 본문은 멀쩡한데 처리가 계속 실패한 메시지와
        (b) 스키마 자체가 깨진 메시지. (a)는 모델을 넘겨 job 단위로 기록할 수 있고,
        (b)는 ``None``이라 훅이 message_id로만 기록한다.
        """
        try:
            return self._schema.model_validate_json(message.get("Body", ""))
        except ValueError:  # pydantic ValidationError는 ValueError 하위
            return None

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
