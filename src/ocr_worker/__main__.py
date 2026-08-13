"""OCR 워커 진입점 (이슈 #15) — ``python -m ocr_worker``.

얇은 부트스트랩만 담당한다: 로깅 구성 → DB 풀 생성 → 마이그레이션 적용 → SQS
프로듀서 준비 → 파이프라인 배선 → ``ocr-job-queue`` 큐 소비 루프. 처리 로직은
``pipeline.py``, 소비·ack(DeleteMessage)·poison 스킵·우아한 종료는 ``core.sqs.consumer``가 맡는다.

수명 순서(진입 시 자원 확보, 종료 시 역순 정리):
  db_pool → run_migrations → SqsProducer(ReportJob 발행) → 삭제 스윕 task +
  SqsConsumer.run()
``SqsConsumer``는 롱폴링으로 소비하고 SIGTERM/SIGINT에 우아하게 멈춘다(DLQ 미도입 —
실패=삭제 안 함으로 재전달, poison은 수신 횟수 상한으로 스킵). 스킵 직전에는
``_poison_journal`` 훅이 ``ai.ocr_job_failures``에 확정 실패를 남기고, 그 문서를 청구
종결 카운트에도 반영한다 — 걷어내기가 "조용한 유실"이 되지 않게 하는 마지막 기록
지점이자, 걷힌 문서 때문에 청구 fan-in이 영영 멈추지 않게 하는 지점이다.
소비 루프와 **병행해** 원본 삭제 outbox 스윕(``_run_delete_sweep``)을 주기적으로 돈다 —
즉시 삭제가 실패했거나 그 전에 crash가 나 ``pending``으로 남은 원본을 재시도한다.
소비 루프가 멈춘 뒤에는 스윕 task를 정리하고, 파이프라인이 백그라운드로 돌리는 S3 원본
삭제 task를 짧게 흡수한다 — 자세한 근거는 ``_SHUTDOWN_DELETE_TIMEOUT_S`` 참고.
로컬 PG·RDS에 같은 ``migrations/*.sql``을 적용해 스키마 드리프트를 차단한다(#19).
"""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable

import asyncpg

from core.config import Settings, get_settings
from core.contracts import OcrJob
from core.db import db_pool, run_migrations
from core.logging import configure_logging, get_logger
from core.sqs.consumer import SqsConsumer
from core.sqs.producer import SqsProducer
from ocr_worker.pipeline import OcrPipeline
from ocr_worker.repository import mark_failure_terminal

logger = get_logger(__name__)

# 마이그레이션 SQL 디렉터리(리포지토리 루트 기준 상대). 진입 시 멱등 DDL을 적용한다.
# ai_owner 전용 서브디렉터리만 가리킨다 — migrations/ 전체를 돌리면 corpus_owner 소유
# 오브젝트(policy_chunks 등)를 건드리다 권한 에러로 기동이 막힌다(실측, #48~#50).
_MIGRATIONS_DIR = "migrations/ai"

# 종료 시 미완료 S3 원본 삭제 task를 기다리는 상한(초). ``OcrPipeline``은 원본 삭제를
# ReportJob 발행과 떼어 백그라운드 task로 돌리므로, 종료 신호가 오면 몇 건이 떠 있을 수
# 있다. 그냥 두면 루프가 닫히며 task가 취소돼 "pending task destroyed" 잡음만 남으니 잠깐
# 흡수한다. 짧게 잡는 이유: 이 대기는 **최선 노력**일 뿐 정합성 장치가 아니다 — 못 끝낸
# 삭제는 원본이 S3에 남을 뿐이고, 그건 검증 실패·crash 시에도 이미 일어나는 일이라
# S3 라이프사이클 정책이 백스톱으로 정리한다(pipeline 모듈 docstring §원본 삭제 게이트).
# SIGTERM→SIGKILL 유예(k8s 기본 30초) 안에 넉넉히 들어가는 값이어야 한다.
_SHUTDOWN_DELETE_TIMEOUT_S = 5.0


async def _run() -> None:
    """자원을 배선하고 소비 루프를 돈다(종료 신호까지)."""
    settings = get_settings()
    async with db_pool(settings) as pool:
        applied = await run_migrations(pool, _MIGRATIONS_DIR)
        logger.info("migrations applied", files=applied)
        # SQS 프로듀서는 유지할 연결 수명이 없어(요청마다 서명되는 boto3 호출) 컨텍스트
        # 매니저가 아니다 — 그냥 만들어 파이프라인에 넘긴다.
        producer = SqsProducer(settings)
        pipeline = OcrPipeline(pool=pool, producer=producer, settings=settings)
        consumer: SqsConsumer[OcrJob] = SqsConsumer(
            queue_url=settings.sqs_ocr_job_queue_url,
            schema=OcrJob,
            handler=pipeline.handle,
            settings=settings,
            on_poison=_poison_journal(pool, pipeline),
        )
        logger.info("ocr worker starting", queue_url=settings.sqs_ocr_job_queue_url)
        # 소비와 병행해 도는 outbox 스윕. 소비 루프와 독립적이라 gather로 묶지 않고
        # 별도 task로 띄운다 — 스윕이 죽어도 소비는 계속돼야 하고, 반대도 마찬가지다
        # (스윕 정리는 아래 finally가 맡는다).
        stopping = asyncio.Event()
        sweep_task = asyncio.create_task(_run_delete_sweep(pipeline, settings, stopping))
        try:
            await consumer.run()
        finally:
            # 스윕을 먼저 멈춘다: 신규 사이클 진입을 막고(stopping), 대기 중이면 즉시
            # 깨우며, S3 왕복 중이면 취소한다. 스윕은 멱등이라 중간에 잘려도 남은
            # pending 행을 다음 기동이 그대로 이어받는다.
            stopping.set()
            sweep_task.cancel()
            try:
                await sweep_task
            except asyncio.CancelledError:
                pass  # 정상 취소 경로 — 사이클 중간이어도 다음 기동이 이어받는다
            except Exception:  # 루프가 이미 죽어 있던 경우: 종료 흐름을 막지 않는다
                logger.exception("original_delete_sweep_task_error")
            # 소비 루프 종료 후 백그라운드 원본 삭제를 짧게 흡수한다(최선 노력).
            # 타임아웃이 나도 **이 함수 범위 안에서는** 진행 중인 삭제가 취소되지
            # 않는다(내부가 gather가 아니라 asyncio.wait). 다만 그 뒤 ``asyncio.run``의
            # teardown(``Runner.close`` → ``_cancel_all_tasks``)이 결국 남은 task를
            # 취소하므로, 이 대기는 "끝날 수 있는 삭제를 끝내주는" 장치일 뿐이다.
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(_SHUTDOWN_DELETE_TIMEOUT_S):
                    await pipeline.wait_for_pending_deletes()


def _poison_journal(
    pool: asyncpg.Pool, pipeline: OcrPipeline
) -> Callable[[OcrJob | None, str, int], Awaitable[None]]:
    """poison 메시지를 걷어내기 직전 실패 저널에 확정 기록하는 훅을 만든다.

    컨슈머의 poison 가드는 수신 횟수 상한을 넘긴 메시지를 **삭제**한다. 그게 마지막
    기회라, 여기서 기록하지 않으면 사용자는 원인 조회조차 불가능한 무음 실패를 겪는다.

    **저널 기록의 예외는 삼키지 않는다** — 컨슈머가 훅의 성공 여부로 삭제/보류를
    정하기 때문이다(``_run_poison_hook``). 여기서 잡아 로그만 남기면 컨슈머는 성공으로
    오인해 메시지를 지우고, 정확히 이 함수가 막으려던 무음 실패가 다시 생긴다.

    기록에 성공하면 이 문서는 **확정 종결**이므로 청구 진행에도 반영한다
    (``advance_claim_progress``) — 재시도를 소진해 걷힌 문서를 안 세면 그 청구의
    ``docs_terminal``이 영영 ``doc_total``에 못 미쳐 리포트가 나오지 않는다.
    이미 세어진 문서를 다시 알려도 안전하다: 종결 카운트는 ``job_id`` 집합이라
    **문서별 멱등**이고(마이그레이션 010), 중복 보고로 수가 늘지 않는다 — 여기에 별도
    "이미 카운트됐나" 가드를 두지 않는 이유다(가드는 조회~증가 사이 경합에 다시
    노출되지만, 집합 방식은 단일 원자적 업서트 안에서 끝난다).

    다만 이 반영의 실패는 **삼킨다**(경고만): 저널이라는 본래 목적은 이미 달성됐고,
    여기서 예외를 올리면 컨슈머가 메시지를 못 지워 poison 가드가 끊으려던 재전달
    루프(큐 보존기간 내내)가 되살아난다. 그쪽 실패 모드가 더 나쁘다. 반영이 끝내
    안 되면 그 청구는 ``pending``에 남는다(아래 알려진 한계와 같은 결과).

    **알려진 한계**: ``job``이 ``None``이면(본문 역직렬화 자체가 실패) ``claim_id``를
    알 수 없어 진행 반영이 불가능하다. 그 청구는 ``docs_terminal``이 ``doc_total``에
    못 미친 채 ``pending``에 머물러 리포트가 나오지 않는다 — 운영 조회(``message_id``로
    남은 ``ai.ocr_job_failures`` 행)와 수동 개입이 유일한 복구 수단이다. MVP 범위 밖으로 둔다.

    Args:
        pool: asyncpg 연결 풀(워커 수명과 같다 — 클로저로 잡아 둔다).
        pipeline: 청구 진행 반영을 위임할 파이프라인(같은 풀을 공유한다).

    Returns:
        ``SqsConsumer(on_poison=...)``에 넘길 훅.
    """

    async def on_poison(job: OcrJob | None, message_id: str, receive_count: int) -> None:
        await mark_failure_terminal(
            pool, job=job, message_id=message_id, receive_count=receive_count
        )
        if job is None:
            return  # claim_id를 모른다 — 위 docstring의 알려진 한계
        try:
            await pipeline.advance_claim_progress(job)
        except Exception as exc:  # 저널은 이미 남았다 — 재전달 루프를 되살리지 않는다(§8)
            logger.warning(
                "claim_progress_advance_failed",
                message_id=message_id,
                claim_id=job.claim_id,
                error_type=type(exc).__name__,
            )

    return on_poison


async def _run_delete_sweep(
    pipeline: OcrPipeline, settings: Settings, stopping: asyncio.Event
) -> None:
    """원본 삭제 outbox를 주기적으로 스윕한다(``corpus_worker``의 폴링 루프 패턴).

    즉시 삭제(fire-and-forget) 실패나 crash로 ``pending``에 남은 원본을 재시도해,
    "저장은 됐는데 원본이 조용히 남는" 경로를 닫는다. 한 사이클의 예외는 잡아 로그만
    남기고 다음 사이클로 넘어간다 — 일시적 DB·S3 오류로 상시 데몬이 죽으면 재시도
    경로 자체가 사라진다(§8 top-loop 격리).
    """
    while not stopping.is_set():
        try:
            await pipeline.sweep_pending_deletes()
        except Exception:  # 최상위 격리: 한 사이클 실패가 스윕을 멈추지 않게(§8)
            logger.exception("original_delete_sweep_error")
        await _sleep_or_stop(stopping, settings.ocr_delete_retry_interval_seconds)


async def _sleep_or_stop(stopping: asyncio.Event, seconds: float) -> None:
    """종료 신호가 오면 즉시 깨어나는 취소 가능 대기(스윕 사이클 간격용)."""
    try:
        await asyncio.wait_for(stopping.wait(), timeout=seconds)
    except TimeoutError:
        return  # 정상 간격 경과 — 다음 사이클로


def main() -> None:
    """프로세스 진입점. 로깅을 구성하고 이벤트 루프를 돌린다."""
    configure_logging()
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:  # 시그널 핸들러 미지원 플랫폼(Windows 등)의 종료 경로
        logger.info("ocr worker interrupted")


if __name__ == "__main__":
    main()
