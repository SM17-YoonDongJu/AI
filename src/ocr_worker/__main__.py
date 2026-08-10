"""OCR 워커 진입점 (이슈 #15) — ``python -m ocr_worker``.

얇은 부트스트랩만 담당한다: 로깅 구성 → DB 풀 생성 → 마이그레이션 적용 → SQS
프로듀서 준비 → 파이프라인 배선 → ``ocr-job-queue`` 큐 소비 루프. 처리 로직은
``pipeline.py``, 소비·ack(DeleteMessage)·poison 스킵·우아한 종료는 ``core.sqs.consumer``가 맡는다.

수명 순서(진입 시 자원 확보, 종료 시 역순 정리):
  db_pool → run_migrations → SqsProducer(ReportJob 발행) → 삭제 스윕 task +
  SqsConsumer.run()
``SqsConsumer``는 롱폴링으로 소비하고 SIGTERM/SIGINT에 우아하게 멈춘다(DLQ 미도입 —
실패=삭제 안 함으로 재전달, poison은 수신 횟수 상한으로 스킵).
소비 루프와 **병행해** 원본 삭제 outbox 스윕(``_run_delete_sweep``)을 주기적으로 돈다 —
즉시 삭제가 실패했거나 그 전에 crash가 나 ``pending``으로 남은 원본을 재시도한다.
소비 루프가 멈춘 뒤에는 스윕 task를 정리하고, 파이프라인이 백그라운드로 돌리는 S3 원본
삭제 task를 짧게 흡수한다 — 자세한 근거는 ``_SHUTDOWN_DELETE_TIMEOUT_S`` 참고.
로컬 PG·RDS에 같은 ``migrations/*.sql``을 적용해 스키마 드리프트를 차단한다(#19).
"""

import asyncio
import contextlib

from core.config import Settings, get_settings
from core.contracts import OcrJob
from core.db import db_pool, run_migrations
from core.logging import configure_logging, get_logger
from core.sqs.consumer import SqsConsumer
from core.sqs.producer import SqsProducer
from ocr_worker.pipeline import OcrPipeline

logger = get_logger(__name__)

# 마이그레이션 SQL 디렉터리(리포지토리 루트 기준 상대). 진입 시 멱등 DDL을 적용한다.
_MIGRATIONS_DIR = "migrations"

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
