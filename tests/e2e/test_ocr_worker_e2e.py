"""OCR 워커 end-to-end 통합 테스트 (이슈 #20).

실 Kafka·PostgreSQL 위에서 워커의 전체 소비 경로를 구동한다 — ``KafkaConsumer`` →
``OcrPipeline.handle`` → ``ocr_results`` 업서트 → ``ReportJob`` 발행. GPU(surya OCR)와
S3만 페이크로 주입하고(그 경계는 이미 단위 테스트가 덮는다), 나머지 인프라 경계
(컨슈머 검증·수동커밋·DLQ·프로듀서·마이그레이션·업서트 멱등)는 전부 진짜다.

검증 시나리오(계획 §검증 3·4):
1. 정상: ``ocr-job-queue``에 ``OcrJob`` 주입 → consume→OCR→마스킹→저장→``report-job``
   발행까지 이어지고, 발행 페이로드가 계약(``ReportJob``)과 일치하는가.
2. 멱등: 같은 ``job_id`` 재투입 시 ``ocr_results`` 행은 하나만 유지되고, 결정적
   ``report_id``로 ``ReportJob``만 재발행되는가.
3. DLQ: 깨진 페이로드가 ``ocr-job-queue.dlq``로 격리되고(원본 유실 없음) 저장·발행이
   일어나지 않는가.

느린 통합 테스트라 인프라(kafka+PG)가 없으면 conftest의 ``infra`` 게이트가 skip한다.
"""

import asyncio
import contextlib
import json
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import asyncpg
import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition

from core.config import Settings
from core.contracts import DocType, OcrJob, ReportJob
from core.kafka.consumer import KafkaConsumer
from core.kafka.producer import KafkaProducer
from ocr_worker.ocr import OcrLine, OcrPage, OcrResult
from ocr_worker.pipeline import OcrPipeline, _derive_report_id

# 그룹 조인·오프셋 리셋 여유를 둔 상한(실 브로커 왕복 포함).
_WAIT_TIMEOUT_S = 30.0
_POLL_INTERVAL_S = 0.5


# ── 페이크 경계(GPU·S3만) ────────────────────────────────────────
class FakeProcessor:
    """OCR 프로세서 대역 — 고정 OcrResult와 더미 페이지 이미지를 돌려준다(GPU 불필요)."""

    def __init__(self, result: OcrResult) -> None:
        self._result = result
        self.calls = 0

    async def process_with_images(
        self, s3_key: str, content_type: str
    ) -> tuple[OcrResult, list[object]]:
        self.calls += 1
        return self._result, [object()]  # 이미지 내용은 페이크 이미지 트랙이 무시


async def _fake_image_pipeline(job: OcrJob, result: OcrResult, images: list[object]) -> list[str]:
    """S3 업로드 없이 페이지별 마스킹 이미지 키만 돌려주는 이미지 트랙 대역."""
    return [f"masked/{job.job_id}/page-0.png"]


# ── 헬퍼 ─────────────────────────────────────────────────────────
def _ocr_result(*texts: str) -> OcrResult:
    """주어진 라인들로 단일 페이지 OcrResult를 만든다(bbox·polygon·conf 채움)."""
    lines = tuple(
        OcrLine(
            text=text,
            bbox=(0.0, 0.0, 10.0, 5.0),
            polygon=((0.0, 0.0), (10.0, 0.0), (10.0, 5.0), (0.0, 5.0)),
            confidence=0.98,
        )
        for text in texts
    )
    return OcrResult(pages=(OcrPage(index=0, width=100, height=50, lines=lines),))


def _job(job_id: str, **overrides: Any) -> OcrJob:
    base: dict[str, Any] = {
        "job_id": job_id,
        "s3_key": "uploads/sample.pdf",
        "content_type": "application/pdf",
        "user_ref": "user-e2e",
        "doc_type_hint": None,
        "claim_id": None,
        "report_id": "bbbbbbbb-0000-0000-0000-000000000001",
        "attachment_id": "bbbbbbbb-0000-0000-0000-000000000002",
        "doc_index": 1,
        "doc_total": 1,
        "uploaded_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    base.update(overrides)
    return OcrJob(**base)


async def _produce(settings: Settings, topic: str, value: bytes, key: bytes) -> None:
    """실 프로듀서로 raw 바이트 한 건을 토픽에 발행한다(토픽 자동생성)."""
    async with KafkaProducer(settings) as producer:
        await producer.publish_raw(topic, value, key)


async def _drain(
    settings: Settings,
    topic: str,
    *,
    expected: int = 1,
    timeout: float = 8.0,  # noqa: ASYNC109
) -> list[bytes]:
    """토픽 파티션 0을 earliest부터 훑어 쌓인 메시지 값을 읽는다.

    **수동 파티션 할당**(그룹 없음)을 써 컨슈머 그룹 조인·리밸런스 지연을 없앤다 —
    그룹 구독은 첫 할당까지 수 초가 걸려 짧은 폴에서 빈 결과로 조기 종료하기 쉽다.
    커밋하지 않으므로 워커 오프셋에 영향이 없다. 테스트 토픽은 자동생성 기본값대로
    단일 파티션이라 파티션 0만 본다. ``expected``개를 모으면 조기 반환, 아니면 타임아웃까지.
    """
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        security_protocol=settings.kafka_security_protocol,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    collected: list[bytes] = []
    try:
        consumer.assign([TopicPartition(topic, 0)])  # 그룹 조인 없이 즉시 할당
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            batches = await consumer.getmany(timeout_ms=500)
            for records in batches.values():
                collected.extend(r.value for r in records)
            if len(collected) >= expected:
                break
    finally:
        await consumer.stop()
    return collected


async def _run_worker_until(
    settings: Settings,
    pipeline: OcrPipeline,
    predicate: Callable[[], Awaitable[bool]],
    *,
    timeout: float = _WAIT_TIMEOUT_S,  # noqa: ASYNC109  # 폴링 예산(취소 타임아웃 아님)
) -> None:
    """실 ``KafkaConsumer``를 백그라운드로 돌리며 ``predicate``가 참이 될 때까지 기다린다.

    조건 충족(또는 타임아웃) 시 컨슈머 태스크를 취소하고 정리한다. 취소는
    ``getmany`` 대기 중에 들어가 ``run()``의 ``finally``가 컨슈머를 닫는다.
    """
    consumer: KafkaConsumer[OcrJob] = KafkaConsumer(
        topic=settings.kafka_ocr_job_topic,
        schema=OcrJob,
        handler=pipeline.handle,
        settings=settings,
    )
    task = asyncio.create_task(consumer.run())
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    try:
        while loop.time() < deadline:
            if task.done():  # 컨슈머가 예기치 않게 죽으면 예외를 드러낸다
                task.result()
                break
            if await predicate():
                return
            await asyncio.sleep(_POLL_INTERVAL_S)
        pytest.fail(f"조건 미충족(타임아웃 {timeout}s)")
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def _pipeline(
    settings: Settings, pool: asyncpg.Pool, producer: KafkaProducer, processor: FakeProcessor
) -> OcrPipeline:
    """실 pool·producer에 페이크 OCR/이미지 트랙을 배선한 파이프라인을 만든다.

    ``settings``를 반드시 넘긴다 — 안 넘기면 ``get_settings()`` 기본값(기본 토픽명)으로
    발행해 테스트의 고유 토픽과 어긋난다.
    """
    return OcrPipeline(
        pool=pool,
        producer=producer,
        settings=settings,
        processor=processor,  # type: ignore[arg-type]
        image_pipeline=_fake_image_pipeline,
    )


async def _count_rows(pool: asyncpg.Pool, job_id: str) -> int:
    """해당 job_id의 ocr_results 행 수(멱등 검증용)."""
    row = await pool.fetchrow(
        "SELECT count(*) AS n FROM ocr_results WHERE job_id = $1", uuid.UUID(job_id)
    )
    return int(row["n"]) if row is not None else 0


# ── 1. 정상 흐름 ─────────────────────────────────────────────────
async def test_e2e_consume_ocr_persist_publish(
    e2e_settings: Settings, e2e_pool: asyncpg.Pool
) -> None:
    # Arrange: 보험증권 + 주민번호가 든 OCR 결과. 실 마스커(정규식)가 PII를 가린다.
    job_id = str(uuid.uuid4())
    processor = FakeProcessor(
        _ocr_result("보험증권", "증권번호 202301-042", "홍길동 901010-1234567")
    )
    await _produce(
        e2e_settings,
        e2e_settings.kafka_ocr_job_topic,
        _job(job_id, claim_id="claim-e2e").model_dump_json().encode(),
        job_id.encode(),
    )

    # Act: ReportJob이 발행될 때까지(=핸들러 저장·발행 완료) 워커를 돌린다.
    async def report_published() -> bool:
        drained = await _drain(e2e_settings, e2e_settings.kafka_report_job_topic, expected=1)
        return len(drained) >= 1

    async with KafkaProducer(e2e_settings) as producer:
        pipeline = _pipeline(e2e_settings, e2e_pool, producer, processor)
        await _run_worker_until(e2e_settings, pipeline, report_published)

    # Assert(DB): 정확히 한 행, 원문 주민번호는 없고 마스킹 토큰이 있다. 이미지 키 적재.
    row = await e2e_pool.fetchrow(
        "SELECT id, doc_type, masked_text, masked_image_s3_keys, ocr_confidence "
        "FROM ocr_results WHERE job_id = $1",
        uuid.UUID(job_id),
    )
    assert row is not None
    assert row["doc_type"] == "policy"
    assert "901010-1234567" not in row["masked_text"]
    assert "[주민등록번호]" in row["masked_text"]
    assert json.loads(row["masked_image_s3_keys"]) == [f"masked/{job_id}/page-0.png"]
    assert row["ocr_confidence"] == pytest.approx(0.98, abs=1e-3)
    ocr_result_id = str(row["id"])

    # Assert(Kafka): report-job에 계약대로 ReportJob 1건.
    messages = await _drain(e2e_settings, e2e_settings.kafka_report_job_topic)
    assert len(messages) == 1
    report = ReportJob.model_validate_json(messages[0])
    assert report.ocr_result_id == ocr_result_id
    assert report.report_id == _derive_report_id(ocr_result_id)
    assert report.job_id == job_id
    assert report.doc_type is DocType.POLICY
    assert report.user_ref == "user-e2e"
    assert report.claim_id == "claim-e2e"  # 패스스루


# ── 2. 멱등 재투입 ───────────────────────────────────────────────
async def test_e2e_idempotent_redelivery(e2e_settings: Settings, e2e_pool: asyncpg.Pool) -> None:
    # Arrange: 같은 job_id 메시지 2건(at-least-once 재전달 모사).
    job_id = str(uuid.uuid4())
    processor = FakeProcessor(_ocr_result("보험증권", "증권번호 202301-777"))
    payload = _job(job_id).model_dump_json().encode()
    for _ in range(2):
        await _produce(e2e_settings, e2e_settings.kafka_ocr_job_topic, payload, job_id.encode())

    # Act: report-job에 2건(최초 발행 + 멱등 재발행)이 보일 때까지 돌린다.
    async def two_reports() -> bool:
        drained = await _drain(e2e_settings, e2e_settings.kafka_report_job_topic, expected=2)
        return len(drained) >= 2

    async with KafkaProducer(e2e_settings) as producer:
        pipeline = _pipeline(e2e_settings, e2e_pool, producer, processor)
        await _run_worker_until(e2e_settings, pipeline, two_reports)

    # Assert: ocr_results 행은 정확히 하나(job_id 업서트 멱등).
    assert await _count_rows(e2e_pool, job_id) == 1
    # 무거운 OCR은 최초 1회만 — 재투입은 진입부 멱등 단락으로 건너뛴다.
    assert processor.calls == 1

    # 두 ReportJob 모두 같은(결정적) report_id·ocr_result_id.
    messages = await _drain(e2e_settings, e2e_settings.kafka_report_job_topic, expected=2)
    reports = [ReportJob.model_validate_json(m) for m in messages]
    assert len(reports) >= 2
    assert len({r.report_id for r in reports}) == 1
    assert len({r.ocr_result_id for r in reports}) == 1


# ── 3. DLQ 격리(깨진 페이로드) ───────────────────────────────────
async def test_e2e_malformed_payload_routed_to_dlq(
    e2e_settings: Settings, e2e_pool: asyncpg.Pool
) -> None:
    # Arrange: 스키마를 만족하지 못하는 JSON(필수 필드 누락).
    bad_value = json.dumps({"job_id": "not-a-real-job", "oops": True}).encode()
    dlq_topic = f"{e2e_settings.kafka_ocr_job_topic}{e2e_settings.kafka_dlq_suffix}"
    processor = FakeProcessor(_ocr_result("무시됨"))
    await _produce(e2e_settings, e2e_settings.kafka_ocr_job_topic, bad_value, b"k")

    # Act: DLQ에 메시지가 도착할 때까지 돌린다.
    async def dlq_has_message() -> bool:
        return len(await _drain(e2e_settings, dlq_topic)) >= 1

    async with KafkaProducer(e2e_settings) as producer:
        pipeline = _pipeline(e2e_settings, e2e_pool, producer, processor)
        await _run_worker_until(e2e_settings, pipeline, dlq_has_message)

    # Assert: 원본 바이트 그대로 DLQ에 실린다(유실 없음).
    dlq_messages = await _drain(e2e_settings, dlq_topic)
    assert bad_value in dlq_messages
    # 저장·발행·OCR은 일어나지 않는다(검증 단계에서 격리).
    assert processor.calls == 0
    # report-job엔 아무것도 안 실린다(없음을 확인하려 짧게만 기다린다).
    reports = await _drain(e2e_settings, e2e_settings.kafka_report_job_topic, timeout=3.0)
    assert reports == []
