"""OCR 파이프라인 오케스트레이션 테스트 (이슈 #15/#20).

Kafka·DB·GPU·PIL 없이 경계(프로듀서·풀·OCR 프로세서·이미지 트랙)를 페이크로 주입해
``OcrPipeline.handle``의 흐름을 검증한다:
- 정상 흐름: OCR→분류→추출→마스킹→저장→ReportJob 발행이 계약대로 이어지는가.
- 멱등 단락: 이미 저장된 job_id는 OCR을 건너뛰고 ReportJob만 재발행하는가.
- 결정적 report_id: 같은 ocr_result_id면 같은 report_id인가(재발행 멱등).
- fail-closed: 마스킹 후 고민감 PII가 남으면 저장·발행 없이 예외로 격리되는가.

마스킹 자체(정규식·NER)의 정확성은 test_masking 계열이 다룬다 — 여기서는 실제
기본 마스커(정규식, torch 불필요)를 써 배선이 실제로 PII를 가리는지까지 확인한다.
"""

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from core.contracts import DocType, OcrJob, ReportJob
from ocr_worker.masking.verify import MaskingError
from ocr_worker.ocr import OcrLine, OcrPage, OcrResult
from ocr_worker.pipeline import OcrPipeline, _derive_report_id
from ocr_worker.vlm_client import VlmClientError

_JOB_ID = "11111111-1111-1111-1111-111111111111"
_SAVE_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
_EXISTING_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")


# ── 페이크 경계 ──────────────────────────────────────────────────
class FakeProducer:
    """KafkaProducer.publish 대역 — 발행한 (topic, message, key)를 포착한다."""

    def __init__(self) -> None:
        self.published: list[tuple[str, ReportJob, str]] = []

    async def publish(self, topic: str, message: ReportJob, *, key: str) -> None:
        self.published.append((topic, message, key))


class FakePool:
    """asyncpg.Pool.fetchrow 대역 — SELECT(멱등 조회)와 INSERT(업서트)를 구분해 응답한다."""

    def __init__(self, existing: dict[str, Any] | None = None) -> None:
        self._existing = existing
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        self.calls.append((sql, args))
        if sql.lstrip().startswith("SELECT"):
            return self._existing
        return {"id": _SAVE_ID}  # 업서트 RETURNING id

    def insert_calls(self) -> list[tuple[str, tuple[Any, ...]]]:
        return [call for call in self.calls if "INSERT" in call[0]]


class FakeProcessor:
    """OcrProcessor.process_with_images 대역 — 고정 결과·이미지를 돌려주고 호출을 기록한다."""

    def __init__(self, result: OcrResult) -> None:
        self._result = result
        self.called = False

    async def process_with_images(
        self, s3_key: str, content_type: str
    ) -> tuple[OcrResult, list[object]]:
        self.called = True
        return self._result, [object()]  # 이미지 내용은 이미지 트랙 페이크가 무시


class _IdentityMasker:
    """마스킹을 하지 않는 마스커 대역 — fail-closed 게이트 검증용."""

    def detect(self, text: str) -> list[Any]:
        return []

    def mask(self, text: str) -> str:
        return text


# ── 헬퍼 ─────────────────────────────────────────────────────────
def _line(text: str) -> OcrLine:
    return OcrLine(
        text=text,
        bbox=(0.0, 0.0, 10.0, 5.0),
        polygon=((0.0, 0.0), (10.0, 0.0), (10.0, 5.0), (0.0, 5.0)),
        confidence=0.9,
    )


def _result(*texts: str) -> OcrResult:
    lines = tuple(_line(text) for text in texts)
    return OcrResult(pages=(OcrPage(index=0, width=100, height=50, lines=lines),))


def _job(**overrides: Any) -> OcrJob:
    base: dict[str, Any] = {
        "job_id": _JOB_ID,
        "s3_key": "uploads/x.pdf",
        "content_type": "application/pdf",
        "user_ref": "user-1",
        "doc_type_hint": None,
        "claim_id": None,
        "uploaded_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    base.update(overrides)
    return OcrJob(**base)


async def _fake_image_pipeline(job: OcrJob, result: OcrResult, images: list[object]) -> list[str]:
    return [f"masked/{job.job_id}/page-0.png"]


def _pipeline(
    pool: FakePool, producer: FakeProducer, processor: FakeProcessor, **kwargs: Any
) -> OcrPipeline:
    return OcrPipeline(
        pool=pool,
        producer=producer,  # type: ignore[arg-type]
        processor=processor,  # type: ignore[arg-type]
        image_pipeline=_fake_image_pipeline,
        **kwargs,
    )


# ── 정상 흐름 ────────────────────────────────────────────────────
async def test_full_flow_masks_persists_and_publishes() -> None:
    # Arrange: 보험증권 + 주민번호가 든 OCR 결과(실제 정규식 마스커 사용).
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("보험증권", "증권번호 202301-042", "홍길동 901010-1234567"))
    pipeline = _pipeline(pool, producer, processor)

    # Act
    await pipeline.handle(_job(claim_id="claim-9"))

    # Assert: 무거운 OCR을 실제로 수행했다.
    assert processor.called is True

    # 저장: masked_text에 원문 주민번호가 없고 마스킹 토큰이 있다. 이미지 키가 실린다.
    insert_args = pool.insert_calls()[0][1]
    masked_text = insert_args[4]
    assert "901010-1234567" not in masked_text
    assert "[주민등록번호]" in masked_text
    assert insert_args[1] == "policy"  # 분류 결과(StrEnum→text)
    assert json.loads(insert_args[7]) == [f"masked/{_JOB_ID}/page-0.png"]  # masked_image_s3_keys

    # 발행: report-job 토픽에 계약대로 ReportJob 1건.
    assert len(producer.published) == 1
    topic, report, key = producer.published[0]
    assert topic == "report-job"
    assert report.ocr_result_id == str(_SAVE_ID)
    assert report.report_id == _derive_report_id(str(_SAVE_ID))
    assert key == report.report_id  # 파티션 키 = report_id
    assert report.doc_type is DocType.POLICY
    assert report.job_id == _JOB_ID
    assert report.claim_id == "claim-9"  # 패스스루
    assert report.user_ref == "user-1"


async def test_publishes_masked_image_keys_from_image_track() -> None:
    # Arrange: 이미지 트랙이 돌려준 키가 그대로 ocr_results에 적재되는지.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("진단서", "상병명 골절"))

    async def two_page_keys(job: OcrJob, result: OcrResult, images: list[object]) -> list[str]:
        return [f"masked/{job.job_id}/page-0.png", f"masked/{job.job_id}/page-1.png"]

    pipeline = OcrPipeline(
        pool=pool,
        producer=producer,  # type: ignore[arg-type]
        processor=processor,  # type: ignore[arg-type]
        image_pipeline=two_page_keys,
    )

    # Act
    await pipeline.handle(_job())

    # Assert
    insert_args = pool.insert_calls()[0][1]
    assert json.loads(insert_args[7]) == [
        f"masked/{_JOB_ID}/page-0.png",
        f"masked/{_JOB_ID}/page-1.png",
    ]
    assert producer.published[0][1].doc_type is DocType.DIAGNOSIS


# ── 멱등 단락 ────────────────────────────────────────────────────
async def test_idempotent_short_circuit_skips_ocr_and_republishes() -> None:
    # Arrange: 이미 저장된 job_id(멱등 재소비).
    pool = FakePool(existing={"id": _EXISTING_ID, "doc_type": "policy"})
    producer = FakeProducer()
    processor = FakeProcessor(_result("무시됨"))
    pipeline = _pipeline(pool, producer, processor)

    # Act
    await pipeline.handle(_job())

    # Assert: OCR·저장을 건너뛰고 기존 id로 ReportJob만 재발행한다.
    assert processor.called is False
    assert pool.insert_calls() == []
    assert len(producer.published) == 1
    _, report, _ = producer.published[0]
    assert report.ocr_result_id == str(_EXISTING_ID)
    assert report.report_id == _derive_report_id(str(_EXISTING_ID))
    assert report.doc_type is DocType.POLICY


# ── 결정적 report_id ─────────────────────────────────────────────
def test_report_id_is_deterministic_per_ocr_result() -> None:
    a1 = _derive_report_id(str(_SAVE_ID))
    a2 = _derive_report_id(str(_SAVE_ID))
    b = _derive_report_id(str(_EXISTING_ID))

    assert a1 == a2  # 같은 결과 id → 같은 report_id(재발행 멱등)
    assert a1 != b  # 다른 결과 id → 다른 report_id
    assert a1 == str(uuid.uuid5(uuid.NAMESPACE_URL, f"report:{_SAVE_ID}"))


# ── 하이브리드 VLM 표 문서 경로 ──────────────────────────────────
class _RecordingVlm:
    """VLM 표 전사 대역 — 호출 여부·인자를 기록하고 고정 결과/예외를 돌려준다."""

    def __init__(self, *, text: str | None = None, error: Exception | None = None) -> None:
        self._text = text
        self._error = error
        self.called = False

    async def __call__(self, image: object) -> str:
        self.called = True
        if self._error is not None:
            raise self._error
        assert self._text is not None
        return self._text


_PAYOUT_TEXTS = ("보험금 지급결과 안내문", "지급결정 내역", "산정내역 및 지급사유 설명")


async def test_vlm_not_called_for_non_table_doc_type() -> None:
    # Arrange: 진단서는 표 문서 유형이 아니므로 VLM을 호출하지 않는다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("진단서", "상병명 골절"))
    vlm = _RecordingVlm(text="표 데이터")
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    assert vlm.called is False


async def test_vlm_success_replaces_masked_text_for_table_doc_type() -> None:
    # Arrange: 지급결과안내문(표 문서 유형) + PII 없는 VLM 마크다운 표 결과.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result(*_PAYOUT_TEXTS))
    vlm_text = "| 항목 | 금액 |\n|---|---|\n| 진찰료 | 10,000 |"
    vlm = _RecordingVlm(text=vlm_text)
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    assert vlm.called is True
    insert_args = pool.insert_calls()[0][1]
    masked_text = insert_args[4]
    entities = json.loads(insert_args[6])
    assert masked_text == vlm_text
    assert entities["table_markdown"] == vlm_text


async def test_vlm_failure_falls_back_to_surya_masked_text() -> None:
    # Arrange: VLM 연결 실패 — surya 기반 masked_text로 계속 진행해야 한다(job 안 죽음).
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result(*_PAYOUT_TEXTS))
    vlm = _RecordingVlm(error=VlmClientError("연결 실패"))
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert: 예외 없이 완료되고, surya 기반 텍스트가 그대로 저장됐다.
    assert vlm.called is True
    insert_args = pool.insert_calls()[0][1]
    masked_text = insert_args[4]
    entities = json.loads(insert_args[6])
    assert "지급결정" in masked_text  # surya 원문 유래(마스킹 대상 PII 없어 그대로)
    assert "table_markdown" not in entities
    assert len(producer.published) == 1  # 파이프라인은 정상 완료


async def test_vlm_residual_pii_discards_table_and_falls_back() -> None:
    # Arrange: VLM 원문에 PII가 있는데 마스커가 이를 못 가리는 상황(_IdentityMasker) —
    # surya 원문엔 PII가 없어 메인 게이트는 통과하지만, VLM 표는 폐기돼야 한다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result(*_PAYOUT_TEXTS))
    vlm = _RecordingVlm(text="수익자 901010-1234567")
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm, masker=_IdentityMasker())

    # Act
    await pipeline.handle(_job())

    # Assert: job은 정상 완료되고, VLM 표는 저장되지 않는다(surya 원문으로 폴백).
    insert_args = pool.insert_calls()[0][1]
    masked_text = insert_args[4]
    entities = json.loads(insert_args[6])
    assert "901010-1234567" not in masked_text
    assert "table_markdown" not in entities
    assert len(producer.published) == 1


async def test_masked_lines_unaffected_by_vlm_path() -> None:
    # Arrange: VLM 성공 여부와 무관하게 masked_lines(이미지 마스킹 bbox)는 surya 기반 그대로.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result(*_PAYOUT_TEXTS))
    vlm = _RecordingVlm(text="| 항목 | 금액 |\n|---|---|\n| 진찰료 | 10,000 |")
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert: 라인 수가 surya OCR 결과(3줄)와 일치 — VLM 결과로 대체되지 않았다.
    insert_args = pool.insert_calls()[0][1]
    masked_lines = json.loads(insert_args[5])
    assert len(masked_lines) == len(_PAYOUT_TEXTS)


# ── fail-closed(마스킹 잔류) ─────────────────────────────────────
async def test_masking_residual_raises_and_skips_persist() -> None:
    # Arrange: 마스킹을 안 하는 마스커 → 주민번호가 그대로 남는다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("홍길동 901010-1234567"))
    pipeline = _pipeline(pool, producer, processor, masker=_IdentityMasker())

    # Act / Assert: 고민감 PII 잔류 → MaskingError 전파(컨슈머가 DLQ 처리).
    with pytest.raises(MaskingError):
        await pipeline.handle(_job())

    # PII를 저장·발행하지 않는다(fail-closed).
    assert pool.insert_calls() == []
    assert producer.published == []
