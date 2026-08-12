"""OCR 파이프라인 오케스트레이션 테스트 (이슈 #15/#20).

SQS·DB·GPU·PIL 없이 경계(프로듀서·풀·OCR 프로세서·이미지 트랙)를 페이크로 주입해
``OcrPipeline.handle``의 흐름을 검증한다:
- 정상 흐름: OCR→분류→추출→마스킹→저장→ReportJob 발행이 계약대로 이어지는가.
- 멱등 단락: 이미 저장된 job_id는 OCR을 건너뛰고 ReportJob만 재발행하는가.
- 결정적 report_id: 같은 ocr_result_id면 같은 report_id인가(재발행 멱등).
- fail-closed: 마스킹 후 고민감 PII가 남으면 저장·발행 없이 예외로 격리되는가.
- 원본 삭제 게이트: 이미지 마스킹 검증(5.1+5.2)을 전 페이지가 통과할 때만 S3 원본을
  지우는가(실패 시 보존 + 로그, 예외 없음). 삭제는 저장 성공 **이후에** 트리거되지만
  ``ReportJob`` 발행이 그 완료를 기다리지 않는가(fire-and-forget).
- 삭제 outbox: 저장이 삭제 대상('pending')·비대상('not_eligible')을 남기는가, 즉시
  삭제의 성공/실패가 outbox에 반영되는가, 스윕이 남은 건을 재시도·기록하는가.
- 실패 저널: 결정적 실패는 terminal=true로, 일시 실패는 false로 남기고 원래 예외를
  전파하는가. 기록 자체가 실패하면 결정적 실패만 **재시도 가능한 예외로 바꿔** 던지는가
  (컨슈머가 즉시 ack해 무음 유실이 되지 않게). 성공·멱등 단락이 저널을 정리하는가.

마스킹 자체(정규식·NER)의 정확성은 test_masking 계열이 다룬다 — 여기서는 실제
기본 마스커(정규식, torch 불필요)를 써 배선이 실제로 PII를 가리는지까지 확인한다.
"""

import asyncio
import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from structlog.testing import capture_logs

from core.config import Settings
from core.contracts import DocType, OcrJob, ReportJob
from core.exceptions import NonRetryableError, OcrError, UnreadableFileError
from ocr_worker import pipeline as pipeline_module
from ocr_worker.masking.image_masker import ImageMasker
from ocr_worker.masking.spans import PiiLabel, Span
from ocr_worker.masking.verify import MaskingError
from ocr_worker.ocr import OcrLine, OcrPage, OcrResult
from ocr_worker.pipeline import ImageTrackResult, OcrPipeline, _derive_report_id
from ocr_worker.repository import DeleteRetryState, PendingDeletion
from ocr_worker.vlm_client import VlmClientError

# 삭제가 다시 블로킹이 되면 테스트가 영원히 멎지 않도록 두는 안전 상한(초).
# 정상 경로는 이 타이머를 쓰지 않고 즉시 끝나므로 flaky 요인이 아니다.
_NON_BLOCKING_TIMEOUT_S = 5.0

_JOB_ID = "11111111-1111-1111-1111-111111111111"
_SAVE_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
_EXISTING_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")


# ── 페이크 경계 ──────────────────────────────────────────────────
class FakeProducer:
    """SqsProducer.send 대역 — 발행한 (queue_url, message)를 포착한다."""

    def __init__(self) -> None:
        self.published: list[tuple[str, ReportJob]] = []

    async def send(self, queue_url: str, message: ReportJob) -> None:
        self.published.append((queue_url, message))


class FakePool:
    """asyncpg.Pool 대역 — SELECT(멱등 조회)·INSERT(업서트)·실패 저널 쓰기를 받아 기록한다."""

    def __init__(
        self, existing: dict[str, Any] | None = None, *, execute_error: Exception | None = None
    ) -> None:
        self._existing = existing
        self._execute_error = execute_error  # 저널 쓰기(DB) 장애 흉내
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        self.calls.append((sql, args))
        if sql.lstrip().startswith("SELECT"):
            return self._existing
        return {"id": _SAVE_ID}  # 업서트 RETURNING id

    async def execute(self, sql: str, *args: Any) -> None:
        # 실제 DB 왕복처럼 루프에 양보한다. 이게 없으면 저널 쓰기를 await하지 않고
        # task로 흘려보내는 구현(예외가 먼저 전파되는 회귀)도 테스트를 통과해버린다.
        await asyncio.sleep(0)
        self.calls.append((sql, args))
        if self._execute_error is not None:
            raise self._execute_error

    def insert_calls(self) -> list[tuple[str, tuple[Any, ...]]]:
        """``ocr_results`` 업서트만 추린다(실패 저널 쓰기는 제외 — 별도 테이블·별도 계약)."""
        return [
            call for call in self.calls if "INSERT" in call[0] and "ocr_job_failures" not in call[0]
        ]

    def journal_calls(self) -> list[tuple[str, tuple[Any, ...]]]:
        """실패 저널(``ai.ocr_job_failures``)로 나간 쓰기만 추린다."""
        return [call for call in self.calls if "ai.ocr_job_failures" in call[0]]


class _FakeImage:
    """PIL 이미지 대역 — ``image_to_png_bytes``가 쓰는 ``save``만 흉내낸다(PIL 불필요)."""

    def __init__(self, name: str) -> None:
        self.name = name

    def save(self, buffer: Any, format: str) -> None:
        # 인자명은 PIL 시그니처(image.save(fp, format=...))를 그대로 따른다.
        buffer.write(self.name.encode())


class FakeProcessor:
    """OcrProcessor.process_with_images 대역 — 고정 결과·이미지를 돌려주고 호출을 기록한다."""

    def __init__(self, result: OcrResult, *, engine: Any = None) -> None:
        self._result = result
        self.called = False
        # 재OCR 검증(5.2)이 프로세서의 엔진을 재사용하므로 같은 속성을 노출한다.
        self.engine = engine

    async def process_with_images(
        self, s3_key: str, content_type: str
    ) -> tuple[OcrResult, list[object]]:
        self.called = True
        # 이미지 내용 자체는 이미지 트랙 페이크가 무시하지만, _extract_pages가
        # images/pages를 zip(strict=True)하므로 개수는 페이지 수와 맞춰야 한다.
        return self._result, [_FakeImage(f"page-{page.index}") for page in self._result.pages]


class _IdentityMasker:
    """마스킹을 하지 않는 마스커 대역 — fail-closed 게이트 검증용."""

    def detect(self, text: str) -> list[Any]:
        return []

    def mask(self, text: str) -> str:
        return text


# ── 헬퍼 ─────────────────────────────────────────────────────────
def _line(text: str, *, confidence: float = 0.9) -> OcrLine:
    return OcrLine(
        text=text,
        bbox=(0.0, 0.0, 10.0, 5.0),
        polygon=((0.0, 0.0), (10.0, 0.0), (10.0, 5.0), (0.0, 5.0)),
        confidence=confidence,
    )


def _result(*texts: str, confidence: float = 0.9) -> OcrResult:
    lines = tuple(_line(text, confidence=confidence) for text in texts)
    return OcrResult(pages=(OcrPage(index=0, width=100, height=50, lines=lines),))


def _multi_page_result(*page_texts: str, confidence: float = 0.9) -> OcrResult:
    """페이지마다 텍스트 한 줄씩 담은 다중 페이지 결과(페이지별 VLM 합성 테스트용)."""
    pages = tuple(
        OcrPage(index=i, width=100, height=50, lines=(_line(text, confidence=confidence),))
        for i, text in enumerate(page_texts)
    )
    return OcrResult(pages=pages)


def _job(**overrides: Any) -> OcrJob:
    base: dict[str, Any] = {
        "job_id": _JOB_ID,
        "s3_key": "uploads/x.pdf",
        "content_type": "application/pdf",
        "user_ref": "user-1",
        "doc_type_hint": None,
        "claim_id": None,
        "report_id": "aaaaaaaa-0000-0000-0000-000000000001",
        "attachment_id": "aaaaaaaa-0000-0000-0000-000000000002",
        "doc_index": 1,
        "doc_total": 1,
        "uploaded_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    base.update(overrides)
    return OcrJob(**base)


async def _fake_image_pipeline(
    job: OcrJob, result: OcrResult, images: list[object]
) -> ImageTrackResult:
    """이미지 트랙 대역 — 사본 키만 돌려주고 원본 삭제는 판정하지 않는다(게이트는 별도 섹션)."""
    return ImageTrackResult(keys=[f"masked/{job.job_id}/page-0.png"], delete_original=False)


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

    # 발행: report-job 큐에 계약대로 ReportJob 1건.
    assert len(producer.published) == 1
    queue_url, report = producer.published[0]
    assert queue_url == pipeline._settings.sqs_report_job_queue_url  # 발행 큐 = 설정값
    assert report.ocr_result_id == str(_SAVE_ID)
    assert report.report_id == _derive_report_id(str(_SAVE_ID))
    assert report.doc_type is DocType.POLICY
    assert report.job_id == _JOB_ID
    assert report.claim_id == "claim-9"  # 패스스루
    assert report.user_ref == "user-1"


async def test_publishes_masked_image_keys_from_image_track() -> None:
    # Arrange: 이미지 트랙이 돌려준 키가 그대로 ocr_results에 적재되는지.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("진단서", "상병명 골절"))

    async def two_page_keys(
        job: OcrJob, result: OcrResult, images: list[object]
    ) -> ImageTrackResult:
        return ImageTrackResult(
            keys=[f"masked/{job.job_id}/page-0.png", f"masked/{job.job_id}/page-1.png"],
            delete_original=False,
        )

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
    # Arrange: 이미 저장된 job_id(멱등 재소비). 저장된 ocr_quality도 그대로 재사용된다.
    pool = FakePool(
        existing={"id": _EXISTING_ID, "doc_type": "policy", "ocr_quality": "needs_reupload"}
    )
    producer = FakeProducer()
    processor = FakeProcessor(_result("무시됨"))
    pipeline = _pipeline(pool, producer, processor)

    # Act
    await pipeline.handle(_job())

    # Assert: OCR·저장을 건너뛰고 기존 id·품질로 ReportJob만 재발행한다.
    assert processor.called is False
    assert pool.insert_calls() == []
    assert len(producer.published) == 1
    _, report = producer.published[0]
    assert report.ocr_result_id == str(_EXISTING_ID)
    assert report.report_id == _derive_report_id(str(_EXISTING_ID))
    assert report.doc_type is DocType.POLICY
    assert report.ocr_quality == "needs_reupload"


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


class _SequencedVlm:
    """페이지마다 다른 결과/예외를 순서대로 돌려주는 VLM 대역(다중 페이지 테스트용)."""

    def __init__(self, outcomes: list[str | Exception]) -> None:
        self._outcomes = list(outcomes)
        self.call_count = 0

    async def __call__(self, image: object) -> str:
        self.call_count += 1
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


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
    # Arrange: 지급결과안내문(표 문서 유형) + PII 없는 VLM 마크다운 표 결과. surya 원문
    # 단어(지급결정·내역 등)를 재사용해 groundedness 체크를 통과하게 한다(환각 아님).
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result(*_PAYOUT_TEXTS))
    vlm_text = (
        "| 항목 | 금액 |\n|---|---|\n"
        "| 지급결정 내역 | 10,000 |\n| 산정내역 지급사유 설명 | 20,000 |"
    )
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


async def test_vlm_backfills_domain_field_surya_missed() -> None:
    # Arrange: surya 원문엔 KCD 코드도 병명 라벨도 없어 diagnosis_name이 None이지만,
    # VLM이 더 깨끗하게 읽은 원문엔 라벨이 있어 실제로는 값이 존재한다 — surya의
    # extract()가 못 찾은 필드를 VLM 채택 원문 재추출로 보강해야 한다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("진단서 위염 관련 스캔 품질 낮음", confidence=0.5))
    vlm = _RecordingVlm(text="진단서 상병명: 급성 위염")
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    insert_args = pool.insert_calls()[0][1]
    entities = json.loads(insert_args[6])
    assert entities["diagnosis_name"] == "급성 위염"


async def test_vlm_value_overrides_field_surya_already_found() -> None:
    # Arrange: surya가 이미 KCD 코드를 찾았어도, VLM이 채택되면 VLM 쪽 값을
    # 우선한다 — VLM은 이미 groundedness 검증을 통과했고, 애초에 surya 신뢰도가
    # 낮아서 호출된 것이므로 더 신뢰할 근거가 있다(실측: surya가 금액을 다른
    # 문서 필드로 오독해 엉뚱한 값을 채운 사례를 근거로 우선순위를 뒤집었다).
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("진단서 A09.9 위염 스캔 품질 낮음", confidence=0.5))
    vlm = _RecordingVlm(text="진단서 상병명: 급성 위염 J20.9")
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    insert_args = pool.insert_calls()[0][1]
    entities = json.loads(insert_args[6])
    assert entities["diagnosis_name"] == "J20.9"


async def test_vlm_keeps_surya_value_when_vlm_text_has_no_match() -> None:
    # Arrange: surya가 KCD를 찾았고, VLM 채택 원문엔 KCD도 병명 라벨도 없다 —
    # 이 경우 vlm_entities는 None이라 surya 값을 그대로 유지해야 한다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("진단서 A09.9 위염 스캔 품질 낮음", confidence=0.5))
    vlm = _RecordingVlm(text="진단서 위염 관련 스캔 품질 낮음")
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    insert_args = pool.insert_calls()[0][1]
    entities = json.loads(insert_args[6])
    assert entities["diagnosis_name"] == "A09.9"


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


# ── 다중 페이지 VLM 합성(페이지별 독립 폴백) ─────────────────────
async def test_vlm_partial_page_failure_keeps_other_pages() -> None:
    # Arrange: 2페이지 표 문서. 1페이지는 VLM 성공(원문 단어 재사용해 grounded),
    # 2페이지는 VLM 호출 실패 — 실패한 페이지만 surya로 폴백하고 문서 전체가
    # 유실되지 않아야 한다(예전엔 1페이지 성공만으로 masked_text 전체가 교체돼
    # 2페이지 이후 내용이 통째로 사라졌었다).
    pool = FakePool(existing=None)
    producer = FakeProducer()
    page1 = "보험금 지급결과 안내문 지급결정 내역"
    page2 = "추가 산정 내역 진찰료 10000원"
    processor = FakeProcessor(_multi_page_result(page1, page2))
    vlm = _SequencedVlm(
        [
            "보험금 지급결과 안내문 지급결정 내역(표)",  # 1페이지: grounded → 채택
            VlmClientError("연결 실패"),  # 2페이지: 실패 → surya 폴백
        ]
    )
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    assert vlm.call_count == 2  # 페이지마다 독립 호출
    insert_args = pool.insert_calls()[0][1]
    masked_text = insert_args[4]
    assert "지급결정 내역(표)" in masked_text  # 1페이지: VLM 결과
    assert "추가 산정 내역" in masked_text  # 2페이지: surya 폴백으로 유지(유실 안 됨)


async def test_vlm_table_markdown_joins_only_adopted_pages() -> None:
    # Arrange: 3페이지 중 1·3페이지만 VLM 채택, 2페이지는 실패.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    page1, page2, page3 = (
        "보험금 지급결과 안내문",
        "지급결정 내역 산정",
        "산정내역 및 지급사유 설명",
    )
    processor = FakeProcessor(_multi_page_result(page1, page2, page3))
    vlm = _SequencedVlm(
        [
            "보험금 지급결과 안내문(표1)",
            VlmClientError("연결 실패"),
            "산정내역 및 지급사유 설명(표3)",
        ]
    )
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert: table_markdown엔 채택된 1·3페이지만 구분자로 이어붙는다(2페이지 없음).
    insert_args = pool.insert_calls()[0][1]
    entities = json.loads(insert_args[6])
    assert "(표1)" in entities["table_markdown"]
    assert "(표3)" in entities["table_markdown"]
    assert "지급결정 내역 산정" not in entities["table_markdown"]


async def test_vlm_all_pages_fail_falls_back_to_pure_surya() -> None:
    # Arrange: 2페이지 모두 VLM 실패 — masked_text는 전 페이지 surya 원문 그대로,
    # table_markdown 키 자체가 없어야 한다(단일 페이지 케이스와 동일한 계약 유지).
    pool = FakePool(existing=None)
    producer = FakeProducer()
    page1, page2 = "보험금 지급결과 안내문", "지급결정 내역"
    processor = FakeProcessor(_multi_page_result(page1, page2))
    vlm = _SequencedVlm([VlmClientError("연결 실패"), VlmClientError("연결 실패")])
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    insert_args = pool.insert_calls()[0][1]
    masked_text = insert_args[4]
    entities = json.loads(insert_args[6])
    assert page1 in masked_text
    assert page2 in masked_text
    assert "table_markdown" not in entities


# ── 저신뢰도 VLM 트리거 ──────────────────────────────────────────
async def test_vlm_called_for_non_table_doc_type_when_confidence_low() -> None:
    # Arrange: 진단서(표 문서 아님)지만 surya 신뢰도가 낮으면(<0.90) 문서 유형과
    # 무관하게 VLM 보완을 시도한다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("진단서", "상병명 골절", confidence=0.5))
    vlm = _RecordingVlm(text="진단서 상병명 골절")
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    assert vlm.called is True


async def test_vlm_not_called_for_non_table_doc_type_when_confidence_high() -> None:
    # Arrange: 표 문서도 아니고 신뢰도도 충분히 높으면(>=0.90) VLM을 호출하지 않는다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("진단서", "상병명 골절", confidence=0.95))
    vlm = _RecordingVlm(text="진단서 상병명 골절")
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    assert vlm.called is False


# ── VLM 환각 완화(groundedness) ──────────────────────────────────
async def test_vlm_ungrounded_result_discarded_and_falls_back_to_surya() -> None:
    # Arrange: 표 문서지만 VLM이 surya 원문과 무관한 내용을 지어낸 상황(환각) —
    # groundedness 체크로 폐기되고 surya 기반 값이 그대로 유지돼야 한다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result(*_PAYOUT_TEXTS))
    vlm = _RecordingVlm(text="완전히 무관한 히알루론산 시술 안내 텍스트입니다")
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    assert vlm.called is True
    insert_args = pool.insert_calls()[0][1]
    masked_text = insert_args[4]
    entities = json.loads(insert_args[6])
    assert "지급결정" in masked_text  # surya 원문 유지(VLM 결과 폐기)
    assert "table_markdown" not in entities


async def test_vlm_empty_result_treated_as_ungrounded_not_adopted() -> None:
    # Arrange — 코드리뷰 지적: 빈 VLM 응답(토큰 없음)을 grounded로 처리하면 이 페이지가
    # "채택"으로 표시되고 검증된 surya 텍스트가 빈 문자열로 덮어써진다(실유실). 빈 결과도
    # 실패로 취급해 surya 폴백이 유지돼야 한다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result(*_PAYOUT_TEXTS))
    vlm = _RecordingVlm(text="")
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    assert vlm.called is True
    insert_args = pool.insert_calls()[0][1]
    masked_text = insert_args[4]
    entities = json.loads(insert_args[6])
    assert "지급결정" in masked_text  # surya 원문 유지(빈 VLM 결과 폐기)
    assert "table_markdown" not in entities


# ── "재확인 필요"(ocr_quality) 판정 ──────────────────────────────
async def test_ocr_quality_needs_reupload_when_low_confidence_no_name_no_domain_info() -> None:
    # Arrange: 신뢰도 낮음 + 이름·도메인 정보(보험사·상품명) 전무. 저신뢰도는 VLM도
    # 트리거하므로(문서 유형 무관) 네트워크 호출 없이 실패로 페이크해 surya 기반
    # quality_source_text가 유지되게 한다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("보험증권", "증권번호 202301-042", confidence=0.5))
    vlm = _RecordingVlm(error=VlmClientError("연결 실패"))
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    insert_args = pool.insert_calls()[0][1]
    assert insert_args[8] == "needs_reupload"
    assert producer.published[0][1].ocr_quality == "needs_reupload"


async def test_ocr_quality_needs_reupload_when_name_present_but_domain_info_missing() -> None:
    # Arrange: 이름은 검출되지만(서명란) 보험사·상품명 등 도메인 정보가 전무.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(
        _result("보험증권", "증권번호 202301-042", "홍길동(서명)", confidence=0.5)
    )
    vlm = _RecordingVlm(error=VlmClientError("연결 실패"))
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    insert_args = pool.insert_calls()[0][1]
    assert insert_args[8] == "needs_reupload"


async def test_ocr_quality_needs_reupload_when_vlm_adopted_but_no_domain_entities() -> None:
    # Arrange — 코드리뷰 지적: VLM이 채택되면 entities.table_markdown이 거의 항상
    # 채워지는데, 이를 도메인 정보로 세면 doc_type 고유 필드(admission_days/surgery)가
    # 하나도 안 뽑힌 저신뢰도 문서도 "표 문서라 VLM이 성공했다"는 이유만으로 ok가
    # 되어버렸다. 이름은 있지만 도메인 필드는 전무한 입퇴원확인서로 재현한다
    # (HOSPITALIZATION_CERT는 표 문서 유형이라 VLM이 항상 시도됨).
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("입원확인서", "환자 성명 홍길동", confidence=0.5))
    vlm = _RecordingVlm(text="입원확인서 환자 성명 홍길동 입원사실을 확인합니다")
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    assert vlm.called is True
    insert_args = pool.insert_calls()[0][1]
    entities = json.loads(insert_args[6])
    assert "table_markdown" in entities  # VLM은 채택됨
    assert entities["admission_days"] is None
    assert entities["surgery"] is None
    assert insert_args[8] == "needs_reupload"  # table_markdown만으로 domain info 인정 안 함


async def test_ocr_quality_ok_for_medical_receipt_when_vlm_adopted_and_name_present() -> None:
    # Arrange — 위 테스트(#7 반영)의 부작용 회귀: MEDICAL_RECEIPT는 extract()가 애초에
    # doc_type 고유 필드를 정의하지 않는 유형이라(진료비 항목은 table_markdown에만
    # 담김), table_markdown을 일괄 제외하면 이 유형만 VLM이 성공해도 확인할 필드 자체가
    # 없어 항상 needs_reupload가 되던 문제가 있었다. 이 유형은 table_markdown 유무를
    # 그대로 신호로 써야 한다 — VLM 성공 + 이름 존재면 ok여야 한다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("진료비영수증", "환자 성명 홍길동", confidence=0.5))
    vlm = _RecordingVlm(text="진료비영수증 환자 성명 홍길동 영수금액 47500원")
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    assert vlm.called is True
    insert_args = pool.insert_calls()[0][1]
    entities = json.loads(insert_args[6])
    assert entities == {"table_markdown": entities["table_markdown"]}  # 고유 필드 없음
    assert insert_args[8] == "ok"


async def test_ocr_quality_needs_reupload_for_medical_receipt_when_vlm_fails() -> None:
    # Arrange — MEDICAL_RECEIPT라도 VLM이 아예 실패해 table_markdown조차 없으면
    # (entities == {}) 여전히 needs_reupload여야 한다(위 fix가 무조건 ok로 바꾸면 안 됨).
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("진료비영수증", "영수금액 47500원", confidence=0.5))
    vlm = _RecordingVlm(error=VlmClientError("연결 실패"))
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    insert_args = pool.insert_calls()[0][1]
    entities = json.loads(insert_args[6])
    assert entities == {}
    assert insert_args[8] == "needs_reupload"


async def test_ocr_quality_ok_when_low_confidence_but_name_and_domain_info_present() -> None:
    # Arrange: 신뢰도는 낮지만 이름·도메인 정보(보험사·상품명)가 모두 검출됨.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(
        _result(
            "보험증권",
            "증권번호 202301-042",
            "현대해상",
            "상품명: 무배당건강보험",
            "홍길동(서명)",
            confidence=0.5,
        )
    )
    vlm = _RecordingVlm(error=VlmClientError("연결 실패"))
    pipeline = _pipeline(pool, producer, processor, vlm_transcribe=vlm)

    # Act
    await pipeline.handle(_job())

    # Assert
    insert_args = pool.insert_calls()[0][1]
    assert insert_args[8] == "ok"
    assert producer.published[0][1].ocr_quality == "ok"


async def test_ocr_quality_ok_when_confidence_high_even_without_name_or_domain_info() -> None:
    # Arrange: 이름·도메인 정보가 전무해도, 신뢰도가 충분히 높으면(>=0.90)
    # needs_reupload로 판정하지 않는다(저신뢰 게이트가 AND 조건).
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(_result("보험증권", "증권번호 202301-042", confidence=0.95))
    pipeline = _pipeline(pool, producer, processor)

    # Act
    await pipeline.handle(_job())

    # Assert
    insert_args = pool.insert_calls()[0][1]
    assert insert_args[8] == "ok"


# ── 원본 삭제 게이트(이미지 마스킹 검증 5.1+5.2) ─────────────────
class _RecordingS3:
    """S3 업로드·삭제 대역 — 호출 순서를 한 로그에 담아 "업로드 후 삭제" 계약까지 고정한다.

    ``events``를 밖에서 주입하면 다른 경계(예: DB 저장)의 이벤트와 한 타임라인에 섞어
    "저장 후 삭제" 같은 교차 순서까지 고정할 수 있다. ``delete_gate``는 삭제를 테스트가
    열어줄 때까지 붙잡아 둔다 — sleep 없이 "발행이 삭제를 기다리지 않는다"를 결정적으로
    관찰하기 위한 장치다.
    """

    def __init__(
        self,
        *,
        delete_error: Exception | None = None,
        delete_errors: dict[str, Exception] | None = None,
        events: list[tuple[str, str]] | None = None,
        delete_gate: asyncio.Event | None = None,
    ) -> None:
        self.events: list[tuple[str, str]] = [] if events is None else events
        self._delete_error = delete_error
        # 키별 실패 지정(스윕 배치에서 일부만 실패하는 상황용).
        self._delete_errors = delete_errors or {}
        self._delete_gate = delete_gate
        # 삭제 호출이 실제로 끝났는지(성공·실패 무관) — 발행 시점 관찰용.
        self.delete_finished = asyncio.Event()
        # 삭제 호출에 **진입**했는지 — "S3 왕복 중"을 붙잡고 취소하는 테스트용.
        self.delete_started = asyncio.Event()

    async def upload(self, key: str, data: bytes, content_type: str) -> None:
        self.events.append(("upload", key))

    async def delete(self, key: str) -> None:
        self.delete_started.set()
        if self._delete_gate is not None:
            await self._delete_gate.wait()
        self.events.append(("delete", key))
        self.delete_finished.set()
        error = self._delete_errors.get(key, self._delete_error)
        if error is not None:
            raise error

    @property
    def deleted(self) -> list[str]:
        return [key for kind, key in self.events if kind == "delete"]


class _TimelinePool(FakePool):
    """저장(INSERT) 시점을 S3 이벤트 로그에 함께 남기는 풀 대역(교차 순서 검증용)."""

    def __init__(self, events: list[tuple[str, str]]) -> None:
        super().__init__(existing=None)
        self._events = events

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        # 실제 asyncpg는 네트워크 왕복에서 반드시 루프에 양보한다. 그 양보를 흉내내지
        # 않으면(진짜 await point 없는 순수 async def) 저장 **전에** 스케줄된 삭제 task가
        # 끼어들 틈이 없어, 순서가 뒤바뀐 코드도 항상 통과하는 착시가 생긴다.
        await asyncio.sleep(0)
        row = await super().fetchrow(sql, *args)
        if "INSERT" in sql:
            self._events.append(("save", str(_SAVE_ID)))
        return row


class _ProbingProducer(FakeProducer):
    """발행 **시점에** 원본 삭제가 끝나 있었는지를 함께 기록하는 프로듀서 대역."""

    def __init__(self, s3: _RecordingS3) -> None:
        super().__init__()
        self._s3 = s3
        self.delete_finished_at_publish: list[bool] = []

    async def send(self, queue_url: str, message: ReportJob) -> None:
        self.delete_finished_at_publish.append(self._s3.delete_finished.is_set())
        await super().send(queue_url, message)


class _FakeReocrEngine:
    """재OCR(5.2) 엔진 대역 — 마스킹 사본에서 읽히는 텍스트를 고정으로 돌려준다."""

    def __init__(self, text: str = "") -> None:
        self._text = text
        self.seen_images: list[str] = []

    def recognize(self, images: list[Any]) -> list[list[OcrLine]]:
        self.seen_images.append(images[0].name)
        return [[_line(self._text)]]


class _RecordingCoverage:
    """커버리지 측정(5.1) 대역 — 어떤 이미지의 어느 라인을 쟀는지 기록한다(numpy 불필요)."""

    def __init__(self, ratio: float = 1.0) -> None:
        self._ratio = ratio
        self.calls: list[tuple[str, set[int]]] = []

    def __call__(self, image: Any, page: OcrPage, line_indices: set[int]) -> dict[int, float]:
        self.calls.append((image.name, set(line_indices)))
        return {index: self._ratio for index in line_indices}


def _detect_name(text: str) -> list[Span]:
    """'홍길동'만 PII로 보는 검출기 대역 — 어느 라인이 가려지는지를 테스트가 통제한다."""
    start = text.find("홍길동")
    return [Span(start, start + 3, PiiLabel.NAME)] if start >= 0 else []


def _fake_redactor(image: Any, page: OcrPage, indices: set[int], boxes: list[Any]) -> _FakeImage:
    """검은블럭 렌더 대역 — 원본과 구분되는 이름의 사본을 돌려준다(PIL 불필요)."""
    return _FakeImage(f"redacted-{page.index}")


def _gate_pipeline(
    pool: FakePool,
    producer: FakeProducer,
    processor: FakeProcessor,
    s3: _RecordingS3,
    coverage: _RecordingCoverage,
    **kwargs: Any,
) -> OcrPipeline:
    """실제 ``_default_image_pipeline``(렌더→업로드→검증→삭제)을 타는 파이프라인."""
    return OcrPipeline(
        pool=pool,
        producer=producer,  # type: ignore[arg-type]
        processor=processor,  # type: ignore[arg-type]
        image_masker=ImageMasker(detect=_detect_name, redactor=_fake_redactor),
        coverage=coverage,
        upload=s3.upload,
        delete=s3.delete,
        **kwargs,
    )


async def test_original_deleted_after_upload_when_all_pages_verified() -> None:
    # Arrange: 1페이지엔 PII(가림), 2페이지엔 없음. 커버리지 충분 + 재OCR 잔류 없음.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    engine = _FakeReocrEngine("")
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동", "증권번호 202301-042"), engine=engine
    )
    s3 = _RecordingS3()
    coverage = _RecordingCoverage(ratio=1.0)
    pipeline = _gate_pipeline(pool, producer, processor, s3, coverage)

    # Act: 삭제는 백그라운드 task라 훅으로 완료를 기다린다(sleep 없이 결정적).
    await pipeline.handle(_job())
    await pipeline.wait_for_pending_deletes()

    # Assert: 사본 2장 업로드가 모두 끝난 **뒤에** 원본을 지운다.
    assert s3.events == [
        ("upload", f"masked/{_JOB_ID}/page-0.png"),
        ("upload", f"masked/{_JOB_ID}/page-1.png"),
        ("delete", "uploads/x.pdf"),
    ]
    # 검증은 원본이 아니라 **마스킹 사본**을 대상으로, redact가 가린 라인을 그대로 잰다.
    assert coverage.calls == [("redacted-0", {0})]
    assert engine.seen_images == ["redacted-0"]


async def test_original_kept_when_coverage_below_threshold() -> None:
    # Arrange: 5.1 커버리지 미달(검은블럭이 bbox를 덜 덮음).
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("")
    )
    s3 = _RecordingS3()
    pipeline = _gate_pipeline(pool, producer, processor, s3, _RecordingCoverage(ratio=0.5))

    # Act
    await pipeline.handle(_job())

    # Assert: 원본 보존. 예외는 던지지 않고 저장·발행은 정상 진행된다(사본은 이미 업로드됨).
    assert s3.deleted == []
    assert json.loads(pool.insert_calls()[0][1][7]) == [f"masked/{_JOB_ID}/page-0.png"]
    assert len(producer.published) == 1
    # 검증 실패분은 outbox에서도 'not_eligible' — 스윕이 나중에 집어 지우면 게이트를
    # 우회하는 셈이 된다(원본 삭제는 되돌릴 수 없다).
    assert pool.insert_calls()[0][1][10] == "not_eligible"


async def test_original_kept_when_reocr_finds_residual_pii() -> None:
    # Arrange: 5.2 — 블럭이 빗나가 사본 재OCR에서 주민번호가 다시 읽힌다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("901010-1234567")
    )
    s3 = _RecordingS3()
    pipeline = _gate_pipeline(pool, producer, processor, s3, _RecordingCoverage(ratio=1.0))

    # Act
    await pipeline.handle(_job())

    # Assert
    assert s3.deleted == []
    assert len(producer.published) == 1  # 파이프라인은 정상 완료(DLQ 사안 아님)


async def test_pages_without_pii_are_not_reocred_and_do_not_block_delete() -> None:
    # Arrange: 어느 페이지에도 PII가 없다 — 가린 게 없으니 검증(재OCR)도 돌리지 않지만,
    # OCR 텍스트 자체는 읽혔으므로 원본 삭제는 막지 않는다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    engine = _FakeReocrEngine("")
    processor = FakeProcessor(_multi_page_result("보험증권", "증권번호 202301-042"), engine=engine)
    s3 = _RecordingS3()
    coverage = _RecordingCoverage(ratio=1.0)
    pipeline = _gate_pipeline(pool, producer, processor, s3, coverage)

    # Act
    await pipeline.handle(_job())
    await pipeline.wait_for_pending_deletes()

    # Assert
    assert engine.seen_images == []  # 가장 비싼 재OCR을 무의미하게 돌리지 않는다
    assert coverage.calls == []
    assert s3.deleted == ["uploads/x.pdf"]


async def test_original_kept_when_ocr_read_no_text() -> None:
    # Arrange: OCR이 한 줄도 못 읽음 — "가릴 게 없다"와 "못 읽어서 못 가렸다"를 구분할 수
    # 없으므로 원본을 지우지 않는다(사본 == 원본). 저신뢰도라 VLM도 트리거되므로 실패로 페이크.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    empty = OcrResult(pages=(OcrPage(index=0, width=100, height=50, lines=()),))
    processor = FakeProcessor(empty, engine=_FakeReocrEngine(""))
    s3 = _RecordingS3()
    pipeline = _gate_pipeline(
        pool,
        producer,
        processor,
        s3,
        _RecordingCoverage(ratio=1.0),
        vlm_transcribe=_RecordingVlm(error=VlmClientError("연결 실패")),
    )

    # Act
    await pipeline.handle(_job())

    # Assert
    assert s3.deleted == []
    assert len(producer.published) == 1


async def test_original_kept_when_only_one_page_has_no_ocr_text() -> None:
    # Arrange: 혼합 문서 — 1페이지는 읽혔고(PII까지 가려짐) 2페이지만 라인 0개.
    # 가드가 **페이지 단위**여야 이 문서를 잡는다. 문서 레벨(`all(not page.lines)`)로
    # 되돌리면 "읽힌 페이지가 하나라도 있으니 통과"가 돼, 미마스킹 원본 그대로인
    # 2페이지 사본을 남긴 채 원본을 지워버린다(복구 불가).
    pool = FakePool(existing=None)
    producer = FakeProducer()
    mixed = OcrResult(
        pages=(
            OcrPage(index=0, width=100, height=50, lines=(_line("보험증권 계약자 홍길동"),)),
            OcrPage(index=1, width=100, height=50, lines=()),
        )
    )
    processor = FakeProcessor(mixed, engine=_FakeReocrEngine(""))
    s3 = _RecordingS3()
    pipeline = _gate_pipeline(pool, producer, processor, s3, _RecordingCoverage(ratio=1.0))

    # Act
    await pipeline.handle(_job())
    await pipeline.wait_for_pending_deletes()

    # Assert: 사본 2장은 올라가되 원본은 남는다(예외 없이 저장·발행은 정상 진행).
    assert s3.deleted == []
    assert json.loads(pool.insert_calls()[0][1][7]) == [
        f"masked/{_JOB_ID}/page-0.png",
        f"masked/{_JOB_ID}/page-1.png",
    ]
    assert len(producer.published) == 1


async def test_delete_failure_does_not_fail_the_job() -> None:
    # Arrange: 검증은 통과했지만 S3 삭제가 실패 — 사본·저장은 이미 유효하므로 작업을
    # 되돌리지 않는다(원본이 남을 뿐, 운영 정리 대상). 백그라운드 task라 SQS 재전달이
    # 이 실패를 받아주지 못하므로, 조용히 사라지지 않고 경고로 드러나는지까지 본다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("")
    )
    s3 = _RecordingS3(delete_error=OcrError("S3 삭제 실패: uploads/x.pdf"))
    pipeline = _gate_pipeline(pool, producer, processor, s3, _RecordingCoverage(ratio=1.0))

    # Act
    with capture_logs() as logs:
        await pipeline.handle(_job())
        published_before_delete = len(producer.published)
        await pipeline.wait_for_pending_deletes()

    # Assert
    assert published_before_delete == 1  # 실패할 삭제를 기다리지 않고 이미 발행됐다
    assert s3.deleted == ["uploads/x.pdf"]  # 시도는 했다
    events = [entry["event"] for entry in logs]
    assert "original_delete_failed" in events  # task 완료 후 경고가 남는다
    # _delete_original이 이미 흡수했으므로 콜백의 "예기치 못한 예외" 경로는 타지 않는다.
    assert "original_delete_task_error" not in events
    assert pipeline._pending_deletes == set()  # 실패해도 task 참조를 흘리지 않는다


async def test_unexpected_delete_exception_is_retrieved_and_logged() -> None:
    # Arrange: _delete_original의 `except OcrError` 방어선을 빠져나가는 예외.
    # fire-and-forget이라 아무도 결과를 안 읽으면 asyncio의 "Task exception was never
    # retrieved"만 뜨고 실패가 묻힌다 — 콜백이 꺼내 경고로 드러내는지 확인한다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("")
    )
    s3 = _RecordingS3(delete_error=RuntimeError("boto3 내부 오류"))
    pipeline = _gate_pipeline(pool, producer, processor, s3, _RecordingCoverage(ratio=1.0))

    # Act
    with capture_logs() as logs:
        await pipeline.handle(_job())
        await pipeline.wait_for_pending_deletes()

    # Assert: 작업 자체는 정상 완료되고, 예외는 타입만 로그로 남는다(§9 — 메시지 금지).
    assert len(producer.published) == 1
    task_errors = [entry for entry in logs if entry["event"] == "original_delete_task_error"]
    assert [entry["error_type"] for entry in task_errors] == ["RuntimeError"]
    assert "boto3 내부 오류" not in json.dumps(logs, default=str)
    assert pipeline._pending_deletes == set()


async def test_cancelled_delete_task_is_logged_and_released() -> None:
    # Arrange: 삭제 task가 취소되는 경우(종료 중 루프 teardown 등). 콜백이 CancelledError를
    # exception()으로 건드리지 않고(그러면 되던지기) 경고만 남기고 참조를 놓는지 본다.
    gate = asyncio.Event()  # 열지 않는다 — task를 취소 가능한 상태로 붙잡아 둔다.
    s3 = _RecordingS3(delete_gate=gate)
    pipeline = _gate_pipeline(
        FakePool(existing=None),
        FakeProducer(),
        FakeProcessor(_multi_page_result("보험증권"), engine=_FakeReocrEngine("")),
        s3,
        _RecordingCoverage(ratio=1.0),
    )

    # Act: 파이프라인 전체를 돌릴 필요 없이 스케줄러만 직접 호출한다(콜백 단위 검증).
    with capture_logs() as logs:
        pipeline._schedule_original_delete(_job(), str(_SAVE_ID))
        task = next(iter(pipeline._pending_deletes))
        task.cancel()
        await pipeline.wait_for_pending_deletes()

    # Assert
    assert task.cancelled()
    assert s3.deleted == []  # 게이트를 못 넘었으므로 S3는 건드리지 않았다
    assert "original_delete_cancelled" in [entry["event"] for entry in logs]
    assert pipeline._pending_deletes == set()


async def test_report_is_published_without_waiting_for_original_delete() -> None:
    # Arrange: 삭제를 게이트로 붙잡아 둔다 — 발행이 S3 왕복 완료를 기다리는지 본다.
    # sleep 대신 asyncio.Event를 써서 타이밍 의존(flaky) 없이 결정적으로 관찰한다.
    gate = asyncio.Event()
    s3 = _RecordingS3(delete_gate=gate)
    pool = FakePool(existing=None)
    producer = _ProbingProducer(s3)
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("")
    )
    pipeline = _gate_pipeline(pool, producer, processor, s3, _RecordingCoverage(ratio=1.0))

    # Act: 게이트를 열지 않은 채로 핸들러가 끝나야 한다. 블로킹으로 되돌아가면 영원히
    # 멎으므로 상한을 둔다 — 정상 경로에선 대기가 없어 벽시계 시간을 쓰지 않는다.
    async with asyncio.timeout(_NON_BLOCKING_TIMEOUT_S):
        await pipeline.handle(_job())

    # Assert: 발행은 끝났고, 그 시점에 삭제는 아직 진행 중이었다.
    assert producer.delete_finished_at_publish == [False]
    assert len(producer.published) == 1
    assert s3.deleted == []

    # 게이트를 열면 백그라운드 task가 삭제를 마치고 참조도 정리된다.
    gate.set()
    await pipeline.wait_for_pending_deletes()
    assert s3.deleted == ["uploads/x.pdf"]
    assert pipeline._pending_deletes == set()


async def test_original_delete_is_triggered_only_after_ocr_result_saved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 저장(INSERT)·S3·outbox 기록을 한 타임라인에 기록한다 — non-blocking 전환
    # 후에도 "저장 성공 이후에만 삭제"라는 순서 계약(저장 전 삭제 = 복구 불가)이
    # 유지되는지, 그리고 종결 기록이 **실제 삭제 뒤에** 오는지(먼저 쓰면 지우지도 않은
    # 원본을 'deleted'로 종결시켜 스윕이 영영 재시도하지 않는다) 고정한다.
    # 페이크(_TimelinePool·_RecordingOutbox)는 실제 asyncpg처럼 루프에 양보한다 —
    # 양보하지 않으면 백그라운드 삭제 task가 끼어들 틈이 없어 순서 회귀를 못 잡는다.
    events: list[tuple[str, str]] = []
    outbox = _RecordingOutbox(events=events)
    _install_outbox(monkeypatch, outbox)
    pool = _TimelinePool(events)
    producer = FakeProducer()
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("")
    )
    s3 = _RecordingS3(events=events)
    pipeline = _gate_pipeline(pool, producer, processor, s3, _RecordingCoverage(ratio=1.0))

    # Act
    await pipeline.handle(_job())
    await pipeline.wait_for_pending_deletes()

    # Assert
    assert [kind for kind, _ in events] == ["upload", "save", "delete", "record_success"]


async def test_original_not_deleted_when_no_pages_rendered() -> None:
    # Arrange: 페이지 이미지가 없으면 비식별 사본 자체가 없다 → 삭제 게이트 진입 금지.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    processor = FakeProcessor(OcrResult(pages=()), engine=_FakeReocrEngine(""))
    s3 = _RecordingS3()
    pipeline = _gate_pipeline(pool, producer, processor, s3, _RecordingCoverage(ratio=1.0))

    # Act
    await pipeline.handle(_job())

    # Assert
    assert s3.events == []
    assert json.loads(pool.insert_calls()[0][1][7]) == []


# ── 원본 삭제 outbox(재시도) ─────────────────────────────────────
class _RecordingOutbox:
    """repository의 outbox 기록 함수 대역 — 어떤 결과를 어떤 인자로 남겼는지 포착한다.

    ``events``를 주입하면 S3·DB 이벤트와 한 타임라인에 섞어 "삭제 → 기록" 같은 교차
    순서까지 고정할 수 있다. 각 메서드는 실제 asyncpg처럼 **루프에 양보한다**
    (``asyncio.sleep(0)``) — 양보하지 않는 순수 ``async def``는 백그라운드 task가
    끼어들 틈을 없애, 순서가 뒤바뀐 코드도 통과시키는 착시를 만든다.
    """

    def __init__(
        self,
        *,
        events: list[tuple[str, str]] | None = None,
        error_on: str | None = None,
        attempts: dict[str, int] | None = None,
    ) -> None:
        self.events: list[tuple[str, str]] = [] if events is None else events
        self.successes: list[str] = []
        self.failures: list[tuple[str, int, float]] = []
        self._error_on = error_on  # 이 ocr_result_id 기록 시 DB 오류를 낸다
        # 행별 누적 시도 횟수(= DB 컬럼). SQL의 CASE와 같은 규칙으로 상태를 계산해,
        # 호출측이 **반환값**으로 exhausted를 판정하는지 검증할 수 있게 한다.
        self._attempts: dict[str, int] = dict(attempts or {})

    async def record_success(self, pool: Any, ocr_result_id: str) -> None:
        await asyncio.sleep(0)
        if self._error_on == ocr_result_id:
            raise RuntimeError("DB 연결 끊김")
        self.successes.append(ocr_result_id)
        self.events.append(("record_success", ocr_result_id))

    async def record_failure(
        self, pool: Any, ocr_result_id: str, max_attempts: int, retry_interval_seconds: float
    ) -> DeleteRetryState:
        await asyncio.sleep(0)
        if self._error_on == ocr_result_id:
            raise RuntimeError("DB 연결 끊김")
        self.failures.append((ocr_result_id, max_attempts, retry_interval_seconds))
        self.events.append(("record_failure", ocr_result_id))
        attempts = self._attempts.get(ocr_result_id, 0) + 1
        self._attempts[ocr_result_id] = attempts
        # UPDATE ... RETURNING이 돌려주는 **갱신 후** 상태(SQL CASE와 같은 규칙).
        status = "exhausted" if attempts >= max_attempts else "pending"
        return DeleteRetryState(status=status, attempts=attempts)


class _FakeDueDeletions:
    """``fetch_due_deletions`` 대역 — 고정 배치를 돌려주고 요청 limit을 기록한다."""

    def __init__(self, rows: list[PendingDeletion]) -> None:
        self._rows = rows
        self.limits: list[int] = []

    async def __call__(self, pool: Any, limit: int) -> list[PendingDeletion]:
        await asyncio.sleep(0)  # 실제 조회의 네트워크 왕복 양보를 흉내
        self.limits.append(limit)
        return self._rows


def _install_outbox(
    monkeypatch: pytest.MonkeyPatch,
    outbox: _RecordingOutbox,
    due: _FakeDueDeletions | None = None,
) -> None:
    """파이프라인이 부르는 repository 함수를 페이크로 갈아끼운다(DB 불필요)."""
    monkeypatch.setattr(pipeline_module, "record_delete_success", outbox.record_success)
    monkeypatch.setattr(pipeline_module, "record_delete_failure", outbox.record_failure)
    if due is not None:
        monkeypatch.setattr(pipeline_module, "fetch_due_deletions", due)


def _sweep_pipeline(s3: _RecordingS3, settings: Settings | None = None) -> OcrPipeline:
    """스윕만 돌리는 최소 파이프라인(OCR·이미지 트랙은 타지 않는다)."""
    return OcrPipeline(
        pool=FakePool(existing=None),  # type: ignore[arg-type]
        producer=FakeProducer(),  # type: ignore[arg-type]
        processor=FakeProcessor(OcrResult(pages=())),  # type: ignore[arg-type]
        image_masker=ImageMasker(detect=_detect_name, redactor=_fake_redactor),
        upload=s3.upload,
        delete=s3.delete,
        settings=settings,
    )


def _due(key: str, *, attempts: int = 0, row_id: str = "row-1") -> PendingDeletion:
    return PendingDeletion(id=row_id, original_s3_key=key, original_delete_attempts=attempts)


async def test_save_marks_outbox_pending_for_deletable_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 검증 통과 → 저장 시 outbox에 삭제 대상('pending')으로 남아야 한다.
    # 이게 있어야 즉시 삭제 전에 crash가 나도 스윕이 이어받을 근거가 생긴다.
    outbox = _RecordingOutbox()
    _install_outbox(monkeypatch, outbox)
    pool = FakePool(existing=None)
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("")
    )
    s3 = _RecordingS3()
    pipeline = _gate_pipeline(pool, FakeProducer(), processor, s3, _RecordingCoverage(ratio=1.0))

    # Act
    await pipeline.handle(_job())
    await pipeline.wait_for_pending_deletes()

    # Assert
    insert_args = pool.insert_calls()[0][1]
    assert insert_args[9] == "uploads/x.pdf"  # 스윕이 다시 쓸 원본 키
    assert insert_args[10] == "pending"


async def test_immediate_delete_success_marks_outbox_deleted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    outbox = _RecordingOutbox()
    _install_outbox(monkeypatch, outbox)
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("")
    )
    s3 = _RecordingS3()
    pipeline = _gate_pipeline(
        FakePool(existing=None), FakeProducer(), processor, s3, _RecordingCoverage(ratio=1.0)
    )

    # Act
    await pipeline.handle(_job())
    await pipeline.wait_for_pending_deletes()

    # Assert: 저장이 돌려준 id로 종결 기록 — 스윕이 같은 행을 다시 집지 않는다.
    assert s3.deleted == ["uploads/x.pdf"]
    assert outbox.successes == [str(_SAVE_ID)]
    assert outbox.failures == []


async def test_immediate_delete_failure_records_retry_with_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 즉시 삭제 실패는 SQS 재전달이 못 받는다(fire-and-forget) — 유일한
    # 재시도 경로가 outbox이므로, 실패가 설정된 상한·간격과 함께 기록돼야 한다.
    outbox = _RecordingOutbox()
    _install_outbox(monkeypatch, outbox)
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("")
    )
    s3 = _RecordingS3(delete_error=OcrError("S3 삭제 실패: uploads/x.pdf"))
    pipeline = _gate_pipeline(
        FakePool(existing=None),
        FakeProducer(),
        processor,
        s3,
        _RecordingCoverage(ratio=1.0),
        settings=Settings(ocr_delete_max_attempts=3, ocr_delete_retry_interval_seconds=60.0),
    )

    # Act
    with capture_logs() as logs:
        await pipeline.handle(_job())
        await pipeline.wait_for_pending_deletes()

    # Assert
    assert outbox.failures == [(str(_SAVE_ID), 3, 60.0)]
    assert outbox.successes == []
    assert "original_delete_failed" in [entry["event"] for entry in logs]  # 기존 로그 유지


async def test_outbox_update_failure_does_not_break_delete_flow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: outbox 갱신(DB)이 죽어도 삭제 로깅·task 정리는 그대로여야 한다 — 둘은
    # 독립이다. 기록 실패는 "스윕이 한 번 더 지운다"(멱등)로 흡수된다.
    outbox = _RecordingOutbox(error_on=str(_SAVE_ID))
    _install_outbox(monkeypatch, outbox)
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("")
    )
    s3 = _RecordingS3()
    pipeline = _gate_pipeline(
        FakePool(existing=None), FakeProducer(), processor, s3, _RecordingCoverage(ratio=1.0)
    )

    # Act
    with capture_logs() as logs:
        await pipeline.handle(_job())
        await pipeline.wait_for_pending_deletes()

    # Assert
    events = [entry["event"] for entry in logs]
    assert s3.deleted == ["uploads/x.pdf"]
    assert "original_deleted" in events  # 삭제 사실은 그대로 남는다
    assert "original_delete_outbox_update_failed" in events
    # 예외가 task 밖으로 새지 않는다(§9: 메시지 말고 타입만).
    assert "original_delete_task_error" not in events
    assert "DB 연결 끊김" not in json.dumps(logs, default=str)
    assert pipeline._pending_deletes == set()


async def test_sweep_retries_pending_rows_and_records_each_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 두 건 중 하나만 S3 삭제가 실패한다.
    outbox = _RecordingOutbox()
    due = _FakeDueDeletions(
        [
            _due("uploads/a.pdf", row_id="row-a"),
            _due("uploads/b.pdf", row_id="row-b", attempts=1),
        ]
    )
    _install_outbox(monkeypatch, outbox, due)
    s3 = _RecordingS3(delete_errors={"uploads/b.pdf": OcrError("S3 삭제 실패: uploads/b.pdf")})
    pipeline = _sweep_pipeline(
        s3, Settings(ocr_delete_max_attempts=5, ocr_delete_retry_interval_seconds=900.0)
    )

    # Act
    with capture_logs() as logs:
        swept = await pipeline.sweep_pending_deletes(batch_size=10)

    # Assert
    assert swept == 2  # 시도 건수(성공·실패 무관)
    assert due.limits == [10]
    assert s3.deleted == ["uploads/a.pdf", "uploads/b.pdf"]
    assert outbox.successes == ["row-a"]
    assert outbox.failures == [("row-b", 5, 900.0)]
    events = [entry["event"] for entry in logs]
    assert "sweep_delete_succeeded" in events
    assert "sweep_delete_failed" in events


async def test_sweep_row_error_does_not_block_remaining_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 첫 행에서 OcrError 방어선을 빠져나가는 예외가 난다. 배치가 통째로
    # 멈추면 그 뒤 행들은 다음 주기까지(15분) 방치된다.
    outbox = _RecordingOutbox()
    due = _FakeDueDeletions(
        [_due("uploads/a.pdf", row_id="row-a"), _due("uploads/b.pdf", row_id="row-b")]
    )
    _install_outbox(monkeypatch, outbox, due)
    s3 = _RecordingS3(delete_errors={"uploads/a.pdf": RuntimeError("boto3 내부 오류")})
    pipeline = _sweep_pipeline(s3)

    # Act
    with capture_logs() as logs:
        swept = await pipeline.sweep_pending_deletes()

    # Assert: 두 번째 행은 정상 처리된다.
    assert swept == 2
    assert outbox.successes == ["row-b"]
    row_errors = [entry for entry in logs if entry["event"] == "sweep_delete_error"]
    assert [entry["error_type"] for entry in row_errors] == ["RuntimeError"]
    assert "boto3 내부 오류" not in json.dumps(logs, default=str)  # §9 — 메시지 금지


async def test_sweep_logs_error_when_attempts_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 이번 실패로 상한(3)을 채우는 행 + 아직 여유가 있는 행.
    # 소진은 자동 재시도가 끝났다는 뜻이라 운영 개입 신호(error)여야 한다.
    # 판정은 **DB가 돌려준 갱신 후 상태**로 한다 — fetch 스냅샷(row.attempts)으로
    # 미리 계산하면 조회~UPDATE 사이 갱신을 놓쳐 로그와 DB가 어긋난다. 그래서 이
    # 테스트는 fetch 쪽 attempts를 **일부러 0으로 주고**(stale 흉내) outbox 쪽에만
    # 실제 누적치를 심는다 — 사전 계산으로 되돌리면 두 행 모두 warning이 돼 실패한다.
    outbox = _RecordingOutbox(attempts={"row-last": 2})
    due = _FakeDueDeletions(
        [
            _due("uploads/last.pdf", row_id="row-last", attempts=0),
            _due("uploads/more.pdf", row_id="row-more", attempts=0),
        ]
    )
    _install_outbox(monkeypatch, outbox, due)
    s3 = _RecordingS3(delete_error=OcrError("S3 삭제 실패"))
    pipeline = _sweep_pipeline(s3, Settings(ocr_delete_max_attempts=3))

    # Act
    with capture_logs() as logs:
        await pipeline.sweep_pending_deletes()

    # Assert
    failures = [entry for entry in logs if entry["event"] == "sweep_delete_failed"]
    exhausted = [entry for entry in failures if entry["ocr_result_id"] == "row-last"]
    retrying = [entry for entry in failures if entry["ocr_result_id"] == "row-more"]
    assert [(entry["log_level"], entry["exhausted"], entry["attempts"]) for entry in exhausted] == [
        ("error", True, 3)
    ]
    assert [(entry["log_level"], entry["exhausted"]) for entry in retrying] == [("warning", False)]


async def test_immediate_delete_logs_error_when_it_exhausts_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: max_attempts=1(재시도 비활성 목적으로 쓸 수 있는 설정)이면 **즉시 삭제
    # 실패 한 번이 그 자리에서 exhausted로 종결**시킨다. 그러면 스윕은 그 행을 다시
    # 조회하지 못하므로(pending이 아니다) 여기서 error를 안 남기면 운영 신호가 지연이
    # 아니라 **영구 소실**된다 — PII 원본이 S3에 고아로 남는데 아무도 모른다.
    outbox = _RecordingOutbox()
    _install_outbox(monkeypatch, outbox)
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("")
    )
    s3 = _RecordingS3(delete_error=OcrError("S3 삭제 실패: uploads/x.pdf"))
    pipeline = _gate_pipeline(
        FakePool(existing=None),
        FakeProducer(),
        processor,
        s3,
        _RecordingCoverage(ratio=1.0),
        settings=Settings(ocr_delete_max_attempts=1),
    )

    # Act
    with capture_logs() as logs:
        await pipeline.handle(_job())
        await pipeline.wait_for_pending_deletes()

    # Assert
    failed = [entry for entry in logs if entry["event"] == "original_delete_failed"]
    assert [(entry["log_level"], entry["exhausted"], entry["attempts"]) for entry in failed] == [
        ("error", True, 1)
    ]


async def test_immediate_delete_failure_below_limit_stays_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 상한에 여유가 있으면 스윕이 이어받으므로 경고면 충분하다(알림 노이즈 방지).
    outbox = _RecordingOutbox()
    _install_outbox(monkeypatch, outbox)
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("")
    )
    s3 = _RecordingS3(delete_error=OcrError("S3 삭제 실패: uploads/x.pdf"))
    pipeline = _gate_pipeline(
        FakePool(existing=None),
        FakeProducer(),
        processor,
        s3,
        _RecordingCoverage(ratio=1.0),
        settings=Settings(ocr_delete_max_attempts=5),
    )

    # Act
    with capture_logs() as logs:
        await pipeline.handle(_job())
        await pipeline.wait_for_pending_deletes()

    # Assert
    failed = [entry for entry in logs if entry["event"] == "original_delete_failed"]
    assert [(entry["log_level"], entry["exhausted"]) for entry in failed] == [("warning", False)]


async def test_delete_failure_log_falls_back_to_warning_when_outbox_update_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: outbox 갱신이 죽어 상태를 모르는 경우. 행은 아직 pending이라 스윕이
    # 다시 시도하므로 error가 아니라 warning이 맞다(정말 소진되면 그때 error가 뜬다).
    outbox = _RecordingOutbox(error_on=str(_SAVE_ID))
    _install_outbox(monkeypatch, outbox)
    processor = FakeProcessor(
        _multi_page_result("보험증권 계약자 홍길동"), engine=_FakeReocrEngine("")
    )
    s3 = _RecordingS3(delete_error=OcrError("S3 삭제 실패: uploads/x.pdf"))
    pipeline = _gate_pipeline(
        FakePool(existing=None),
        FakeProducer(),
        processor,
        s3,
        _RecordingCoverage(ratio=1.0),
        settings=Settings(ocr_delete_max_attempts=1),  # 상태를 알았다면 error였을 설정
    )

    # Act
    with capture_logs() as logs:
        await pipeline.handle(_job())
        await pipeline.wait_for_pending_deletes()

    # Assert
    failed = [entry for entry in logs if entry["event"] == "original_delete_failed"]
    assert [(entry["log_level"], entry["exhausted"], entry["attempts"]) for entry in failed] == [
        ("warning", False, None)
    ]
    assert "original_delete_outbox_update_failed" in [entry["event"] for entry in logs]


async def test_sweep_is_cancelled_mid_flight_without_recording_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 종료 중 스윕이 S3 왕복 도중 취소되는 경우. 어중간한 outbox 기록을
    # 남기면(성공도 실패도 아닌데 기록) 행이 종결되거나 attempts만 축나 다음 기동이
    # 재처리하지 못한다 — 아무것도 안 남기고 pending 그대로 두는 게 맞다.
    outbox = _RecordingOutbox()
    due = _FakeDueDeletions([_due("uploads/a.pdf", row_id="row-a")])
    _install_outbox(monkeypatch, outbox, due)
    gate = asyncio.Event()  # 열지 않는다 — 삭제를 S3 왕복 중 상태로 붙잡아 둔다.
    s3 = _RecordingS3(delete_gate=gate)
    pipeline = _sweep_pipeline(s3)

    # Act
    task = asyncio.create_task(pipeline.sweep_pending_deletes())
    await s3.delete_started.wait()  # 실제로 S3 호출에 들어간 뒤 취소한다
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Assert: 기록 없음 → 행은 pending으로 남아 다음 기동 스윕이 그대로 재처리한다.
    assert outbox.successes == [] and outbox.failures == []
    assert s3.deleted == []


async def test_sweep_is_noop_when_no_rows_are_due(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 대기열이 비면(정상 상태) S3를 건드리지도, 요약 로그를 남기지도 않는다.
    outbox = _RecordingOutbox()
    due = _FakeDueDeletions([])
    _install_outbox(monkeypatch, outbox, due)
    s3 = _RecordingS3()
    pipeline = _sweep_pipeline(s3)

    # Act
    with capture_logs() as logs:
        swept = await pipeline.sweep_pending_deletes()

    # Assert
    assert swept == 0
    assert s3.events == []
    assert outbox.successes == [] and outbox.failures == []
    assert "original_delete_sweep" not in [entry["event"] for entry in logs]


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


# ── 실패 저널(ai.ocr_job_failures) ───────────────────────────────
# 저널이 붙기 전엔 위 fail-closed 격리가 **아무 기록도 남기지 않았다** — 사용자에겐
# 업로드가 조용히 증발하는 무음 실패였다. 아래 테스트는 세 경로(결정적·일시·회복)와
# "저널이 원래 예외를 가리지 않는다"는 규칙을 고정한다.
# 업서트 인자 순서는 test_repository가 고정하므로 여기서는 분류·terminal·호출 여부만 본다.
_JOURNAL_FAILURE_CLASS_ARG = 8  # $9 failure_class
_JOURNAL_ERROR_TYPE_ARG = 9  # $10 error_type
_JOURNAL_TERMINAL_ARG = 10  # $11 terminal


def _journal_entry(pool: FakePool) -> tuple[str, str, bool]:
    """저널 쓰기 1건에서 (failure_class, error_type, terminal)을 뽑는다."""
    calls = pool.journal_calls()
    assert len(calls) == 1, f"저널 쓰기 1건이어야 한다: {len(calls)}건"
    args = calls[0][1]
    return (
        args[_JOURNAL_FAILURE_CLASS_ARG],
        args[_JOURNAL_ERROR_TYPE_ARG],
        args[_JOURNAL_TERMINAL_ARG],
    )


def _context_chain(exc: BaseException) -> list[BaseException]:
    """``__context__``를 따라간 예외 체인(자기 자신 제외)."""
    chain: list[BaseException] = []
    current = exc.__context__
    while current is not None and current not in chain:
        chain.append(current)
        current = current.__context__
    return chain


class _RaisingProcessor:
    """``process_with_images``가 정해진 예외를 던지는 프로세서 대역."""

    def __init__(self, error: Exception) -> None:
        self._error = error
        self.engine = None

    async def process_with_images(
        self, s3_key: str, content_type: str
    ) -> tuple[OcrResult, list[object]]:
        raise self._error


async def test_masking_residual_is_journaled_as_terminal_then_reraised() -> None:
    # Arrange: 마스킹을 안 하는 마스커 → 주민번호 잔류 → MaskingError(결정적).
    pool = FakePool(existing=None)
    processor = FakeProcessor(_result("홍길동 901010-1234567"))
    pipeline = _pipeline(pool, FakeProducer(), processor, masker=_IdentityMasker())

    # Act / Assert: 원래 예외를 그대로 올려야 컨슈머가 즉시 ack할 수 있다.
    with pytest.raises(MaskingError):
        await pipeline.handle(_job())

    # 예외가 나가기 **전에** 기록이 끝나 있어야 한다 — 컨슈머가 곧바로 메시지를 지우므로
    # 이 순서가 깨지면 저널에도 큐에도 아무것도 남지 않는다.
    assert _journal_entry(pool) == ("masking_residual", "MaskingError", True)


async def test_unreadable_file_is_journaled_as_terminal_not_transient_ocr_error() -> None:
    # UnreadableFileError는 OcrError이기도 하다. except 순서가 뒤집혀 일반 OcrError로
    # 잡히면 확정 실패가 terminal=False로 남고 컨슈머는 재전달을 반복한다.
    pool = FakePool(existing=None)
    pipeline = _pipeline(
        pool, FakeProducer(), _RaisingProcessor(UnreadableFileError("PDF 렌더 실패"))
    )

    with pytest.raises(UnreadableFileError):
        await pipeline.handle(_job())

    assert _journal_entry(pool) == ("unreadable_file", "UnreadableFileError", True)


async def test_terminal_journal_failure_raises_retryable_error_instead() -> None:
    # Arrange: 저널 DB가 죽었다. 원래 예외(NonRetryable)를 그대로 올리면 컨슈머가
    # 메시지를 지워버려 기록도 메시지도 없는 무음 유실이 된다.
    pool = FakePool(existing=None, execute_error=RuntimeError("저널 DB 다운"))
    processor = FakeProcessor(_result("홍길동 901010-1234567"))
    pipeline = _pipeline(pool, FakeProducer(), processor, masker=_IdentityMasker())

    # Act
    # MaskingError는 RuntimeError가 아니므로, 변환이 사라지면 여기서 바로 깨진다.
    with pytest.raises(RuntimeError) as exc_info:
        await pipeline.handle(_job())

    # Assert: 재시도 가능한 예외로 바뀌어 나간다 → 컨슈머가 삭제하지 않고 재전달한다.
    # (변환 예외에 마커를 달아버리는 회귀도 막는다 — 그러면 다시 즉시 ack된다.)
    assert not isinstance(exc_info.value, NonRetryableError)
    # 원인은 유실되지 않는다 — 저널 예외가 직접 원인이고, 그 뒤로 원래 결정적 실패가
    # 예외 체인에 남아 트레이스백만으로 "무엇을 기록하려다 실패했는가"가 읽힌다.
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert any(isinstance(exc, MaskingError) for exc in _context_chain(exc_info.value))


async def test_transient_ocr_error_is_journaled_as_non_terminal() -> None:
    # 일시 실패는 재전달로 회복될 수 있다 — terminal로 굳히면 사용자에게 없는 확정
    # 실패를 노출하게 된다.
    pool = FakePool(existing=None)
    pipeline = _pipeline(pool, FakeProducer(), _RaisingProcessor(OcrError("S3 다운로드 실패")))

    with pytest.raises(OcrError):
        await pipeline.handle(_job())

    assert _journal_entry(pool) == ("ocr_error", "OcrError", False)


async def test_unclassified_transient_error_falls_back_to_unknown() -> None:
    # 분류 못 하는 예외도 기록은 남긴다 — CHECK 제약을 어겨 기록 자체가 실패하면
    # 원래 없애려던 무음 실패로 되돌아간다.
    pool = FakePool(existing=None)
    pipeline = _pipeline(pool, FakeProducer(), _RaisingProcessor(ValueError("예상 못한 오류")))

    with pytest.raises(ValueError):
        await pipeline.handle(_job())

    assert _journal_entry(pool) == ("unknown", "ValueError", False)


async def test_transient_journal_failure_does_not_mask_original_error() -> None:
    # 일시 실패 경로에선 저널 실패를 삼킨다 — 원래 예외가 이미 재전달을 유발하므로
    # 저널 예외로 원인을 가리면 진단만 어려워진다.
    pool = FakePool(existing=None, execute_error=RuntimeError("저널 DB 다운"))
    pipeline = _pipeline(pool, FakeProducer(), _RaisingProcessor(OcrError("S3 다운로드 실패")))

    # RuntimeError(저널 실패)가 아니라 원래 예외가 나와야 한다.
    with capture_logs() as logs, pytest.raises(OcrError):
        await pipeline.handle(_job())

    warned = [e for e in logs if e["event"] == "ocr_job_failure_journal_failed"]
    assert len(warned) == 1
    assert warned[0]["error_type"] == "RuntimeError"  # 예외는 타입만 남긴다(§9)
    assert warned[0]["original_error_type"] == "OcrError"


async def test_success_clears_failure_journal() -> None:
    # 이전 시도가 남긴 일시 실패 행을 지운다. 남겨두면 이미 처리된 작업이 계속
    # "실패"로 조회된다.
    pool = FakePool(existing=None)
    producer = FakeProducer()
    pipeline = _pipeline(pool, producer, FakeProcessor(_result("보험증권", "증권번호 202301-042")))

    await pipeline.handle(_job())

    cleared = pool.journal_calls()
    assert len(cleared) == 1
    assert cleared[0][0].strip().startswith("DELETE FROM ai.ocr_job_failures")
    assert cleared[0][1] == (uuid.UUID(_JOB_ID),)
    assert len(producer.published) == 1  # 정리는 발행을 막지 않는다


async def test_idempotent_short_circuit_clears_failure_journal() -> None:
    # 저장은 됐는데 발행 직전에 죽어 실패로 기록된 작업이 재전달로 여기 도달한다 —
    # 재발행으로 끝났으니 저널도 함께 정리해야 한다.
    pool = FakePool(existing={"id": _EXISTING_ID, "doc_type": "policy", "ocr_quality": "ok"})
    pipeline = _pipeline(pool, FakeProducer(), FakeProcessor(_result("무시됨")))

    await pipeline.handle(_job())

    cleared = pool.journal_calls()
    assert len(cleared) == 1
    assert cleared[0][0].strip().startswith("DELETE FROM ai.ocr_job_failures")


async def test_clear_failure_error_does_not_fail_the_job() -> None:
    # 저장·발행이 끝난 작업을 남은 저널 행 하나 때문에 실패로 뒤집지 않는다.
    pool = FakePool(existing=None, execute_error=RuntimeError("저널 DB 다운"))
    producer = FakeProducer()
    pipeline = _pipeline(pool, producer, FakeProcessor(_result("보험증권", "증권번호 202301-042")))

    with capture_logs() as logs:
        await pipeline.handle(_job())  # 예외 없음

    assert len(producer.published) == 1
    warned = [e for e in logs if e["event"] == "ocr_job_failure_clear_failed"]
    assert [e["error_type"] for e in warned] == ["RuntimeError"]
