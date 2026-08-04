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
        # 이미지 내용 자체는 이미지 트랙 페이크가 무시하지만, _extract_pages가
        # images/pages를 zip(strict=True)하므로 개수는 페이지 수와 맞춰야 한다.
        return self._result, [object() for _ in self._result.pages]


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
    _, report, _ = producer.published[0]
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
    processor = FakeProcessor(
        _result("보험증권", "증권번호 202301-042", confidence=0.5)
    )
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
    processor = FakeProcessor(
        _result("보험증권", "증권번호 202301-042", confidence=0.95)
    )
    pipeline = _pipeline(pool, producer, processor)

    # Act
    await pipeline.handle(_job())

    # Assert
    insert_args = pool.insert_calls()[0][1]
    assert insert_args[8] == "ok"


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
