"""OCR 워커 파이프라인 (이슈 #15) — consume→OCR→분류→추출→마스킹→저장→produce.

``KafkaConsumer``가 역직렬화·검증한 ``OcrJob`` 하나를 받아 전 과정을 오케스트레이션하고
``ReportJob``을 발행하는 **핸들러 본체**다. 순수 로직(분류·추출·마스킹)은 각 모듈에
있고, 여기서는 I/O 경계(S3·OCR·DB·Kafka)를 잇는 오케스트레이션만 담당한다.

정식 at-least-once 규약(계획 · [[ocr-worker-build]]):
- **멱등(job_id)**: 진입부에서 이미 저장된 작업이면 무거운 OCR을 건너뛰고 기존
  ``ocr_result_id``로 ``ReportJob``만 재발행한다. 저장은 ``job_id`` 업서트라 중복
  소비가 새 행을 만들지 않는다. 다운스트림은 ``ocr_result_id``로 멱등 처리한다.
- **수동 커밋(발행 후)**: 커밋은 컨슈머가 핸들러 성공 후 수행한다. 그래서 저장~발행
  사이 crash 시 메시지가 재전달되고, 재발행이 안전해야 한다 → 위 멱등 단락 + 결정적
  ``report_id``(``ocr_result_id`` 파생)로 완전 멱등을 만든다.
- **DLQ**: 핸들러가 예외를 던지면 컨슈머가 재시도 후 DLQ로 보낸다. 마스킹 잔류 검증
  실패(``MaskingError``)도 예외로 전파해 **PII를 저장하지 않고** 격리한다(fail-closed).

블로킹 격리(CODE_CONVENTIONS §7): OCR·이미지 렌더는 ``ocr.py``가, PII 마스킹(정규식·
NER)과 이미지 검은블럭(PIL)은 이 모듈이 ``asyncio.to_thread``로 이벤트 루프에서 뗀다.

PII 규약(§13): OCR 원문(평문)은 이 핸들러 메모리에만 존재하고, ``ocr_results``·로그·
``report-job``에는 마스킹본·비-PII 값·식별자만 나간다.
"""

import asyncio
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import asyncpg

from core.config import Settings, get_settings
from core.contracts import DocType, OcrJob, ReportJob
from core.kafka.producer import KafkaProducer
from core.logging import bind_context, clear_context, get_logger
from ocr_worker.classify import classify
from ocr_worker.extract import extract
from ocr_worker.masking.image_masker import ImageMasker, image_to_png_bytes
from ocr_worker.masking.masker import Masker, get_masker
from ocr_worker.masking.spans import PiiLabel
from ocr_worker.masking.verify import MaskingError, assert_no_residual
from ocr_worker.ocr import OcrProcessor, OcrResult, PageImage, get_processor
from ocr_worker.repository import (
    OcrResultRecord,
    build_masked_lines,
    find_ocr_result,
    save_ocr_result,
)
from ocr_worker.storage import put_object
from ocr_worker.vlm_client import VlmClientError, transcribe_table

logger = get_logger(__name__)

# 결정적 report_id 파생용 네임스페이스(같은 ocr_result_id → 같은 report_id → 완전 멱등).
_REPORT_ID_NAMESPACE = uuid.NAMESPACE_URL
# 비식별 이미지 사본 키 공간 — 원본 키와 분리(원본은 별도 보존정책).
_MASKED_IMAGE_CONTENT_TYPE = "image/png"

# 하이브리드 VLM 보조 대상 — surya 라인 순서가 표 행/열 구조를 보존 못하는 다중
# 항목 표 문서 유형만. 단순 라벨-값 문서(진단서·증권)는 surya만으로 충분해 제외한다.
_TABLE_DOC_TYPES: frozenset[DocType] = frozenset(
    {DocType.PAYOUT_NOTICE, DocType.CLAIM, DocType.HOSPITALIZATION_CERT, DocType.MEDICAL_RECEIPT}
)

# 표 문서와 별개로, surya 신뢰도가 낮으면(실측: 클린 PDF 0.951+ vs 실제 폰카 사진
# 0.861-) 문서 유형 무관하게 VLM 보완을 시도한다. 표본 3건 기준 시작값 — 운영 중 재조정.
_LOW_CONFIDENCE_THRESHOLD: float = 0.90

# VLM 결과 groundedness(환각 여부 대략 검사)용 토큰 추출·임계값. surya가 오타를 내도
# 대부분의 토큰(날짜·숫자·구조어)은 겹치므로 낮게 잡아 정상 교정까지 걸리지 않게 한다.
_TOKEN_RE = re.compile(r"[가-힣A-Za-z0-9]{2,}")
_MIN_GROUNDED_OVERLAP = 0.3

# 다중 페이지 문서에서 일부 페이지만 VLM이 채택됐을 때 entities.table_markdown을
# 페이지별로 구분해 이어붙이는 구분자(사람이 읽기 쉽게). masked_text/quality_source_text
# 는 OcrResult.full_text와 같은 "\n" 접합을 그대로 써서 문서 형태를 일관되게 유지한다.
_TABLE_PAGE_SEPARATOR = "\n\n---\n\n"


def _looks_grounded(vlm_text: str, source_text: str) -> bool:
    """VLM 결과가 surya 원문과 무관한 내용을 지어낸 게 아닌지 대략적으로 검사한다.

    ``assert_no_residual``은 PII 유출만 잡고 환각(이미지에 없는 내용 생성)은 못 잡으므로
    별도 안전장치로 둔다. 완벽한 검증은 아니다 — 토큰 중복률 기반 근사치.
    """
    vlm_tokens = set(_TOKEN_RE.findall(vlm_text))
    if not vlm_tokens:
        return True  # 빈 결과는 마스킹 검증 등 다른 경로에서 처리
    source_tokens = set(_TOKEN_RE.findall(source_text))
    overlap = len(vlm_tokens & source_tokens) / len(vlm_tokens)
    return overlap >= _MIN_GROUNDED_OVERLAP


def _missing_domain_info(entities: dict[str, object]) -> bool:
    """extract()(+VLM table_markdown)로 뽑힌 값이 하나도 없는지 확인한다.

    "재확인 필요" 판정 신호 중 하나. doc_type별 필수 필드 정책을 새로 만들지 않고
    이미 있는 추출 결과를 재사용한다 — 알려진 한계: entities 필드가 1개뿐인
    DIAGNOSIS·CLAIM은 그 필드가 원래 없는 양식이어도 오탐할 수 있다.
    """
    if not entities:
        return True
    return all(v is None for v in entities.values())


def _masked_image_key(job_id: str, page_index: int) -> str:
    """비식별 이미지 사본의 S3 키(페이지별). 원본과 분리된 키 공간을 쓴다."""
    return f"masked/{job_id}/page-{page_index}.png"


def _derive_report_id(ocr_result_id: str) -> str:
    """``ocr_result_id``에서 결정적 ``report_id``를 파생한다(재발행 시 동일 값)."""
    return str(uuid.uuid5(_REPORT_ID_NAMESPACE, f"report:{ocr_result_id}"))


# 이미지 마스킹 트랙 주입점 — (job, OCR결과, 페이지 이미지) → 페이지별 S3 키.
type ImagePipeline = Callable[[OcrJob, OcrResult, list[PageImage]], Awaitable[list[str]]]
# S3 업로드 주입점(테스트에서 페이크).
type UploadFn = Callable[[str, bytes, str], Awaitable[None]]
# 하이브리드 VLM 표 전사 주입점(테스트에서 페이크로 대체).
type VlmTranscribe = Callable[[PageImage], Awaitable[str]]


@dataclass(frozen=True, slots=True)
class _Analysis:
    """CPU 바운드 분석(분류·추출·텍스트 마스킹) 산출 — 한 번의 스레드 호출로 묶는다."""

    doc_type: DocType
    doc_type_confidence: float
    entities: dict[str, object]
    masked_text: str
    masked_lines: list[dict[str, object]]


class OcrPipeline:
    """``OcrJob`` 한 건을 처리해 ``ReportJob``을 발행하는 파이프라인 핸들러.

    I/O 경계(OCR 프로세서·마스커·이미지 마스킹·업로드·프로듀서·DB 풀)를 주입 가능하게
    두어, Kafka·DB·GPU·PIL 없이 오케스트레이션을 단위 테스트한다(#20).
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        producer: KafkaProducer,
        settings: Settings | None = None,
        processor: OcrProcessor | None = None,
        masker: Masker | None = None,
        image_pipeline: ImagePipeline | None = None,
        vlm_transcribe: VlmTranscribe | None = None,
    ) -> None:
        """파이프라인을 구성한다.

        Args:
            pool: asyncpg 연결 풀(``ocr_results`` 저장·조회).
            producer: ``ReportJob`` 발행용 Kafka 프로듀서(시작된 상태).
            settings: 환경설정. ``None``이면 ``get_settings()``.
            processor: OCR 오케스트레이터. ``None``이면 프로세스 공용 ``get_processor()``.
            masker: 텍스트 마스커. ``None``이면 프로세스 공용 ``get_masker()``.
            image_pipeline: 이미지 마스킹 트랙. ``None``이면 기본(PIL 렌더+S3 업로드).
            vlm_transcribe: 하이브리드 VLM 표 전사 함수. ``None``이면
                ``vlm_client.transcribe_table``.
        """
        self._pool = pool
        self._producer = producer
        self._settings = settings or get_settings()
        self._processor = processor or get_processor()
        self._masker = masker or get_masker()
        self._image_masker = ImageMasker(detect=self._masker.detect)
        self._vlm_transcribe: VlmTranscribe = vlm_transcribe or transcribe_table
        self._upload: UploadFn = put_object
        self._image_pipeline: ImagePipeline = image_pipeline or self._default_image_pipeline

    async def handle(self, job: OcrJob) -> None:
        """OCR 작업 하나를 처리한다(컨슈머 핸들러 진입점).

        멱등: 이미 저장된 ``job_id``면 OCR을 건너뛰고 ``ReportJob``만 재발행한다.
        예외는 그대로 전파해 컨슈머의 재시도/DLQ 규약에 맡긴다(핸들러가 삼키지 않는다).

        Args:
            job: 검증된 ``OcrJob``.
        """
        bind_context(job_id=job.job_id, user_ref=job.user_ref)
        try:
            existing = await find_ocr_result(self._pool, job.job_id)
            if existing is not None:
                ocr_result_id, doc_type, ocr_quality = existing
                logger.info("job already processed → republish", ocr_result_id=ocr_result_id)
                await self._publish_report(job, ocr_result_id, doc_type, ocr_quality)
                return
            await self._process(job)
        finally:
            clear_context()

    async def _process(self, job: OcrJob) -> None:
        """신규 작업의 전체 처리 흐름(OCR→분석→하이브리드 VLM→품질 판정→저장→발행)."""
        result, images = await self._processor.process_with_images(job.s3_key, job.content_type)

        # 분류·추출·텍스트 마스킹은 CPU 바운드(정규식·NER) → 한 스레드 호출로 격리한다.
        # surya 기반 masked_text는 자체 게이트(assert_no_residual)를 통과한 안전한
        # 값이라, 아래 VLM 경로가 실패해도 항상 되돌아갈 수 있는 폴백으로 유지한다.
        analysis = await asyncio.to_thread(self._analyze, result, job.doc_type_hint)

        # 표 문서 유형(다중 항목) 또는 surya 신뢰도가 낮으면(문서 유형 무관) VLM으로
        # 보완한다. 페이지마다 독립적으로 채택 여부가 갈린다 — 한 페이지가 실패해도
        # 그 페이지만 surya 결과로 폴백하고 나머지 페이지는 영향받지 않는다(과거엔
        # 1페이지 VLM 성공만으로 문서 전체 masked_text가 교체돼 2페이지 이후가 통째로
        # 유실됐었다). masked_lines(이미지 검은블록 bbox)는 VLM이 좌표를 안 주므로
        # 항상 surya 기반 그대로 둔다.
        masked_text = analysis.masked_text
        entities = analysis.entities
        quality_source_text = result.full_text  # 최종 품질 판정 기준 원문(기본 surya)

        needs_vlm = (
            analysis.doc_type in _TABLE_DOC_TYPES
            or result.mean_confidence < _LOW_CONFIDENCE_THRESHOLD
        )
        if needs_vlm and images:
            vlm_outcome = await self._extract_pages(images, result)
            if vlm_outcome is not None:
                masked_text, quality_source_text, table_markdown = vlm_outcome
                entities = {**entities, "table_markdown": table_markdown}

        # "재확인 필요" 판정: surya 신뢰도가 낮은데(문서 자체가 저품질) 사람 이름조차
        # 못 찾았거나 도메인 정보가 하나도 없으면, 자동 처리로는 신뢰할 수 없다고 본다.
        has_name = any(
            span.label is PiiLabel.NAME for span in self._masker.detect(quality_source_text)
        )
        ocr_quality = (
            "needs_reupload"
            if result.mean_confidence < _LOW_CONFIDENCE_THRESHOLD
            and (not has_name or _missing_domain_info(entities))
            else "ok"
        )

        # 이미지 마스킹은 렌더한 페이지 이미지를 재사용한다(재다운로드/재렌더 없음).
        image_keys = await self._image_pipeline(job, result, images)

        record = OcrResultRecord(
            job_id=job.job_id,
            doc_type=analysis.doc_type,
            doc_type_confidence=analysis.doc_type_confidence,
            ocr_confidence=result.mean_confidence,
            masked_text=masked_text,
            masked_lines=analysis.masked_lines,
            entities=entities,
            masked_image_s3_keys=image_keys,
            ocr_quality=ocr_quality,
        )
        ocr_result_id = await save_ocr_result(self._pool, record)
        await self._publish_report(job, ocr_result_id, analysis.doc_type, ocr_quality)

    async def _extract_pages(
        self, images: list[PageImage], result: OcrResult
    ) -> tuple[str, str, str] | None:
        """페이지마다 독립적으로 VLM 보완을 시도해 문서 전체를 합성한다.

        한 페이지의 VLM 실패(호출 오류·환각·잔류PII)가 다른 페이지까지 막지 않는다.
        실패한 페이지는 그 페이지의 surya 결과로 폴백해 문서 전체 내용이 유실되지
        않게 한다. masked_text/원문은 ``OcrResult.full_text``와 같은 "\\n" 접합을
        써서 VLM 관여 여부와 무관하게 문서 형태가 일관되게 유지된다.

        Returns:
            어느 페이지든 VLM이 채택됐으면 ``(합성 masked_text, 합성 원문,
            table_markdown)``. 전 페이지가 실패했으면 ``None``(호출측이 순수
            surya 결과를 그대로 쓰게 한다).
        """
        masked_parts: list[str] = []
        raw_parts: list[str] = []
        table_parts: list[str] = []
        adopted = False

        for image, page in zip(images, result.pages, strict=True):
            outcome = await self._extract_table(image, page.text)
            if outcome is not None:
                raw, masked = outcome
                masked_parts.append(masked)
                raw_parts.append(raw)
                table_parts.append(masked)
                adopted = True
            else:
                masked_parts.append(await asyncio.to_thread(self._masker.mask, page.text))
                raw_parts.append(page.text)

        if not adopted:
            return None
        return (
            "\n".join(masked_parts),
            "\n".join(raw_parts),
            _TABLE_PAGE_SEPARATOR.join(table_parts),
        )

    async def _extract_table(
        self, image: PageImage, source_text: str
    ) -> tuple[str, str] | None:
        """VLM으로 표를 전사하고 검증·마스킹한다.

        실패(호출 오류·환각·잔류PII)하면 ``None``을 반환해 surya 결과로 폴백한다.

        Returns:
            성공 시 ``(VLM 원문, 마스킹된 텍스트)``. 원문은 호출측의 품질 판정(이름
            스팬 확인)에도 재사용한다.
        """
        try:
            raw = await self._vlm_transcribe(image)
        except VlmClientError:
            logger.warning("vlm_table_extraction_failed")
            return None
        if not _looks_grounded(raw, source_text):
            logger.warning("vlm_table_ungrounded")
            return None
        masked = await asyncio.to_thread(self._mask_table, raw)
        if masked is None:
            return None
        return raw, masked

    def _mask_table(self, raw: str) -> str | None:
        """VLM 원문을 마스킹한다. 잔류 고민감 PII가 있으면 ``None``(surya 결과로 폴백)."""
        masked = self._masker.mask(raw)
        try:
            assert_no_residual(masked)
        except MaskingError:
            logger.warning("vlm_table_masking_residual_pii")
            return None
        return masked

    def _analyze(self, result: OcrResult, hint: str | None) -> _Analysis:
        """분류·추출·텍스트 마스킹을 수행한다(순수/CPU — ``to_thread``에서 호출).

        마스킹 후 ``assert_no_residual``로 고민감 PII 잔류를 확인한다. 잔류 시
        ``MaskingError``를 던져 **PII를 저장하지 않고** DLQ로 보낸다(fail-closed).
        """
        classification = classify(result.first_page_text, hint)
        entities = extract(classification.doc_type, result.full_text)
        masked_text = self._masker.mask(result.full_text)
        assert_no_residual(masked_text)  # 고민감 PII 잔류 시 MaskingError(전파 → DLQ)
        masked_lines = build_masked_lines(result, self._masker.mask, self._masker.detect)
        return _Analysis(
            doc_type=classification.doc_type,
            doc_type_confidence=classification.confidence,
            entities=entities,
            masked_text=masked_text,
            masked_lines=masked_lines,
        )

    async def _default_image_pipeline(
        self, job: OcrJob, result: OcrResult, images: list[PageImage]
    ) -> list[str]:
        """PII 라인을 가린 비식별 페이지 사본을 만들어 S3에 올리고 키를 돌려준다.

        검은블럭 렌더·PNG 인코딩은 CPU/PIL 바운드 → ``to_thread``로 격리한다. 업로드는
        페이지별 독립 I/O라 ``gather``로 병렬화한다. 페이지가 없으면 빈 목록.
        """
        if not images:
            return []
        pngs = await asyncio.to_thread(self._redact_to_png, result, images)
        keys = [_masked_image_key(job.job_id, index) for index in range(len(pngs))]
        await asyncio.gather(
            *(
                self._upload(key, png, _MASKED_IMAGE_CONTENT_TYPE)
                for key, png in zip(keys, pngs, strict=True)
            )
        )
        logger.info("masked_images_uploaded", pages=len(keys))
        return keys

    def _redact_to_png(self, result: OcrResult, images: list[PageImage]) -> list[bytes]:
        """페이지별 검은블럭 사본을 PNG 바이트로 인코딩한다(동기 — PIL, ``to_thread``용)."""
        redacted = self._image_masker.redact_pages(images, result)
        return [image_to_png_bytes(image) for image in redacted]

    async def _publish_report(
        self, job: OcrJob, ocr_result_id: str, doc_type: DocType, ocr_quality: str
    ) -> None:
        """``ReportJob``을 ``report-job`` 토픽에 발행한다(파티션 키 = report_id).

        ``report_id``는 ``ocr_result_id``에서 결정적으로 파생해 재발행 시 동일하다 —
        report_worker가 ``report_id``/``ocr_result_id`` 어느 쪽으로 멱등 처리해도 안전하다.
        ``claim_id``는 가공 없이 패스스루한다(USER_CLAIMS는 report_worker가 직접 읽음).
        ``ocr_quality``가 ``needs_reupload``면 report_worker가 리포트 생성을 건너뛸 신호다
        (report_worker·게이트웨이 쪽 소비는 이번 범위 밖).
        """
        report_id = _derive_report_id(ocr_result_id)
        report = ReportJob(
            report_id=report_id,
            ocr_result_id=ocr_result_id,
            job_id=job.job_id,
            doc_type=doc_type,
            user_ref=job.user_ref,
            claim_id=job.claim_id,
            created_at=datetime.now(UTC),
            ocr_quality=ocr_quality,
        )
        await self._producer.publish(self._settings.kafka_report_job_topic, report, key=report_id)
