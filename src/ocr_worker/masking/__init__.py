"""PII 마스킹 (이슈 #18) — 정규식 + NER 합집합.

노션 "마스킹 전략" A안(텍스트 마스킹) 구현. 공개 API:

- ``mask(text) -> str``      : 기본 진입점(정규식 + USE_NER 시 NER).
- ``Masker``                 : 디텍터 주입·재사용용 클래스.
- ``Detector``               : 디텍터 프로토콜(교체 가능 보장).
- ``RegexDetector`` / ``NerDetector`` : 개별 디텍터.
- ``Span`` / ``PiiLabel``    : 탐지 결과(이미지 마스킹 B안이 bbox 매핑에 사용).
- ``find_residual_pii`` / ``assert_no_residual`` / ``MaskingError`` : 텍스트 검증(Tier 1).
- ``ImageMasker`` / ``pii_line_indices`` : 이미지 마스킹(줄 단위 검은블럭 비식별 사본).
- ``verify_redacted_page`` / ``VerificationResult`` : 이미지 마스킹 검증(5.1 커버리지 + 5.2 재OCR).
"""

from ocr_worker.masking.image_masker import (
    ImageMasker,
    image_to_png_bytes,
    pii_line_indices,
    redact_page_image,
)
from ocr_worker.masking.image_verify import (
    VerificationResult,
    decide,
    measure_coverage,
    reocr_residual_pii,
    verify_redacted_page,
)
from ocr_worker.masking.masker import Detector, Masker, get_masker, mask
from ocr_worker.masking.ner_detector import NerDetector
from ocr_worker.masking.regex_detector import RegexDetector
from ocr_worker.masking.spans import PiiLabel, Span, apply_mask, merge_overlaps
from ocr_worker.masking.verify import (
    MaskingError,
    assert_no_residual,
    find_residual_pii,
)

__all__ = [
    "Detector",
    "ImageMasker",
    "Masker",
    "MaskingError",
    "NerDetector",
    "PiiLabel",
    "RegexDetector",
    "Span",
    "VerificationResult",
    "apply_mask",
    "assert_no_residual",
    "decide",
    "find_residual_pii",
    "get_masker",
    "image_to_png_bytes",
    "mask",
    "measure_coverage",
    "merge_overlaps",
    "pii_line_indices",
    "redact_page_image",
    "reocr_residual_pii",
    "verify_redacted_page",
]
