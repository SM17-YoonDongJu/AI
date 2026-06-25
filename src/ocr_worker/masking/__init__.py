"""PII 마스킹 (이슈 #18) — 정규식 + NER 합집합.

노션 "마스킹 전략" A안(텍스트 마스킹) 구현. 공개 API:

- ``mask(text) -> str``      : 기본 진입점(정규식 + USE_NER 시 NER).
- ``Masker``                 : 디텍터 주입·재사용용 클래스.
- ``Detector``               : 디텍터 프로토콜(교체 가능 보장).
- ``RegexDetector`` / ``NerDetector`` : 개별 디텍터.
- ``Span`` / ``PiiLabel``    : 탐지 결과(이미지 마스킹 B안이 bbox 매핑에 사용).
- ``find_residual_pii`` / ``assert_no_residual`` / ``MaskingError`` : 검증(Tier 1).
"""

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
    "Masker",
    "MaskingError",
    "NerDetector",
    "PiiLabel",
    "RegexDetector",
    "Span",
    "apply_mask",
    "assert_no_residual",
    "find_residual_pii",
    "get_masker",
    "mask",
    "merge_overlaps",
]
