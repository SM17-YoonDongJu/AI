"""마스킹 오케스트레이션 — 정규식 + (옵션)NER 합집합.

노션 §4 인터페이스 그대로:

    spans = regex_detector.detect(text)
    if USE_NER:
        spans += ner_detector.detect(text)   # 합집합(OR)
    return apply_mask(text, merge_overlaps(spans))

``Detector`` 프로토콜을 공유하므로 NER 모델 교체·LLM 디텍터 추가에도 ``mask()``는
불변이다. NER은 무거우니 ``USE_NER``이고 실제로 필요한 첫 호출에서만 lazy 로드한다.
"""

from typing import Protocol, runtime_checkable

from core.config import get_settings
from ocr_worker.masking.regex_detector import RegexDetector
from ocr_worker.masking.spans import Span, apply_mask


@runtime_checkable
class Detector(Protocol):
    """PII 디텍터 인터페이스 — 텍스트 → 스팬 목록(노션 §4)."""

    def detect(self, text: str) -> list[Span]: ...


class Masker:
    """정규식 + (옵션)NER 디텍터를 합쳐 텍스트를 마스킹한다.

    스레드/태스크 간 재사용 가능하도록 디텍터는 상태를 두지 않거나(정규식),
    내부적으로만 lazy 캐시한다(NER 파이프라인).
    """

    def __init__(
        self,
        *,
        use_ner: bool | None = None,
        regex_detector: Detector | None = None,
        ner_detector: Detector | None = None,
    ) -> None:
        """마스커를 구성한다.

        Args:
            use_ner: NER 사용 여부. ``None``이면 ``settings.use_ner``를 따른다.
            regex_detector: 주입용(테스트). 기본은 ``RegexDetector``.
            ner_detector: 주입용(테스트). 기본은 첫 사용 시 lazy 생성.
        """
        settings = get_settings()
        self._use_ner = settings.use_ner if use_ner is None else use_ner
        self._regex: Detector = regex_detector or RegexDetector()
        self._ner = ner_detector

    def detect(self, text: str) -> list[Span]:
        """정규식 + (활성 시)NER 스팬의 합집합을 반환한다(병합 전).

        이미지 마스킹(노션 B안)은 이 스팬을 OCR bbox에 매핑하므로, 텍스트
        치환과 좌표 마스킹이 동일한 1회 탐지를 공유한다.
        """
        spans = self._regex.detect(text)
        if self._use_ner:
            spans = spans + self._ner_detector().detect(text)
        return spans

    def mask(self, text: str) -> str:
        """PII를 ``[분류]`` 토큰으로 치환한 텍스트를 반환한다(순수 str→str).

        Args:
            text: OCR로 추출된 원본 텍스트.

        Returns:
            마스킹된 텍스트.
        """
        return apply_mask(text, self.detect(text))

    def _ner_detector(self) -> Detector:
        """NER 디텍터를 lazy 생성한다(transformers/torch는 이때 import)."""
        if self._ner is None:
            from ocr_worker.masking.ner_detector import NerDetector

            settings = get_settings()
            self._ner = NerDetector(settings.ner_model, settings.ner_score_threshold)
        return self._ner


# ── 모듈 전역 기본 인스턴스 + 편의 함수 (노션 A안: mask(text)->str) ──
_default_masker: Masker | None = None


def get_masker() -> Masker:
    """프로세스 1회 생성되는 기본 ``Masker``를 반환한다."""
    global _default_masker
    if _default_masker is None:
        _default_masker = Masker()
    return _default_masker


def mask(text: str) -> str:
    """기본 마스커로 텍스트를 마스킹한다(노션 A안 진입점).

    Args:
        text: OCR로 추출된 원본 텍스트.

    Returns:
        PII가 마스킹된 텍스트.
    """
    return get_masker().mask(text)
