"""마스킹 검증 — 노션 §7 Tier 1(결정적 잔류 스캔).

마스킹 *후* 텍스트에 고민감 PII가 남았는지 더 포괄적인(관대한) 정규식으로
재스캔한다. 본 디텍터(정밀)가 놓친 변형(공백 섞인 주민번호 등)을 잡는 안전망이다.
Tier 2(독립 NER/LLM 교차검증)·Tier 3(오프라인 QA)은 별도 트랙이라 여기 없다.

용도:
- ``find_residual_pii``: 잔류 PII 스팬 목록(메트릭·로깅용 — leak rate 추적).
- ``assert_no_residual``: 고민감 PII 잔류 시 ``MaskingError`` 발생(엄격 모드).
"""

import re

from ocr_worker.masking.spans import PiiLabel, Span


class MaskingError(Exception):
    """마스킹 검증 실패(고민감 PII 잔류 등)."""


# 잔류 스캔용 *관대한* 패턴 — 본 디텍터보다 구분자에 느슨하게(OCR 노이즈 대비).
# 토큰 본문(예: "[주민등록번호]")은 가린 결과이므로 잔류로 세지 않도록 제외한다.
_RESIDUAL_PATTERNS: tuple[tuple[re.Pattern[str], PiiLabel], ...] = (
    # 주민/외국인등록번호: 구분자에 공백·점 허용
    (re.compile(r"(?<!\d)\d{6}[-.\s]?\d{7}(?!\d)"), PiiLabel.RRN),
    # 16자리 카드(구분자 관대)
    (re.compile(r"(?<!\d)(?:\d[-.\s]?){15}\d(?!\d)"), PiiLabel.CARD),
    # 이메일
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), PiiLabel.EMAIL),
    # 휴대폰(고민감은 아니나 누락 추적용)
    (re.compile(r"(?<!\d)01[016789][-.\s]?\d{3,4}[-.\s]?\d{4}(?!\d)"), PiiLabel.PHONE),
)

# 잔류 시 즉시 실패시킬 고민감 분류(확률 모델에 안 맡기는 결정적 항목).
HIGH_SENSITIVITY: frozenset[PiiLabel] = frozenset(
    {PiiLabel.RRN, PiiLabel.FRN, PiiLabel.CARD, PiiLabel.ACCOUNT}
)


def find_residual_pii(masked_text: str) -> list[Span]:
    """마스킹된 텍스트에 남은 PII를 관대한 패턴으로 재스캔한다.

    Args:
        masked_text: ``mask()`` 출력.

    Returns:
        잔류 PII 스팬 목록(없으면 빈 목록).
    """
    residual: list[Span] = []
    for pattern, label in _RESIDUAL_PATTERNS:
        residual.extend(
            Span(m.start(), m.end(), label, source="verify") for m in pattern.finditer(masked_text)
        )
    return residual


def assert_no_residual(masked_text: str) -> None:
    """고민감 PII가 남아 있으면 ``MaskingError``를 던진다(엄격 모드).

    원문 값은 예외 메시지·로그에 넣지 않는다(PII 로깅 금지, CODE_CONVENTIONS §9).
    분류와 건수만 보고한다.

    Raises:
        MaskingError: ``HIGH_SENSITIVITY`` 분류가 1건 이상 잔류한 경우.
    """
    leaks = [s for s in find_residual_pii(masked_text) if s.label in HIGH_SENSITIVITY]
    if leaks:
        counts = ", ".join(sorted({s.label.value for s in leaks}))
        raise MaskingError(f"마스킹 후 고민감 PII 잔류: {counts} ({len(leaks)}건)")
