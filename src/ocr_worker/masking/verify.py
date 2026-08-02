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
    # 16자리·15자리(Amex) 카드(구분자 관대)
    (re.compile(r"(?<!\d)(?:\d[-.\s]?){14,15}\d(?!\d)"), PiiLabel.CARD),
    # 이메일
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), PiiLabel.EMAIL),
    # 휴대폰(고민감은 아니나 누락 추적용)
    (re.compile(r"(?<!\d)01[016789][-.\s]?\d{3,4}[-.\s]?\d{4}(?!\d)"), PiiLabel.PHONE),
    # 계좌번호(문맥 앵커) — 자유형식이라 단독 숫자로는 오탐↑. 본 디텍터와 같은 키워드 뒤
    # 25자 안의 숫자 묶음(8~16자리, 구분자에 '.'까지 관대)만 잔류로 본다. 추적 전용(_TRACK_ONLY).
    (
        re.compile(
            r"(?:계좌\s*번호|계좌|예금주|예금|가상계좌|입금|이체|account)"
            r"[^\d\n]{0,25}(?<![\d-])(?:\d[-.\s]?){7,15}\d(?![\d-])",
            re.IGNORECASE,
        ),
        PiiLabel.ACCOUNT,
    ),
    # 병실번호·승인번호·환자등록번호(신규) — 앵커 기반이라 ACCOUNT와 같은 이유로
    # 메트릭 추적만 한다(HIGH_SENSITIVITY 미포함 → assert_no_residual 하드게이트 대상 아님).
    (
        re.compile(r"(?:병실\s*번호|병실|병동|입원실|호실)[^\d\n]{0,10}\d{1,4}\s*호?"),
        PiiLabel.WARD_ROOM,
    ),
    (
        re.compile(r"(?:현금영수증\s*)?승인\s*번호[^\d\n]{0,15}\d{6,10}"),
        PiiLabel.APPROVAL_NUMBER,
    ),
    (
        re.compile(
            r"(?:원무\s*접수\s*번호|환자\s*등록\s*번호|환자\s*번호|접수\s*번호|차트\s*번호)"
            r"[^\d\n]{0,15}[A-Za-z0-9-]{4,15}"
        ),
        PiiLabel.PATIENT_ID,
    ),
)

# 분류상 고민감 PII(확률 모델에 안 맡기는 결정적 항목).
HIGH_SENSITIVITY: frozenset[PiiLabel] = frozenset(
    {PiiLabel.RRN, PiiLabel.FRN, PiiLabel.CARD, PiiLabel.ACCOUNT}
)
# 고민감이나 '추적 전용' — 계좌 잔류 패턴은 문맥 앵커 기반이라 오탐 시 하드 게이트가
# 정상 마스킹본을 MaskingError로 차단할 위험이 있다. 그래서 find_residual_pii(메트릭)엔
# 잡되 하드 실패는 안 시킨다. FP율을 계측해 안정되면 이 집합에서 빼 게이트로 승격한다.
_TRACK_ONLY: frozenset[PiiLabel] = frozenset({PiiLabel.ACCOUNT})
# assert_no_residual이 하드 실패시키는 대상(고민감에서 추적 전용 제외).
_GATED: frozenset[PiiLabel] = HIGH_SENSITIVITY - _TRACK_ONLY


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
    분류와 건수만 보고한다. ``_TRACK_ONLY``(현재 계좌번호)는 메트릭으로만 추적하고
    하드 실패시키지 않는다 — ``find_residual_pii``로는 잡힌다.

    Raises:
        MaskingError: ``_GATED``(고민감에서 추적 전용 제외) 분류가 1건 이상 잔류한 경우.
    """
    leaks = [s for s in find_residual_pii(masked_text) if s.label in _GATED]
    if leaks:
        counts = ", ".join(sorted({s.label.value for s in leaks}))
        raise MaskingError(f"마스킹 후 고민감 PII 잔류: {counts} ({len(leaks)}건)")
