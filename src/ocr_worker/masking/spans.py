"""PII 스팬 모델과 마스킹 적용 로직 (순수 함수, 부수효과 없음).

디텍터(정규식·NER)는 텍스트에서 PII 위치를 ``Span``으로 보고하고, 본 모듈이
합집합(OR)·겹침 병합·치환을 담당한다. 텍스트 마스킹(노션 A안)과 이미지
마스킹(B안) 모두 같은 ``Span`` 출력을 쓴다 — B안은 ``span``을 OCR bbox에 매핑한다.
"""

from dataclasses import dataclass
from enum import StrEnum


class PiiLabel(StrEnum):
    """마스킹 대상 PII 분류. 값은 치환 토큰의 본문(``[값]``)이 된다."""

    RRN = "주민등록번호"
    FRN = "외국인등록번호"
    PHONE = "전화번호"
    ACCOUNT = "계좌번호"
    EMAIL = "이메일"
    CARD = "카드번호"
    BIZ = "사업자등록번호"
    PASSPORT = "여권번호"
    DRIVER = "운전면허번호"
    MEDICAL_LICENSE = "면허번호"  # 의사 면허번호
    POLICY_NUMBER = "증권번호"  # 보험 증권/증서번호
    ADDRESS = "주소"
    NAME = "이름"

    @property
    def token(self) -> str:
        """치환에 쓰는 마스킹 토큰. 예: ``PiiLabel.RRN.token == "[주민등록번호]"``."""
        return f"[{self.value}]"


@dataclass(frozen=True, slots=True)
class Span:
    """텍스트 내 PII 1건의 위치·분류.

    Attributes:
        start: 시작 문자 인덱스(포함).
        end: 끝 문자 인덱스(미포함) — ``text[start:end]``가 PII 원문.
        label: PII 분류(치환 토큰 결정).
        source: 탐지 출처(``"regex"`` | ``"ner"``) — 디버깅·교차검증용.
        score: 신뢰도 0~1(정규식은 1.0 고정, NER은 모델 확률).
    """

    start: int
    end: int
    label: PiiLabel
    source: str = "regex"
    score: float = 1.0

    def __post_init__(self) -> None:
        if self.start < 0 or self.end <= self.start:
            raise ValueError(f"잘못된 스팬 범위: ({self.start}, {self.end})")

    @property
    def length(self) -> int:
        return self.end - self.start


def merge_overlaps(spans: list[Span]) -> list[Span]:
    """겹치는 스팬을 넓은 쪽으로 병합한다(노션 §2: 합집합 + 겹침 병합).

    인접(맞닿음)은 병합하지 않는다 — ``홍길동010-...``처럼 서로 다른 PII가 붙은
    경우 각각의 토큰으로 치환되도록 한다. 병합된 스팬의 라벨은 더 긴 원본 스팬을
    따르고, 길이가 같으면 결정적인 정규식(``source == "regex"``)을 우선한다.

    Args:
        spans: 임의 순서의 스팬 목록(중복·겹침 허용).

    Returns:
        시작 인덱스 오름차순으로 정렬된, 서로 겹치지 않는 스팬 목록.
    """
    if not spans:
        return []

    # 시작 오름차순, 같은 시작이면 긴(end 큰) 것을 먼저 둬서 넓은 쪽을 기준으로 잡는다.
    ordered = sorted(spans, key=lambda s: (s.start, -s.end))
    merged: list[Span] = [ordered[0]]

    for span in ordered[1:]:
        last = merged[-1]
        if span.start < last.end:  # 진짜 겹침(인접은 제외)
            merged[-1] = _wider(last, span)
        else:
            merged.append(span)
    return merged


def _wider(a: Span, b: Span) -> Span:
    """두 겹치는 스팬을 [min(start), max(end)]로 합치고 대표 라벨을 고른다."""
    start = min(a.start, b.start)
    end = max(a.end, b.end)
    primary = _choose_label_source(a, b)
    return Span(
        start=start,
        end=end,
        label=primary.label,
        source=primary.source,
        score=max(a.score, b.score),
    )


def _choose_label_source(a: Span, b: Span) -> Span:
    """라벨·출처를 정할 대표 스팬: 더 긴 쪽 → 동률이면 정규식 우선."""
    if a.length != b.length:
        return a if a.length > b.length else b
    return a if a.source == "regex" else b


def apply_mask(text: str, spans: list[Span]) -> str:
    """스팬을 병합한 뒤 텍스트를 마스킹 토큰으로 치환한다.

    치환은 **뒤에서 앞으로** 수행해 앞쪽 치환이 뒤쪽 인덱스를 밀지 않게 한다
    (노션 §2). 입력 ``spans``는 빈 목록일 수 있다(그대로 반환).

    Args:
        text: 원본 텍스트.
        spans: 디텍터가 보고한 PII 스팬(겹침·중복 허용).

    Returns:
        PII가 ``[분류]`` 토큰으로 치환된 텍스트.
    """
    masked = text
    for span in sorted(merge_overlaps(spans), key=lambda s: s.start, reverse=True):
        masked = masked[: span.start] + span.label.token + masked[span.end :]
    return masked
