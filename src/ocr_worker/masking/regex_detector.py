"""정규식 PII 디텍터 — 항상 ON, 결정적.

노션 분담대로 정형 PII(주민·전화·계좌·이메일·카드·사업자·여권·운전면허·주소)를
잡아 ``Span`` 목록으로 반환한다. ``Detector`` 프로토콜을 만족하므로 NER 디텍터와
동일 인터페이스로 ``mask()``에서 합쳐 쓴다.
"""

import re

from ocr_worker.masking.patterns import (
    ACCOUNT_KEYWORD_RE,
    ACCOUNT_LOOKAHEAD,
    ACCOUNT_NUMBER_RE,
    ADDRESS_RE,
    APPROVAL_NUMBER_KEYWORD_RE,
    APPROVAL_NUMBER_LOOKAHEAD,
    APPROVAL_NUMBER_VALUE_RE,
    BIZ_RE,
    CARD_RE,
    DOCTOR_NAME_COLON_RE,
    DOCTOR_NAME_RE,
    DRIVER_RE,
    EMAIL_RE,
    FOREIGN_GENDER_DIGITS,
    MEDICAL_LICENSE_KEYWORD_RE,
    MEDICAL_LICENSE_LOOKAHEAD,
    MEDICAL_LICENSE_VALUE_RE,
    PASSPORT_RE,
    PATIENT_ID_KEYWORD_RE,
    PATIENT_ID_LOOKAHEAD,
    PATIENT_ID_VALUE_RE,
    PERSON_LABEL_NAME_RE,
    PHONE_RE,
    POLICY_NUMBER_KEYWORD_RE,
    POLICY_NUMBER_LOOKAHEAD,
    POLICY_NUMBER_VALUE_RE,
    RRN_RE,
    SIGNATURE_NAME_RE,
    WARD_ROOM_KEYWORD_RE,
    WARD_ROOM_LOOKAHEAD,
    WARD_ROOM_VALUE_RE,
    is_non_name,
)
from ocr_worker.masking.spans import PiiLabel, Span

# 라벨이 1:1로 고정된 단순 패턴들. (정규식, 라벨) 순서대로 적용.
# RRN(라벨 분기)·ACCOUNT(문맥 앵커)는 특수 처리라 여기 없다.
_SIMPLE_PATTERNS: tuple[tuple[re.Pattern[str], PiiLabel], ...] = (
    (EMAIL_RE, PiiLabel.EMAIL),
    (CARD_RE, PiiLabel.CARD),
    (PHONE_RE, PiiLabel.PHONE),
    (BIZ_RE, PiiLabel.BIZ),
    (DRIVER_RE, PiiLabel.DRIVER),
    (PASSPORT_RE, PiiLabel.PASSPORT),
    (ADDRESS_RE, PiiLabel.ADDRESS),
)


class RegexDetector:
    """정형 PII 정규식 디텍터. 상태 없음 — 인스턴스 재사용 안전(스레드 안전)."""

    def detect(self, text: str) -> list[Span]:
        """텍스트에서 정형 PII 스팬을 모두 찾아 반환한다(겹침·중복 허용).

        Args:
            text: OCR로 추출된 원본 텍스트.

        Returns:
            ``Span`` 목록. 정렬·병합은 호출측(``apply_mask``)이 담당한다.
        """
        spans: list[Span] = []
        spans.extend(self._detect_rrn(text))
        spans.extend(self._detect_doctor_names(text))
        # 문맥 앵커형(키워드 뒤 값) — 계좌·면허번호·증권번호.
        spans.extend(
            self._detect_anchored(
                text, ACCOUNT_KEYWORD_RE, ACCOUNT_NUMBER_RE, PiiLabel.ACCOUNT, ACCOUNT_LOOKAHEAD
            )
        )
        spans.extend(
            self._detect_anchored(
                text,
                MEDICAL_LICENSE_KEYWORD_RE,
                MEDICAL_LICENSE_VALUE_RE,
                PiiLabel.MEDICAL_LICENSE,
                MEDICAL_LICENSE_LOOKAHEAD,
            )
        )
        spans.extend(
            self._detect_anchored(
                text,
                POLICY_NUMBER_KEYWORD_RE,
                POLICY_NUMBER_VALUE_RE,
                PiiLabel.POLICY_NUMBER,
                POLICY_NUMBER_LOOKAHEAD,
            )
        )
        spans.extend(
            self._detect_anchored(
                text,
                WARD_ROOM_KEYWORD_RE,
                WARD_ROOM_VALUE_RE,
                PiiLabel.WARD_ROOM,
                WARD_ROOM_LOOKAHEAD,
            )
        )
        spans.extend(
            self._detect_anchored(
                text,
                APPROVAL_NUMBER_KEYWORD_RE,
                APPROVAL_NUMBER_VALUE_RE,
                PiiLabel.APPROVAL_NUMBER,
                APPROVAL_NUMBER_LOOKAHEAD,
            )
        )
        spans.extend(
            self._detect_anchored(
                text,
                PATIENT_ID_KEYWORD_RE,
                PATIENT_ID_VALUE_RE,
                PiiLabel.PATIENT_ID,
                PATIENT_ID_LOOKAHEAD,
            )
        )
        for pattern, label in _SIMPLE_PATTERNS:
            spans.extend(
                Span(m.start(), m.end(), label, source="regex") for m in pattern.finditer(text)
            )
        return spans

    @staticmethod
    def _detect_rrn(text: str) -> list[Span]:
        """주민·외국인등록번호. 7번째 자리로 라벨을 분기한다."""
        spans: list[Span] = []
        for match in RRN_RE.finditer(text):
            gender_digit = match.group(2)[0]
            label = PiiLabel.FRN if gender_digit in FOREIGN_GENDER_DIGITS else PiiLabel.RRN
            spans.append(Span(match.start(), match.end(), label, source="regex"))
        return spans

    @staticmethod
    def _detect_anchored(
        text: str,
        keyword_re: re.Pattern[str],
        value_re: re.Pattern[str],
        label: PiiLabel,
        lookahead: int,
    ) -> list[Span]:
        """문맥 앵커형 PII — 키워드 뒤 ``lookahead`` 글자 안의 값만 잡는다.

        계좌·면허번호·증권번호처럼 값 자체로는 다른 숫자열(전화·금액·날짜)과 구분이
        안 되는 PII에 쓴다. 키워드(계좌/면허번호/증권번호...)를 앵커로 둬서 오탐을 막는다.

        Args:
            text: 원본 텍스트.
            keyword_re: 앵커 키워드 패턴.
            value_re: 키워드 뒤 윈도에서 찾을 값 패턴.
            label: 부여할 PII 분류.
            lookahead: 키워드 끝부터 값 탐색을 허용할 최대 글자 수.

        Returns:
            값 위치(키워드 제외)를 가리키는 ``Span`` 목록.
        """
        spans: list[Span] = []
        for kw in keyword_re.finditer(text):
            window = text[kw.end() : kw.end() + lookahead]
            value = value_re.search(window)
            if value:
                start = kw.end() + value.start()
                end = kw.end() + value.end()
                spans.append(Span(start, end, label, source="regex"))
        return spans

    @staticmethod
    def _detect_doctor_names(text: str) -> list[Span]:
        """의사명·서명란·환자/계약자 등 라벨 뒤 이름. 임상어/직함은 ``is_non_name``으로 제외.

        서명·도장 이미지는 B안(검은박스) 담당이고, 여기서는 텍스트에 노출된 이름만
        ``[이름]``으로 가린다. NER이 흔치 않은 합성 이름을 놓치는 사례가 실측으로
        확인돼(§PERSON_LABEL_NAME_RE), 라벨 앵커 정규식을 NER과 별개 안전망으로 둔다
        (NER 사용 여부와 무관하게 항상 적용).
        """
        spans: list[Span] = []
        for pattern in (
            DOCTOR_NAME_RE,
            DOCTOR_NAME_COLON_RE,
            SIGNATURE_NAME_RE,
            PERSON_LABEL_NAME_RE,
        ):
            for match in pattern.finditer(text):
                name = match.group(1)
                if is_non_name(name):
                    continue
                spans.append(Span(match.start(1), match.end(1), PiiLabel.NAME, source="regex"))
        return spans
