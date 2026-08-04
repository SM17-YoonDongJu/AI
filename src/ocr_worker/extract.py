"""유형별 엔티티 추출 (이슈 #17) — 규칙 기반 순수 로직.

OCR 텍스트(str)에서 ``ocr_results.entities``(jsonb)에 저장할 **비-PII 도메인 값**만
뽑는다. 부수효과·I/O 없는 순수 함수이며 외부 의존이 없다.

⚠️ PII 금지(핵심 제약): 추출은 마스킹보다 *먼저* 일어난다. 따라서 이름·주민번호·
전화·계좌·주소 등 개인정보를 entities에 **절대** 넣지 않는다. 여기서 뽑는 값은
KCD 코드·보험사명·상품명·금액(참고값)처럼 개인을 식별하지 않는 도메인 값뿐이다.

⚠️ 금액 단정 금지: 금액은 추출되면 *참고용*으로만 싣고, 못 찾으면 ``None``. 최종
금액 단정은 리포트 단계의 가드레일이 범위(estimated_range)로만 표현한다.

키 정책: 반환 dict는 **해당 유형에 의미 있는 필드만** 담는다. 유형에 해당하지 않는
필드는 키 자체를 생략하고, 의미는 있으나 값을 못 찾은 필드는 값 ``None``으로 둔다
(downstream이 "유형상 무관"과 "추출 실패"를 구분할 수 있게). ``OTHER``는 빈 dict.
"""

import re
from datetime import date

from core.contracts import DocType
from ocr_worker.masking.regex_detector import RegexDetector

# ── KCD(한국표준질병사인분류) 코드 ──────────────────────────────
# 영문 1 + 숫자 2 (+ 선택 소수점 1자리). 예: S82, S82.1, J20.9. 앞뒤 영숫자 경계로
# 단어 중간 부분매칭을 막는다("KCD"의 글자나 일련번호가 잡히지 않게).
KCD_RE = re.compile(r"(?<![A-Za-z0-9])([A-Z]\d{2}(?:\.\d)?)(?![0-9])")

# ── 병명(한글, KCD 코드 없는 진단서용 폴백) ───────────────────────
# 실측(실제 진단서 샘플): KCD 코드 없이 한글 병명만 적히는 문서가 더 흔하다.
# 표 양식은 라벨 글자 사이·줄 사이에 공백/개행이 끼는 경우가 많다(예: "병 명\n및\n진 단"
# 처럼 셀이 여러 줄로 쪼개짐) → 라벨 글자 사이 \s*로 이를 흡수한다.
DIAGNOSIS_LABEL_RE = re.compile(
    r"(?:상\s*병\s*명|병\s*명|진\s*단\s*명)(?:\s*및\s*진\s*단)?\s*[:：]?\s*([^\n]+)"  # noqa: RUF001 (전각 콜론)
)
# 라벨 캡처가 한 줄 전체를 삼키지 않도록 상한(PRODUCT_MAX_LEN과 동일 정책).
DIAGNOSIS_NAME_MAX_LEN = 40

# ── 보험사명 ─────────────────────────────────────────────────────
# 회사명 접두 + 업권 접미사. 접미사를 강제해 일반어("생명보험" 단독)·사람 이름을
# 배제한다(사람 이름은 이 접미사로 끝나지 않으므로 PII 혼입 위험 없음).
INSURER_RE = re.compile(
    r"([가-힣A-Za-z]{2,10}(?:손해보험|생명보험|화재보험|해상보험|생명|화재|손보|해상))"
)

# ── 상품명 ───────────────────────────────────────────────────────
# 1순위: '상품명' 라벨 뒤 같은 줄. 2순위: '무배당…보험' 형태(보험상품 관용 표기).
PRODUCT_LABEL_RE = re.compile(r"상품\s*명\s*[:：]?\s*([^\n]+)")  # noqa: RUF001 (전각 콜론)
PRODUCT_FALLBACK_RE = re.compile(r"(무배당\s*[가-힣A-Za-z0-9()]+(?:\s*[가-힣A-Za-z0-9()]+)*보험)")
# 라벨 캡처가 한 줄 전체를 삼키지 않도록 상한(과도한 꼬리·다음 항목 혼입 방지).
PRODUCT_MAX_LEN = 40

# ── 금액(참고값) ─────────────────────────────────────────────────
# 숫자(천단위 콤마 허용) + '원'. '원' 앵커로 날짜·일련번호·코드를 배제한다.
AMOUNT_RE = re.compile(r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)\s*원")
# 금액 바로 앞 이 글자수 안에 관련 키워드가 있을 때만 채택(무관한 숫자 오추출 방지).
AMOUNT_KEYWORD_WINDOW = 15
# 유형별 금액 문맥 키워드.
_PAYOUT_AMOUNT_KEYWORDS = ("지급", "보험금", "결정")
_CLAIM_AMOUNT_KEYWORDS = ("청구", "보험금")

# ── 입원일수(참고값) ────────────────────────────────────────────
# YYYY-MM-DD / YYYY.MM.DD / YYYY년 MM월 DD일 형태를 폭넓게 허용.
_DATE_RE = r"(\d{4})[.\-./년]\s*(\d{1,2})[.\-./월]\s*(\d{1,2})일?"
# 직접 일수 표기("입원일수: 5일", "재원일수 5일")가 있으면 최우선으로 쓴다.
ADMISSION_DAYS_LABEL_RE = re.compile(r"(?:입원|재원)\s*일수\s*[:：]?\s*(\d{1,4})\s*일")  # noqa: RUF001
# "입원기간 2026-01-01부터 2026-01-05까지"처럼 한 라벨 아래 범위로 오는 형태.
ADMISSION_RANGE_RE = re.compile(
    rf"입원\s*(?:기간|일자?)\s*[:：]?\s*{_DATE_RE}\s*(?:부터|~|-|–)\s*{_DATE_RE}\s*까지?"  # noqa: RUF001
)
# "입원일"/"퇴원일"이 서로 다른 라벨로 떨어져 있는 형태.
ADMISSION_DATE_RE = re.compile(rf"입원\s*일자?\s*[:：]?\s*{_DATE_RE}")  # noqa: RUF001
DISCHARGE_DATE_RE = re.compile(rf"퇴원\s*일자?\s*[:：]?\s*{_DATE_RE}")  # noqa: RUF001

# ── 수술 여부 ────────────────────────────────────────────────────
SURGERY_LABEL_RE = re.compile(r"수술\s*명\s*[:：]?\s*([^\n]+)")  # noqa: RUF001
_SURGERY_NEGATIONS = frozenset({"없음", "해당없음", "해당 없음", "무", "-", "x", "X"})

# ── 라벨 폴백 후보의 PII 오염 검사 ──────────────────────────────
# DIAGNOSIS_LABEL_RE·PRODUCT_LABEL_RE의 [^\n]+는 라벨 뒤 줄 끝까지 통째로 캡처한다.
# OCR이 인접 셀을 한 줄로 병합하면(예: "병명: 급성 기관지염 성명: 홍길동") 이름 같은
# PII가 같이 캡처될 수 있다 — 마스킹(§13)은 이 값을 검사하지 않으므로 여기서 직접
# 걸러야 한다. RegexDetector만 쓴다(정규식 전용, NER 모델 로드 없음 — extract()는
# 순수·경량이어야 함).
_PII_DETECTOR = RegexDetector()


def _pii_free(candidate: str) -> str | None:
    """후보 문자열에 PII가 섞여 있으면 통째로 버린다(부분 편집 없음, 실측 확인)."""
    return None if _PII_DETECTOR.detect(candidate) else candidate


def _extract_kcd(text: str) -> str | None:
    """첫 KCD 코드를 반환한다(없으면 None)."""
    match = KCD_RE.search(text)
    return match.group(1) if match else None


def _extract_diagnosis_name(text: str) -> str | None:
    """진단명을 반환한다. KCD 코드가 있으면 코드를, 없으면 라벨 뒤 한글 병명을 쓴다.

    코드가 우선인 이유: 코드는 모호함이 없는 표준값이다. 코드가 없는 문서가 더
    흔하므로(실측), 라벨(병명·상병명·진단명) 뒤 텍스트를 폴백으로 잡아 완전히
    빈 값이 되는 경우를 줄인다.
    """
    kcd = _extract_kcd(text)
    if kcd is not None:
        return kcd
    label = DIAGNOSIS_LABEL_RE.search(text)
    if label is None:
        return None
    return _pii_free(label.group(1).strip()[:DIAGNOSIS_NAME_MAX_LEN])


def _extract_insurer(text: str) -> str | None:
    """첫 보험사명을 반환한다(없으면 None)."""
    match = INSURER_RE.search(text)
    return match.group(1) if match else None


def _extract_product(text: str) -> str | None:
    """상품명을 반환한다('상품명' 라벨 우선, 없으면 '무배당…보험' 폴백, 둘 다 없으면 None).

    라벨 값이 PII로 오염됐으면(같은 줄에 다른 항목이 병합된 경우) 그 값은 버리고
    폴백 패턴으로 다시 시도한다 — 폴백은 "무배당…보험" 구조만 매칭해 PII 혼입 위험이 없다.
    """
    label = PRODUCT_LABEL_RE.search(text)
    if label is not None:
        candidate = _pii_free(label.group(1).strip()[:PRODUCT_MAX_LEN])
        if candidate is not None:
            return candidate
    fallback = PRODUCT_FALLBACK_RE.search(text)
    return fallback.group(1).strip() if fallback else None


def _extract_amount_reference(text: str, keywords: tuple[str, ...]) -> int | None:
    """관련 키워드 근처의 금액(원)을 콤마 제거한 정수로 반환한다(참고값, 없으면 None).

    여러 금액이 있으면 키워드 문맥에 가장 먼저 부합하는 값을 택한다. 어떤 금액이
    "정답"인지 단정하지 않으며, 추출값은 참고용임을 호출자가 전제한다.
    """
    for match in AMOUNT_RE.finditer(text):
        window_start = max(0, match.start() - AMOUNT_KEYWORD_WINDOW)
        window = text[window_start : match.start()]
        if any(keyword in window for keyword in keywords):
            return int(match.group(1).replace(",", ""))
    return None


def _parse_date(y: str, m: str, d: str) -> date | None:
    """캡처된 연/월/일 문자열을 날짜로 파싱한다(잘못된 값이면 None)."""
    try:
        return date(int(y), int(m), int(d))
    except ValueError:
        return None


def _extract_admission_days(text: str) -> int | None:
    """입원일수를 반환한다(참고값, 없으면 None).

    "입원일수: N일"처럼 직접 표기가 있으면 그대로 쓰고, 없으면 입원~퇴원 날짜 쌍에서
    일수를 계산한다(한 라벨 아래 범위 표기 우선, 그다음 입원일/퇴원일 별도 라벨).
    """
    label = ADMISSION_DAYS_LABEL_RE.search(text)
    if label is not None:
        return int(label.group(1))

    range_match = ADMISSION_RANGE_RE.search(text)
    if range_match is not None:
        start = _parse_date(*range_match.group(1, 2, 3))
        end = _parse_date(*range_match.group(4, 5, 6))
    else:
        start_match = ADMISSION_DATE_RE.search(text)
        end_match = DISCHARGE_DATE_RE.search(text)
        if start_match is None or end_match is None:
            return None
        start = _parse_date(*start_match.group(1, 2, 3))
        end = _parse_date(*end_match.group(1, 2, 3))

    if start is None or end is None or end < start:
        return None
    return (end - start).days


def _extract_surgery(text: str) -> bool | None:
    """수술 시행 여부를 반환한다. '수술명' 라벨 자체가 없으면 알 수 없음(None)."""
    match = SURGERY_LABEL_RE.search(text)
    if match is None:
        return None
    value = match.group(1).strip()
    return bool(value) and value not in _SURGERY_NEGATIONS


def extract(doc_type: DocType, text: str) -> dict[str, object]:
    """문서 유형에 맞는 비-PII 도메인 엔티티를 추출한다.

    부수효과·I/O 없는 순수 함수. 반환 dict는 ``ocr_results.entities``(jsonb)에 그대로
    저장 가능한 비-PII 값만 담는다(이름·주민번호·전화·계좌·주소 등 PII는 절대 포함하지
    않는다).

    Args:
        doc_type: ``classify``가 판정한 문서 유형.
        text: OCR 텍스트(평문).

    Returns:
        유형별 엔티티 dict. 유형 무관 필드는 키 생략, 추출 실패 필드는 ``None``.
        ``DocType.OTHER``는 빈 dict.
    """
    if doc_type is DocType.DIAGNOSIS:
        # icd: report_worker(리포트 05번)의 지급액 산정 로직이 참조하는 필드명.
        # diagnosis_name과 값이 같을 수 있으나(둘 다 KCD 코드 우선), diagnosis_name은
        # 코드가 없을 때 한글 병명으로 폴백하는 반면 icd는 코드가 없으면 None으로
        # 남긴다 — 폴백값을 코드로 오인해 쓰면 안 되는 소비자를 위한 별도 필드.
        return {"diagnosis_name": _extract_diagnosis_name(text), "icd": _extract_kcd(text)}
    if doc_type is DocType.POLICY:
        # insurer/product는 report_worker가 참조하지 않는다(user_insurances 테이블에서
        # 따로 조회) — 여기 값은 ocr_quality(재확인 필요) 판정용으로만 쓰인다.
        return {"insurer": _extract_insurer(text), "product": _extract_product(text)}
    if doc_type is DocType.PAYOUT_NOTICE:
        # payout_amount도 위와 같은 이유로 quality 판정용 참고값일 뿐 report_worker
        # 소비 대상이 아니다 — 다중 항목 표에서 오추출될 수 있는 알려진 한계 포함.
        return {
            "insurer": _extract_insurer(text),
            "payout_amount": _extract_amount_reference(text, _PAYOUT_AMOUNT_KEYWORDS),
        }
    if doc_type is DocType.CLAIM:
        return {"payout_amount": _extract_amount_reference(text, _CLAIM_AMOUNT_KEYWORDS)}
    if doc_type is DocType.HOSPITALIZATION_CERT:
        # admission_days/surgery: report_worker의 지급액 산정 로직이 참조하는 필드명.
        return {
            "admission_days": _extract_admission_days(text),
            "surgery": _extract_surgery(text),
        }
    if doc_type is DocType.MEDICAL_RECEIPT:
        # 항목별 진료비 세부내역은 표 구조가 필요해 ocr_worker.pipeline의 하이브리드
        # VLM 경로가 entities["table_markdown"]에 채운다.
        return {}
    return {}
