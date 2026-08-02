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

from core.contracts import DocType

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
    return label.group(1).strip()[:DIAGNOSIS_NAME_MAX_LEN] if label else None


def _extract_insurer(text: str) -> str | None:
    """첫 보험사명을 반환한다(없으면 None)."""
    match = INSURER_RE.search(text)
    return match.group(1) if match else None


def _extract_product(text: str) -> str | None:
    """상품명을 반환한다('상품명' 라벨 우선, 없으면 '무배당…보험' 폴백, 둘 다 없으면 None)."""
    label = PRODUCT_LABEL_RE.search(text)
    if label is not None:
        return label.group(1).strip()[:PRODUCT_MAX_LEN]
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
        return {"diagnosis_name": _extract_diagnosis_name(text)}
    if doc_type is DocType.POLICY:
        return {"insurer": _extract_insurer(text), "product": _extract_product(text)}
    if doc_type is DocType.PAYOUT_NOTICE:
        return {
            "insurer": _extract_insurer(text),
            "payout_amount": _extract_amount_reference(text, _PAYOUT_AMOUNT_KEYWORDS),
        }
    if doc_type is DocType.CLAIM:
        return {"payout_amount": _extract_amount_reference(text, _CLAIM_AMOUNT_KEYWORDS)}
    if doc_type in (DocType.HOSPITALIZATION_CERT, DocType.MEDICAL_RECEIPT):
        # 항목별 구조화 데이터(입원기간·진료 항목별 금액 등)는 표 구조가 필요해
        # ocr_worker.pipeline의 하이브리드 VLM 경로가 entities["table_markdown"]에 채운다.
        return {}
    return {}
