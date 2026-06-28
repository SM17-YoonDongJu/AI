"""문서 유형 분류 (이슈 #17) — 규칙 기반 순수 로직.

OCR로 추출된 **첫 페이지 텍스트(str)** 위에서 동작하는 부수효과 없는 순수 함수다.
키워드/패턴 점수로 ``DocType``을 판정하고, 단서가 약하면 ``DocType.OTHER``로
폴백하면서 신뢰도를 함께 반환한다. surya-ocr·S3·DB·Kafka 등 외부 의존은 없다
(그 책임은 다른 sub-issue 소관).

설계 메모:
- **공백 정규화 후 부분일치**: OCR은 토큰 사이 공백이 불규칙하므로(`보험금 지급`↔
  `보험금지급`) 매칭 전 모든 공백을 제거하고, 키워드는 공백 없는 형태로 둔다.
- **가중 키워드**: 문서 제목·고유 항목(`진단서`·`증권번호`)은 가중치를 높여 일반어
  보다 강하게 본다. 같은 키워드가 여러 번 나와도 한 번만 센다(반복 입력으로 점수가
  왜곡되지 않게 — 결정적 동작 보장).
- **불확실성 처리**: 최고 점수가 없거나(단서 0) 1·2위 격차가 작으면(동률/저신뢰)
  ``OTHER``로 폴백한다. ``hint``(OcrJob.doc_type_hint)는 *오직* 이때 타이브레이커로만
  쓴다 — 본문 단서를 덮어쓰지 않는다.
"""

import re
from dataclasses import dataclass

from core.contracts import DocType

# ── 신뢰도 상수 (매직값 금지) ────────────────────────────────────
# confidence = best / (best + second + SMOOTHING). 평활항으로 단서가 단 하나일 때
# 신뢰도가 1.0으로 과신되지 않게 한다.
CONFIDENCE_SMOOTHING = 1.0
# 이 값 미만이면 본문 단서만으로는 불확실로 보고 OTHER 폴백(또는 hint 타이브레이커).
CONFIDENCE_THRESHOLD = 0.55
# 본문 단서가 전무할 때 hint만 믿고 판정하는 약한 신뢰도.
HINT_ONLY_CONFIDENCE = 0.3
# 동률/저신뢰를 hint로 깬 경우의 신뢰도 하한.
TIEBREAK_CONFIDENCE = 0.5

# 매칭 전 공백 제거용.
_WHITESPACE_RE = re.compile(r"\s+")

# ── 유형별 가중 키워드 (공백 없는 형태) ─────────────────────────
# 값은 (키워드, 가중치). 가중치 3=제목/고유항목, 2=특징적, 1=보조.
_DOC_TYPE_KEYWORDS: dict[DocType, tuple[tuple[str, int], ...]] = {
    DocType.DIAGNOSIS: (
        ("진단서", 3),
        ("상병명", 3),
        ("진단명", 2),
        ("병명", 2),
        ("KCD", 2),
        ("처방", 1),
        ("소견서", 1),
        ("향후치료의견", 1),
    ),
    DocType.POLICY: (
        ("보험증권", 3),
        ("증권번호", 3),
        ("피보험자", 2),
        ("보장내용", 2),
        ("보험기간", 2),
        ("보험가입금액", 2),
        ("계약자", 1),
    ),
    DocType.PAYOUT_NOTICE: (
        ("지급결과안내", 3),
        ("지급결정", 3),
        ("보험금지급", 2),
        ("산정내역", 2),
        ("지급사유", 2),
        ("지급금액", 1),
    ),
    DocType.CLAIM: (
        ("청구서", 3),
        ("청구금액", 3),
        ("보험금청구", 2),
        ("청구인", 2),
    ),
}


@dataclass(slots=True)
class ClassifyResult:
    """분류 결과(내부 순수 데이터 — pydantic 불필요).

    Attributes:
        doc_type: 판정된 문서 유형. 불확실 시 ``DocType.OTHER``.
        confidence: 0.0~1.0 신뢰도. OTHER 폴백 시에도 근거 점수 기반으로 기록한다.
    """

    doc_type: DocType
    confidence: float


def _normalize(text: str) -> str:
    """매칭용으로 모든 공백을 제거한다(OCR 공백 불규칙성 흡수)."""
    return _WHITESPACE_RE.sub("", text)


def _score_doc_types(normalized: str) -> dict[DocType, int]:
    """각 유형의 가중 키워드 부분일치 점수를 합산한다(키워드별 1회만 계산)."""
    scores: dict[DocType, int] = {}
    for doc_type, keywords in _DOC_TYPE_KEYWORDS.items():
        total = sum(weight for keyword, weight in keywords if keyword in normalized)
        if total > 0:
            scores[doc_type] = total
    return scores


def _hint_to_doc_type(hint: str | None) -> DocType | None:
    """hint 문자열을 DocType으로 변환한다(미지정·미지의 값이면 None)."""
    if hint is None:
        return None
    try:
        return DocType(hint)
    except ValueError:
        return None


def classify(text: str, hint: str | None = None) -> ClassifyResult:
    """첫 페이지 텍스트의 키워드 점수로 문서 유형을 판정한다.

    부수효과·I/O가 없는 순수 함수다. 본문 단서가 우선이며, hint는 동률이거나
    신뢰도가 낮을 때만 타이브레이커로 사용한다. 어느 경우든 확신이 없으면
    ``DocType.OTHER``로 폴백하고 신뢰도를 함께 반환한다.

    Args:
        text: OCR로 추출된 첫 페이지 텍스트(평문). 마스킹 이전·이후 무관(이 함수는
            PII를 읽지도 출력하지도 않는다).
        hint: ``OcrJob.doc_type_hint``. 동률/저신뢰 시에만 후보를 결정하는 보조 신호.
            유효한 DocType 값이 아니면 무시한다.

    Returns:
        ``ClassifyResult(doc_type, confidence)``. confidence는 0.0~1.0.
    """
    normalized = _normalize(text)
    scores = _score_doc_types(normalized)
    hint_type = _hint_to_doc_type(hint)

    # 본문 단서가 전혀 없음: hint가 있으면 약하게 채택, 없으면 OTHER.
    if not scores:
        if hint_type is not None:
            return ClassifyResult(hint_type, HINT_ONLY_CONFIDENCE)
        return ClassifyResult(DocType.OTHER, 0.0)

    # 점수 내림차순(동점은 DocType 정의 순으로 안정 정렬).
    ranked = sorted(scores.items(), key=lambda item: -item[1])
    best_type, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0
    is_tie = second_score == best_score

    confidence = round(best_score / (best_score + second_score + CONFIDENCE_SMOOTHING), 3)
    uncertain = is_tie or confidence < CONFIDENCE_THRESHOLD

    if uncertain:
        # hint가 실제 후보(점수>0) 중 하나면 타이브레이커로 채택.
        if hint_type is not None and scores.get(hint_type, 0) > 0:
            return ClassifyResult(hint_type, max(confidence, TIEBREAK_CONFIDENCE))
        return ClassifyResult(DocType.OTHER, confidence)

    return ClassifyResult(best_type, confidence)
