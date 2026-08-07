"""문서 유형 분류(#17) 단위 테스트 — 완전 결정적 순수 로직.

DocType별 대표 샘플로 정확도, OTHER 폴백, hint 타이브레이커를 고정한다.
AAA 구조이며 테스트 본문에 분기·반복을 두지 않는다.
"""

from core.contracts import DocType
from ocr_worker.classify import (
    CONFIDENCE_THRESHOLD,
    HINT_ONLY_CONFIDENCE,
    ClassifyResult,
    classify,
)

# ── 유형별 정확도 ────────────────────────────────────────────────


def test_classifies_diagnosis_by_keywords() -> None:
    # Arrange
    text = "진단서\n상병명: 급성 기관지염\n진단명 KCD J20.9\n처방 내역 ..."
    # Act
    result = classify(text)
    # Assert
    assert result.doc_type is DocType.DIAGNOSIS
    assert result.confidence >= CONFIDENCE_THRESHOLD


def test_classifies_policy_by_keywords() -> None:
    # Arrange
    text = "보험증권\n증권번호 A1234\n피보험자 항목\n보장내용 및 보험기간 안내"
    # Act
    result = classify(text)
    # Assert
    assert result.doc_type is DocType.POLICY
    assert result.confidence >= CONFIDENCE_THRESHOLD


def test_classifies_payout_notice_by_keywords() -> None:
    # Arrange
    text = "보험금 지급결과 안내문\n지급결정 내역\n산정내역 및 지급사유 설명"
    # Act
    result = classify(text)
    # Assert
    assert result.doc_type is DocType.PAYOUT_NOTICE
    assert result.confidence >= CONFIDENCE_THRESHOLD


def test_classifies_claim_by_keywords() -> None:
    # Arrange
    text = "보험금 청구서\n청구금액: 1,200,000원\n청구인 정보"
    # Act
    result = classify(text)
    # Assert
    assert result.doc_type is DocType.CLAIM
    assert result.confidence >= CONFIDENCE_THRESHOLD


def test_classifies_hospitalization_cert_by_keywords() -> None:
    # Arrange
    text = "입원확인서\n입원기간 2026-01-01부터\n치료기간 안내\n퇴원 예정"
    # Act
    result = classify(text)
    # Assert
    assert result.doc_type is DocType.HOSPITALIZATION_CERT
    assert result.confidence >= CONFIDENCE_THRESHOLD


def test_classifies_medical_receipt_by_keywords() -> None:
    # Arrange
    text = "외래진료비 계산서(영수증)\n진료비계산서\n영수금액 47500원\n본인부담금 안내"
    # Act
    result = classify(text)
    # Assert
    assert result.doc_type is DocType.MEDICAL_RECEIPT
    assert result.confidence >= CONFIDENCE_THRESHOLD


def test_hospitalization_cert_not_confused_with_diagnosis() -> None:
    # Arrange — "병명"은 진단서 키워드와도 겹치지만 입원확인서 고유 단서가 더 강함
    text = "입원확인서\n환자의 병명 골절\n입원기간 2026-01-01부터"
    # Act
    result = classify(text)
    # Assert
    assert result.doc_type is DocType.HOSPITALIZATION_CERT


# ── 공백 불규칙성 흡수 ──────────────────────────────────────────


def test_matches_keyword_despite_irregular_spacing() -> None:
    # Arrange — OCR이 토큰을 띄어 쓴 경우에도 '보험증권'/'증권번호'를 잡아야 함
    text = "보 험 증 권\n증권 번호 B-0001\n피 보 험 자"
    # Act
    result = classify(text)
    # Assert
    assert result.doc_type is DocType.POLICY


# ── OTHER 폴백 ──────────────────────────────────────────────────


def test_falls_back_to_other_when_no_evidence() -> None:
    # Arrange
    text = "안녕하세요. 이것은 일반적인 안내문입니다."
    # Act
    result = classify(text)
    # Assert
    assert result.doc_type is DocType.OTHER
    assert result.confidence == 0.0


def test_falls_back_to_other_on_low_confidence() -> None:
    # Arrange — 약한 단서 1개만(처방 가중치 1) → 임계 미만
    text = "처방 안내"
    # Act
    result = classify(text)
    # Assert
    assert result.doc_type is DocType.OTHER
    assert result.confidence < CONFIDENCE_THRESHOLD


def test_falls_back_to_other_on_tie_without_hint() -> None:
    # Arrange — 진단서(3)와 청구서(3) 동률, hint 없음
    text = "진단서 청구서"
    # Act
    result = classify(text)
    # Assert
    assert result.doc_type is DocType.OTHER


# ── hint 타이브레이커 ───────────────────────────────────────────


def test_hint_breaks_tie() -> None:
    # Arrange — 동률 상황에서 hint가 후보 중 하나를 선택
    text = "진단서 청구서"
    # Act
    result = classify(text, hint="claim")
    # Assert
    assert result.doc_type is DocType.CLAIM


def test_hint_used_when_no_evidence() -> None:
    # Arrange — 본문 단서 0, hint만 존재
    text = "별다른 키워드가 없는 문서"
    # Act
    result = classify(text, hint="diagnosis")
    # Assert
    assert result.doc_type is DocType.DIAGNOSIS
    assert result.confidence == HINT_ONLY_CONFIDENCE


def test_hint_does_not_override_strong_evidence() -> None:
    # Arrange — 본문은 명백한 진단서, hint는 엉뚱한 값
    text = "진단서\n상병명 골절\n진단명 KCD S82.1"
    # Act
    result = classify(text, hint="policy")
    # Assert — 강한 본문 단서가 hint를 이긴다
    assert result.doc_type is DocType.DIAGNOSIS


def test_invalid_hint_is_ignored() -> None:
    # Arrange — 단서 0 + 알 수 없는 hint → OTHER
    text = "키워드 없는 문서"
    # Act
    result = classify(text, hint="not_a_doc_type")
    # Assert
    assert result.doc_type is DocType.OTHER


# ── 결과 타입 ────────────────────────────────────────────────────


def test_returns_classify_result_dataclass() -> None:
    # Arrange
    text = "진단서 상병명"
    # Act
    result = classify(text)
    # Assert
    assert isinstance(result, ClassifyResult)
    assert 0.0 <= result.confidence <= 1.0
