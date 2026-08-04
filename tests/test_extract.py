"""유형별 엔티티 추출(#17) 단위 테스트 — 결정적 순수 로직.

유형별 필드, 금액 None/참고값, 그리고 **텍스트에 PII가 있어도 entities에는 들어가지
않는지**를 고정한다. AAA 구조이며 본문에 분기·반복을 두지 않는다.
"""

from core.contracts import DocType
from ocr_worker.extract import extract

# ── 진단서: diagnosis_name(KCD) ─────────────────────────────────


def test_diagnosis_extracts_kcd_code() -> None:
    # Arrange
    text = "최종진단 상병명: 발목 골절 (KCD S82.1)"
    # Act
    entities = extract(DocType.DIAGNOSIS, text)
    # Assert — icd는 report_worker(#11)가 참조하는 필드명, diagnosis_name과 별개로 유지
    assert entities == {"diagnosis_name": "S82.1", "icd": "S82.1"}


def test_diagnosis_falls_back_to_label_text_when_kcd_absent() -> None:
    # Arrange — 실측: KCD 코드 없이 한글 병명만 적힌 진단서가 더 흔하다.
    text = "상병명: 급성 기관지염"
    # Act
    entities = extract(DocType.DIAGNOSIS, text)
    # Assert — icd는 코드가 없으면 한글 병명으로 폴백하지 않고 None
    assert entities == {"diagnosis_name": "급성 기관지염", "icd": None}


def test_diagnosis_falls_back_across_split_label_lines() -> None:
    # Arrange — 실제 샘플: 라벨 셀이 "병 명"/"및"/"진 단"으로 여러 줄에 걸쳐
    # 쪼개지고, 값(병명)은 그다음 줄에 온다(표 양식, OCR 라인 분리).
    text = (
        "병 명\n및\n진 단\n만성 요통증\n"
        " - 요통증은 6개월 이상 지속되는 지속적인 통증으로, 원인을 명확히 알 수 없는 경우를 말함"
    )
    # Act
    entities = extract(DocType.DIAGNOSIS, text)
    # Assert — 값 줄만 잡고 다음 줄(부연 설명)은 삼키지 않는다.
    assert entities == {"diagnosis_name": "만성 요통증", "icd": None}


def test_diagnosis_name_none_when_no_kcd_and_no_label() -> None:
    # Arrange — KCD 코드도, 병명/진단명 라벨도 전혀 없는 텍스트.
    text = "환자는 정기 검진을 위해 내원하였다."
    # Act
    entities = extract(DocType.DIAGNOSIS, text)
    # Assert
    assert entities == {"diagnosis_name": None, "icd": None}


# ── 보험증권: insurer / product ─────────────────────────────────


def test_policy_extracts_insurer_and_product() -> None:
    # Arrange
    text = "보험회사: 한화생명\n상품명: 무배당 행복드림종합보험\n증권번호 A12345678"
    # Act
    entities = extract(DocType.POLICY, text)
    # Assert
    assert entities == {"insurer": "한화생명", "product": "무배당 행복드림종합보험"}


def test_policy_fields_none_when_absent() -> None:
    # Arrange
    text = "증권번호만 있고 회사·상품 표기가 없는 문서"
    # Act
    entities = extract(DocType.POLICY, text)
    # Assert
    assert entities == {"insurer": None, "product": None}


# ── 지급결과안내문: payout_amount(참고값) ───────────────────────


def test_payout_extracts_amount_reference() -> None:
    # Arrange
    text = "지급결정 안내\n지급 보험금 1,200,000원 입니다"
    # Act
    entities = extract(DocType.PAYOUT_NOTICE, text)
    # Assert — 콤마 제거 정수, 참고용
    assert entities["payout_amount"] == 1200000


def test_payout_amount_none_when_absent() -> None:
    # Arrange — 금액 표기 없음
    text = "지급결정 안내문이며 산정내역은 별첨 참조"
    # Act
    entities = extract(DocType.PAYOUT_NOTICE, text)
    # Assert
    assert entities["payout_amount"] is None


def test_payout_ignores_amount_without_context_keyword() -> None:
    # Arrange — 관련 키워드 없이 떨어진 숫자는 금액으로 단정하지 않음
    text = "접수 순번 7번, 페이지 3원본 보관. 별도 안내."
    # Act
    entities = extract(DocType.PAYOUT_NOTICE, text)
    # Assert
    assert entities["payout_amount"] is None


# ── 청구서: payout_amount ───────────────────────────────────────


def test_claim_extracts_amount_reference() -> None:
    # Arrange
    text = "청구금액 950,000원 청구합니다"
    # Act
    entities = extract(DocType.CLAIM, text)
    # Assert
    assert entities == {"payout_amount": 950000}


# ── 입퇴원확인서: admission_days(참고값) / surgery ──────────────


def test_hospitalization_cert_extracts_days_from_range_label() -> None:
    # Arrange — "입원기간" 한 라벨 아래 시작~종료 날짜가 함께 오는 형태.
    text = "입원확인서\n입원기간 2026-01-01부터 2026-01-05까지"
    # Act
    entities = extract(DocType.HOSPITALIZATION_CERT, text)
    # Assert
    assert entities == {"admission_days": 4, "surgery": None}


def test_hospitalization_cert_extracts_days_from_direct_label() -> None:
    # Arrange — "입원일수: N일" 직접 표기가 있으면 최우선으로 쓴다.
    text = "입원확인서\n입원일수: 7일"
    # Act
    entities = extract(DocType.HOSPITALIZATION_CERT, text)
    # Assert
    assert entities["admission_days"] == 7


def test_hospitalization_cert_extracts_days_from_separate_labels() -> None:
    # Arrange — 입원일/퇴원일이 서로 다른 라벨로 떨어져 있는 형태.
    text = "입원일자: 2026-02-10\n퇴원일자: 2026-02-15"
    # Act
    entities = extract(DocType.HOSPITALIZATION_CERT, text)
    # Assert
    assert entities["admission_days"] == 5


def test_hospitalization_cert_days_none_when_dates_absent() -> None:
    # Arrange — 날짜 표기가 전혀 없는 텍스트.
    text = "입원확인서\n환자는 경과가 양호하다."
    # Act
    entities = extract(DocType.HOSPITALIZATION_CERT, text)
    # Assert
    assert entities["admission_days"] is None


def test_hospitalization_cert_surgery_true_when_named() -> None:
    # Arrange
    text = "입원확인서\n수술명: 골절정복술"
    # Act
    entities = extract(DocType.HOSPITALIZATION_CERT, text)
    # Assert
    assert entities["surgery"] is True


def test_hospitalization_cert_surgery_false_when_negated() -> None:
    # Arrange — 라벨은 있지만 값이 "없음"이면 시행하지 않은 것으로 본다.
    text = "입원확인서\n수술명: 없음"
    # Act
    entities = extract(DocType.HOSPITALIZATION_CERT, text)
    # Assert
    assert entities["surgery"] is False


def test_hospitalization_cert_surgery_none_when_label_absent() -> None:
    # Arrange — 라벨 자체가 없으면 알 수 없음(True/False 단정 금지).
    text = "입원확인서\n입원일수: 3일"
    # Act
    entities = extract(DocType.HOSPITALIZATION_CERT, text)
    # Assert
    assert entities["surgery"] is None


def test_medical_receipt_returns_empty_entities() -> None:
    # Arrange
    text = "진료비계산서·영수증\n영수금액 47,500원"
    # Act
    entities = extract(DocType.MEDICAL_RECEIPT, text)
    # Assert
    assert entities == {}


# ── OTHER ───────────────────────────────────────────────────────


def test_other_returns_empty_entities() -> None:
    # Arrange
    text = "어떤 유형에도 속하지 않는 문서"
    # Act
    entities = extract(DocType.OTHER, text)
    # Assert
    assert entities == {}


# ── PII 누출 금지(핵심) ─────────────────────────────────────────


def test_pii_never_enters_entities_diagnosis() -> None:
    # Arrange — 이름·주민번호·전화가 섞인 진단서 텍스트
    text = (
        "환자 성명: 홍길동\n주민등록번호 901010-1234567\n연락처 010-1234-5678\n"
        "상병명: 발목 골절 (KCD S82.1)"
    )
    # Act
    entities = extract(DocType.DIAGNOSIS, text)
    # Assert — 도메인 값(KCD)만 들어가고 PII는 어떤 값에도 없음
    assert entities == {"diagnosis_name": "S82.1", "icd": "S82.1"}
    serialized = repr(entities)
    assert "홍길동" not in serialized
    assert "901010" not in serialized
    assert "1234-5678" not in serialized


def test_pii_never_enters_entities_diagnosis_label_fallback() -> None:
    # Arrange — KCD 코드 없이 병명 라벨만 있는 문서 + 이름·주민번호·전화(PII).
    text = (
        "환자의 성명 홍길동\n주민등록번호 901010-1234567\n연락처 010-1234-5678\n"
        "병 명\n및\n진 단\n만성 요통증"
    )
    # Act
    entities = extract(DocType.DIAGNOSIS, text)
    # Assert — 병명 값만 들어가고 PII는 어떤 값에도 없음
    assert entities == {"diagnosis_name": "만성 요통증", "icd": None}
    serialized = repr(entities)
    assert "홍길동" not in serialized
    assert "901010" not in serialized
    assert "1234-5678" not in serialized


def test_pii_never_enters_entities_policy() -> None:
    # Arrange — 계좌·이름이 섞인 증권 텍스트
    text = (
        "계약자 성명: 김철수\n계좌번호 110-234-567890\n보험회사: 삼성화재\n상품명: 무배당 안심보험"
    )
    # Act
    entities = extract(DocType.POLICY, text)
    # Assert
    assert entities == {"insurer": "삼성화재", "product": "무배당 안심보험"}
    serialized = repr(entities)
    assert "김철수" not in serialized
    assert "567890" not in serialized


def test_pii_never_enters_entities_hospitalization_cert() -> None:
    # Arrange — 이름·주민번호가 섞인 입퇴원확인서 텍스트
    text = (
        "환자 성명: 이샘플\n주민등록번호 901010-1234567\n"
        "입원기간 2026-01-01부터 2026-01-05까지\n수술명: 골절정복술"
    )
    # Act
    entities = extract(DocType.HOSPITALIZATION_CERT, text)
    # Assert — 입원일수·수술여부만 들어가고 PII는 어떤 값에도 없음
    assert entities == {"admission_days": 4, "surgery": True}
    serialized = repr(entities)
    assert "이샘플" not in serialized
    assert "901010" not in serialized
