"""PII 마스킹(#18) 단위 테스트 — 정규식 레이어는 완전히 결정적(노션 §3).

NER 레이어는 transformers/torch(ner extra)가 필요하므로 미설치 환경에서는
스킵한다. 정규식·병합·검증은 표준 라이브러리만으로 항상 검증한다.
"""

import pytest

from ocr_worker.masking import (
    MaskingError,
    PiiLabel,
    RegexDetector,
    Span,
    apply_mask,
    assert_no_residual,
    find_residual_pii,
    mask,
    merge_overlaps,
)

# ── 정규식 디텍터: 정형 PII를 가린다 ─────────────────────────────


def test_masks_resident_registration_number() -> None:
    text = "환자 주민등록번호 901010-1234567 입니다"
    assert mask(text) == "환자 주민등록번호 [주민등록번호] 입니다"


def test_foreign_registration_number_gets_distinct_label() -> None:
    # 7번째 자리 5 → 외국인등록번호
    assert mask("등록번호 901010-5234567") == "등록번호 [외국인등록번호]"


def test_masks_phone_email_card() -> None:
    masked = mask("연락처 010-1234-5678, 이메일 hong@example.com, 카드 1234-5678-9012-3456")
    assert "[전화번호]" in masked
    assert "[이메일]" in masked
    assert "[카드번호]" in masked
    assert "@example.com" not in masked


def test_masks_account_only_with_context_keyword() -> None:
    # 키워드(계좌)가 있으면 가린다
    assert "[계좌번호]" in mask("입금 계좌 110-234-567890 으로")


def test_amount_is_not_masked_as_account() -> None:
    # 금액(콤마)·문맥 키워드 없는 숫자는 계좌로 오인하지 않는다(화이트리스트 보호)
    assert mask("보험금 1,200,000원 지급") == "보험금 1,200,000원 지급"


def test_address_is_masked() -> None:
    masked = mask("주소 서울특별시 강남구 테헤란로 123 입니다")
    assert "[주소]" in masked
    assert "테헤란로" not in masked


def test_hospital_name_is_not_masked() -> None:
    # "서울아산병원"의 "서울"이 주소로 트리거되면 안 됨(화이트리스트 핵심)
    text = "진단병원: 서울아산병원, 진단명: 급성 기관지염 (J20.9)"
    assert mask(text) == text


def test_clinical_whitelist_preserved() -> None:
    # 진단명·KCD코드·입원기간·금액은 그대로 유지
    text = "입원일자 2024-01-05 퇴원일자 2024-01-20, KCD S82.1, 수술명 골절정복술"
    assert mask(text) == text


def test_masks_medical_license_number() -> None:
    masked = mask("발급 의사 면허번호: 제45678호")
    assert "[면허번호]" in masked
    assert "45678" not in masked


def test_masks_policy_number() -> None:
    masked = mask("증권번호: A12345678-01 / 보험사: 한화생명")
    assert "[증권번호]" in masked
    assert "A12345678" not in masked
    assert "한화생명" in masked  # 보험사명은 유지(화이트리스트)


def test_masks_doctor_name_after_keyword() -> None:
    assert mask("작성 의사: 김철수") == "작성 의사: [이름]"


def test_license_field_label_not_masked_as_name() -> None:
    # '의사 면허번호'의 '면허번호'(필드 라벨)가 이름으로 오인되면 안 됨
    masked = mask("발급 의사 면허번호: 제45678호")
    assert "[이름]" not in masked
    assert "[면허번호]" in masked


def test_masks_signature_line_name() -> None:
    assert mask("주치의 이영희 (서명)") == "주치의 [이름] (서명)"


def test_doctor_opinion_not_masked() -> None:
    # '의사소견'은 화이트리스트(유지) — 이름 룰에 오인되면 안 됨
    text = "의사소견: 6주간 안정 가료를 요함"
    assert mask(text) == text


# ── 병합·치환 로직 ──────────────────────────────────────────────


def test_merge_overlaps_widens_and_dedupes() -> None:
    spans = [
        Span(0, 5, PiiLabel.NAME, source="ner"),
        Span(3, 10, PiiLabel.ADDRESS, source="regex"),  # 더 길다 → 대표 라벨
    ]
    merged = merge_overlaps(spans)
    assert len(merged) == 1
    assert merged[0].start == 0
    assert merged[0].end == 10
    assert merged[0].label is PiiLabel.ADDRESS


def test_adjacent_spans_not_merged() -> None:
    # 맞닿은(겹치지 않은) 서로 다른 PII는 각각 치환
    text = "홍길동010"
    spans = [
        Span(0, 3, PiiLabel.NAME, source="ner"),
        Span(3, 6, PiiLabel.PHONE, source="regex"),
    ]
    assert apply_mask(text, spans) == "[이름][전화번호]"


def test_apply_mask_is_index_safe_back_to_front() -> None:
    # 토큰 길이가 원문과 달라도 인덱스가 밀리지 않아야 함
    text = "a 901010-1234567 b hong@x.com c"
    out = mask(text)
    assert out == "a [주민등록번호] b [이메일] c"


def test_no_pii_returns_unchanged() -> None:
    text = "진단명 급성 기관지염, 치료기간 2주"
    assert mask(text) == text


# ── 검증 (Tier 1 잔류 스캔) ─────────────────────────────────────


def test_find_residual_detects_unmasked_rrn() -> None:
    leaks = find_residual_pii("주민번호 901010-1234567 누락")
    assert any(s.label is PiiLabel.RRN for s in leaks)


def test_assert_no_residual_passes_on_masked_text() -> None:
    assert_no_residual(mask("주민등록번호 901010-1234567"))  # 예외 없음


def test_assert_no_residual_raises_on_leak() -> None:
    with pytest.raises(MaskingError):
        assert_no_residual("계좌 미마스킹 901010-1234567")


# ── 디텍터 직접 호출(이미지 마스킹 B안 스팬 공유) ───────────────


def test_regex_detector_returns_spans_with_offsets() -> None:
    spans = RegexDetector().detect("주민 901010-1234567")
    assert len(spans) == 1
    span = spans[0]
    assert span.label is PiiLabel.RRN
    assert span.source == "regex"
    # 스팬 좌표가 원문 PII를 정확히 가리킴(bbox 매핑 전제)
    assert "901010" in "주민 901010-1234567"[span.start : span.end]


# ── NER (옵션, ner extra 설치 시에만) ───────────────────────────


def test_ner_detector_masks_name_when_available() -> None:
    transformers = pytest.importorskip("transformers")  # noqa: F841
    from ocr_worker.masking import Masker

    masker = Masker(use_ner=True)
    masked = masker.mask("환자 성명: 홍길동")
    # 이름이 가려졌는지(모델 다운로드 필요 — 통합 환경에서만 의미)
    assert "[이름]" in masked or "홍길동" not in masked
