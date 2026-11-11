"""_extract_claim_diagnosis — user_claims.details(암호화, bytea) 진단명 추출 테스트.

백엔드 PR #236(이슈 #235)에서 details가 jsonb→bytea로 바뀌었다: ClaimDetails를 JSON
문자열로 직렬화한 뒤 통째로 한 번 암호화한다(AAD=user_claims:details). ClaimDetails는
사고유형별 7개 구현체로 나뉘지만 diagnosis: string[]는 전 타입 공통 필드다(Notion
ClaimDetails 구조 문서). 타입 분기 없이 그 필드만 뽑는 경로를 고정한다.
"""

import json
import os

import pytest
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from core.exceptions import PiiCryptoError
from report_worker.nodes.agents import _extract_claim_diagnosis

_DEK = os.urandom(32)


def _seal(plaintext: str, table: str = "user_claims", column: str = "details") -> bytes:
    """테스트용 봉투 인코더 — core.crypto가 기대하는 레이아웃을 그대로 재현한다."""
    header = bytes([0x01, 0x01, 0x00, 0x01])
    nonce = os.urandom(12)
    aad = header + f"{table}:{column}".encode()
    ciphertext_and_tag = AESGCM(_DEK).encrypt(nonce, plaintext.encode("utf-8"), aad)
    return header + nonce + ciphertext_and_tag


def _seal_details(details: dict) -> bytes:
    return _seal(json.dumps(details, ensure_ascii=False))


def test_decrypts_and_extracts_multiple_diagnosis_names() -> None:
    # Arrange
    claim = {"details": _seal_details({"type": "traffic", "diagnosis": ["요추 염좌", "경추 염좌"]})}

    # Act
    result = _extract_claim_diagnosis(claim, _DEK)

    # Assert
    assert result == "요추 염좌, 경추 염좌"


def test_returns_none_when_claim_is_none() -> None:
    # Arrange / Act / Assert
    assert _extract_claim_diagnosis(None, _DEK) is None


def test_returns_none_when_details_missing() -> None:
    # Arrange
    claim = {"details": None}

    # Act / Assert
    assert _extract_claim_diagnosis(claim, _DEK) is None


def test_returns_none_when_diagnosis_key_absent() -> None:
    # Arrange: medical_indemnity 등 다른 확장 필드만 있고 diagnosis가 빠진 경우.
    claim = {"details": _seal_details({"type": "fire", "hospitalizations": []})}

    # Act / Assert
    assert _extract_claim_diagnosis(claim, _DEK) is None


def test_returns_none_on_malformed_json_after_decrypt() -> None:
    # Arrange: 복호화는 되는데 JSON이 아닌 문자열인 경우(데이터 품질 문제).
    claim = {"details": _seal("{not valid json")}

    # Act / Assert: 크래시하지 않고 reports.treatment 폴백으로 넘어갈 수 있게 None.
    assert _extract_claim_diagnosis(claim, _DEK) is None


def test_filters_out_falsy_diagnosis_entries() -> None:
    # Arrange
    claim = {"details": _seal_details({"type": "other", "diagnosis": ["", None, "타박상"]})}

    # Act
    result = _extract_claim_diagnosis(claim, _DEK)

    # Assert
    assert result == "타박상"


def test_wrong_dek_raises_pii_crypto_error_not_swallowed() -> None:
    """복호화 실패(태그 불일치)는 데이터 품질 문제가 아니라 보안 문제라 이 함수가
    삼키지 않고 그대로 전파해야 한다 — 호출부(load_context)가 차단 경로로 보낸다."""
    # Arrange
    claim = {"details": _seal_details({"type": "traffic", "diagnosis": ["요추 염좌"]})}
    other_dek = os.urandom(32)

    # Act / Assert
    with pytest.raises(PiiCryptoError):
        _extract_claim_diagnosis(claim, other_dek)


def test_wrong_aad_raises_pii_crypto_error() -> None:
    """AAD가 다른(예: 다른 컬럼으로 잘못 봉인된) 봉투도 태그 검증에서 걸러야 한다."""
    # Arrange: additional_information용으로 봉인된 값을 details로 읽으려는 상황.
    claim = {
        "details": _seal(
            json.dumps({"type": "traffic", "diagnosis": ["요추 염좌"]}),
            table="user_claims",
            column="additional_information",
        )
    }

    # Act / Assert
    with pytest.raises(PiiCryptoError):
        _extract_claim_diagnosis(claim, _DEK)


def test_invalid_tag_is_wrapped_as_pii_crypto_error() -> None:
    # Arrange: crypto.decrypt_pii가 InvalidTag를 PiiCryptoError로 통일해 던지는지 확인.
    claim = {"details": _seal_details({"type": "traffic", "diagnosis": ["요추 염좌"]})}
    other_dek = os.urandom(32)

    # Act
    try:
        _extract_claim_diagnosis(claim, other_dek)
        raise AssertionError("키가 다른데 복호화가 성공했다")
    except PiiCryptoError as e:
        # InvalidTag가 원인으로 체이닝돼 있어야 한다(core.crypto.decrypt_pii 계약).
        assert isinstance(e.__cause__, InvalidTag)
