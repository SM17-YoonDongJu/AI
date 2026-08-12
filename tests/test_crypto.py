"""core.crypto — PII 봉투(envelope) 복호화 계약 테스트 (§12: 경계 계약은 테스트로 고정).

백엔드와 합의한 바이트 레이아웃(version|aadScope|keyVersion|nonce|ciphertext‖tag)을
이쪽에서도 인코더로 재현해 왕복·AAD 바인딩·경계값을 고정한다. `get_pii_dek()`의 DB/KMS
분기는 asyncpg 풀·boto3가 필요해 이 파일 범위 밖(다른 통합 테스트에서 다룰 대상).
"""

import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from core.crypto import (
    HEADER_LEN,
    MIN_ENVELOPE_LEN,
    NONCE_LEN,
    decrypt_pii,
    maybe_decrypt,
    parse_key_version,
)
from core.exceptions import PiiCryptoError

_DEK = os.urandom(32)


def _seal(plaintext: str, table: str, column: str, *, key_version: int = 1) -> bytes:
    """테스트용 봉투 인코더 — crypto.py가 기대하는 레이아웃을 그대로 재현한다."""
    header = bytes([0x01, 0x01]) + key_version.to_bytes(2, "big")
    nonce = os.urandom(NONCE_LEN)
    aad = header + f"{table}:{column}".encode()
    ciphertext_and_tag = AESGCM(_DEK).encrypt(nonce, plaintext.encode("utf-8"), aad)
    return header + nonce + ciphertext_and_tag


def test_decrypt_pii_round_trip() -> None:
    # Arrange
    envelope = _seal("김민수", "user_claims", "description", key_version=7)

    # Act
    plaintext = decrypt_pii(envelope, "user_claims", "description", _DEK)

    # Assert
    assert plaintext == "김민수"
    assert parse_key_version(envelope) == 7


def test_decrypt_pii_empty_plaintext_round_trip() -> None:
    # Arrange: 평문 0바이트여도 헤더+nonce+태그(MIN_ENVELOPE_LEN)는 반드시 있다.
    envelope = _seal("", "user_claims", "description")

    # Act
    plaintext = decrypt_pii(envelope, "user_claims", "description", _DEK)

    # Assert
    assert plaintext == ""
    assert len(envelope) == MIN_ENVELOPE_LEN


def test_decrypt_pii_wrong_column_fails_tag_check() -> None:
    # Arrange: AAD가 컬럼에 바인딩되므로, 다른 컬럼으로 복호화 시도하면 태그 검증이 깨진다.
    envelope = _seal("김민수", "user_claims", "description")

    # Act / Assert
    try:
        decrypt_pii(envelope, "user_claims", "additional_information", _DEK)
        raise AssertionError("컬럼 불일치인데 복호화가 성공했다")
    except PiiCryptoError:
        pass


def test_decrypt_pii_wrong_table_fails_tag_check() -> None:
    # Arrange: AAD가 테이블에도 바인딩되므로, 같은 컬럼명이라도 다른 테이블이면 깨진다.
    envelope = _seal("보험사명", "user_insurances", "insurer_name")

    # Act / Assert
    try:
        decrypt_pii(envelope, "reports", "insurer_name", _DEK)
        raise AssertionError("테이블 불일치인데 복호화가 성공했다")
    except PiiCryptoError:
        pass


def test_decrypt_pii_wrong_dek_fails_tag_check() -> None:
    # Arrange
    envelope = _seal("김민수", "user_claims", "description")
    other_dek = os.urandom(32)

    # Act / Assert
    try:
        decrypt_pii(envelope, "user_claims", "description", other_dek)
        raise AssertionError("키가 다른데 복호화가 성공했다")
    except PiiCryptoError:
        pass


def test_decrypt_pii_rejects_unsupported_version() -> None:
    # Arrange: version 바이트만 지원 밖 값(0x02)으로 바꾼다.
    envelope = bytearray(_seal("김민수", "user_claims", "description"))
    envelope[0] = 0x02

    # Act / Assert
    try:
        decrypt_pii(bytes(envelope), "user_claims", "description", _DEK)
        raise AssertionError("미지원 버전인데 복호화가 성공했다")
    except PiiCryptoError:
        pass


def test_decrypt_pii_rejects_short_envelope() -> None:
    # Arrange: 헤더+nonce+태그 최소치(MIN_ENVELOPE_LEN)에 못 미치는 값.
    short = b"\x01\x01\x00\x01" + b"\x00" * (HEADER_LEN + NONCE_LEN)

    # Act / Assert
    try:
        decrypt_pii(short, "user_claims", "description", _DEK)
        raise AssertionError("짧은 봉투인데 복호화가 성공했다")
    except PiiCryptoError:
        pass


def test_maybe_decrypt_dispatches_by_type() -> None:
    # Arrange
    envelope = _seal("김민수", "user_claims", "description")

    # Act / Assert: bytes=암호화됨(복호화), str=미배포(통과), None=NULL(통과)
    assert maybe_decrypt(envelope, "user_claims", "description", _DEK) == "김민수"
    assert maybe_decrypt("아직 평문", "user_claims", "description", _DEK) == "아직 평문"
    assert maybe_decrypt(None, "user_claims", "description", _DEK) is None


def test_maybe_decrypt_rejects_unexpected_type() -> None:
    # Arrange: 스키마가 바뀌어 asyncpg가 bytes/str/None 이외를 돌려주는 신호.
    # Act / Assert
    try:
        maybe_decrypt(12345, "user_claims", "description", _DEK)  # type: ignore[arg-type]
        raise AssertionError("예상 밖 타입인데 통과했다")
    except TypeError:
        pass
