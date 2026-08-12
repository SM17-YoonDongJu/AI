"""_extract_coverages — user_insurances.coverages(암호화, bytea) 특약명 추출 테스트.

백엔드 PR #222에서 이 컬럼이 text[]→bytea로 바뀌었다: 배열 전체를 JSON 문자열로
직렬화한 뒤 통째로 한 번 암호화한다(AAD=user_insurances:coverages). 원소별 봉투가
아니다 — 예전엔 원소별로 잘못 가정해서 bytea를 바이트 단위로 순회하며 TypeError가
나던 버그가 실제로 머지된 적 있어(#54), 그 회귀를 여기서 고정한다.
"""

import json
import os

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from core.exceptions import PiiCryptoError
from report_worker.nodes.agents import _extract_coverages

_DEK = os.urandom(32)


def _seal(plaintext: str, table: str = "user_insurances", column: str = "coverages") -> bytes:
    header = bytes([0x01, 0x01, 0x00, 0x01])
    nonce = os.urandom(12)
    aad = header + f"{table}:{column}".encode()
    ciphertext_and_tag = AESGCM(_DEK).encrypt(nonce, plaintext.encode("utf-8"), aad)
    return header + nonce + ciphertext_and_tag


def _seal_coverages(coverages: list) -> bytes:
    return _seal(json.dumps(coverages, ensure_ascii=False))


def test_decrypts_and_parses_coverage_list() -> None:
    # Arrange
    ins = {"coverages": _seal_coverages(["상해후유장해", "골절진단비"])}

    # Act
    result = _extract_coverages(ins, _DEK)

    # Assert
    assert result == ["상해후유장해", "골절진단비"]


def test_returns_empty_list_when_ins_is_none() -> None:
    # Arrange / Act / Assert
    assert _extract_coverages(None, _DEK) == []


def test_returns_empty_list_when_coverages_missing() -> None:
    # Arrange
    ins = {"coverages": None}

    # Act / Assert
    assert _extract_coverages(ins, _DEK) == []


def test_filters_out_falsy_entries() -> None:
    # Arrange
    ins = {"coverages": _seal_coverages(["", None, "실손의료비"])}

    # Act
    result = _extract_coverages(ins, _DEK)

    # Assert
    assert result == ["실손의료비"]


def test_returns_empty_list_on_malformed_json_after_decrypt() -> None:
    # Arrange: 복호화는 되는데 JSON 배열이 아닌 경우 — 크래시하지 않고 빈 리스트로.
    ins = {"coverages": _seal("not a json array")}

    # Act / Assert
    assert _extract_coverages(ins, _DEK) == []


def test_wrong_dek_raises_pii_crypto_error_not_swallowed() -> None:
    """복호화 실패는 데이터 품질 문제가 아니라 보안 문제라 삼키지 않고 전파해야
    한다 — 호출부(load_context)가 차단 경로로 보낸다."""
    # Arrange
    ins = {"coverages": _seal_coverages(["상해후유장해"])}
    other_dek = os.urandom(32)

    # Act / Assert
    with pytest.raises(PiiCryptoError):
        _extract_coverages(ins, other_dek)


def test_bytea_is_not_iterated_element_by_element() -> None:
    """회귀 테스트 — #54에서 머지됐던 실제 버그: coverages를 text[]로 착각해
    bytea를 `for cov in ins["coverages"]`로 순회하면 바이트(int) 단위로 쪼개져
    maybe_decrypt에 int가 들어가 TypeError가 났다. 지금은 통째로 한 번만
    복호화해야 하므로, 바이트 수가 많아도(=원소가 많다고 착각할 길이) 정상 동작해야
    한다."""
    # Arrange: 특약이 많아서 암호문 바이트 길이도 충분히 긴 경우.
    coverages = [f"특약{i}" for i in range(20)]
    ins = {"coverages": _seal_coverages(coverages)}

    # Act
    result = _extract_coverages(ins, _DEK)

    # Assert
    assert result == coverages
