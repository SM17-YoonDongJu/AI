"""_extract_claim_diagnosis — user_claims.details(jsonb) 진단명 추출 테스트.

ClaimDetails는 사고유형별 7개 구현체로 나뉘지만 diagnosis: string[]는 전 타입 공통
필드다(Notion ClaimDetails 구조 문서). 타입 분기 없이 그 필드만 뽑는 경로를 고정한다.
"""

import json

from report_worker.nodes.agents import _extract_claim_diagnosis


def test_extracts_and_joins_multiple_diagnosis_names() -> None:
    # Arrange
    claim = {"details": {"type": "traffic", "diagnosis": ["요추 염좌", "경추 염좌"]}}

    # Act
    result = _extract_claim_diagnosis(claim)

    # Assert
    assert result == "요추 염좌, 경추 염좌"


def test_accepts_details_as_json_string() -> None:
    # Arrange: asyncpg가 jsonb 코덱 미등록 시 str로 돌려주는 경우를 재현.
    claim = {"details": json.dumps({"type": "cancer_diagnosis", "diagnosis": ["위암"]})}

    # Act
    result = _extract_claim_diagnosis(claim)

    # Assert
    assert result == "위암"


def test_returns_none_when_claim_is_none() -> None:
    # Arrange / Act / Assert
    assert _extract_claim_diagnosis(None) is None


def test_returns_none_when_details_missing() -> None:
    # Arrange
    claim = {"details": None}

    # Act / Assert
    assert _extract_claim_diagnosis(claim) is None


def test_returns_none_when_diagnosis_key_absent() -> None:
    # Arrange: medical_indemnity 등 다른 확장 필드만 있고 diagnosis가 빠진 경우.
    claim = {"details": {"type": "fire", "hospitalizations": []}}

    # Act / Assert
    assert _extract_claim_diagnosis(claim) is None


def test_returns_none_on_malformed_json_string() -> None:
    # Arrange
    claim = {"details": "{not valid json"}

    # Act / Assert: 크래시하지 않고 reports.treatment 폴백으로 넘어갈 수 있게 None.
    assert _extract_claim_diagnosis(claim) is None


def test_filters_out_falsy_diagnosis_entries() -> None:
    # Arrange
    claim = {"details": {"type": "other", "diagnosis": ["", None, "타박상"]}}

    # Act
    result = _extract_claim_diagnosis(claim)

    # Assert
    assert result == "타박상"
