"""contracts.py 경계 계약 테스트.

계약(SQS 큐 페이로드·RAG·가드레일 모델)을 코드로 고정한다. 발행자·소비자가 같은
스키마를 쓰는지, contracts.md 예시가 그대로 검증되는지 확인한다.
"""

import pytest
from pydantic import ValidationError

from core.contracts import (
    Citation,
    DocType,
    OcrJob,
    OutputGuardResult,
    RagResult,
    ReportJob,
)


def test_ocr_job_validates_contract_example() -> None:
    # Arrange: contracts.md §1 JSON 예시(소유권/fan-in 필드 포함)
    payload = {
        "job_id": "8f1c2d3e-a1",
        "s3_key": "uploads/8f1c2d3e.pdf",
        "content_type": "application/pdf",
        "user_ref": "3f0e-uid",
        "doc_type_hint": "diagnosis",
        "claim_id": "c1a1-cl",
        "report_id": "r2b2-rp",
        "attachment_id": "a3c3-at",
        "doc_index": 1,
        "doc_total": 3,
        "uploaded_at": "2026-06-17T05:30:00Z",
    }

    # Act
    job = OcrJob.model_validate(payload)

    # Assert
    assert job.job_id == "8f1c2d3e-a1"
    assert job.report_id == "r2b2-rp"
    assert job.attachment_id == "a3c3-at"
    assert job.doc_index == 1
    assert job.doc_total == 3
    assert job.uploaded_at.isoformat() == "2026-06-17T05:30:00+00:00"


def test_ocr_job_rejects_unknown_content_type() -> None:
    # Arrange: content_type만 계약 외 — 나머지 필수 필드는 채운다
    payload = {
        "job_id": "x",
        "s3_key": "y",
        "content_type": "text/plain",  # 계약 외
        "user_ref": "u",
        "report_id": "r",
        "attachment_id": "a",
        "uploaded_at": "2026-06-17T05:30:00Z",
    }

    # Act / Assert
    with pytest.raises(ValidationError):
        OcrJob.model_validate(payload)


def test_ocr_job_requires_report_and_attachment_refs() -> None:
    # Arrange: report_id·attachment_id 누락(구 계약 페이로드) — UPDATE 대상 참조 키는 필수
    payload = {
        "job_id": "j",
        "s3_key": "s",
        "content_type": "application/pdf",
        "user_ref": "u",
        "uploaded_at": "2026-06-17T05:30:00Z",
    }

    # Act / Assert: 참조 키 누락은 진입점에서 거른다(→ DLQ)
    with pytest.raises(ValidationError):
        OcrJob.model_validate(payload)


def test_ocr_job_doc_index_total_default_to_none() -> None:
    # Arrange: doc_index·doc_total 없이도 통과(옵셔널)
    payload = {
        "job_id": "j",
        "s3_key": "s",
        "content_type": "image/png",
        "user_ref": "u",
        "report_id": "r",
        "attachment_id": "a",
        "uploaded_at": "2026-06-17T05:30:00Z",
    }

    # Act
    job = OcrJob.model_validate(payload)

    # Assert
    assert job.doc_index is None
    assert job.doc_total is None


def test_report_job_doc_type_is_enum() -> None:
    # Arrange
    payload = {
        "report_id": "b2a9-77",
        "ocr_result_id": "c3d8-12",
        "job_id": "8f1c2d3e-a1",
        "doc_type": "diagnosis",
        "user_ref": "u_4821",
        "created_at": "2026-06-17T05:31:10Z",
    }

    # Act
    job = ReportJob.model_validate(payload)

    # Assert
    assert job.doc_type is DocType.DIAGNOSIS


def test_report_job_accepts_hospitalization_cert_and_medical_receipt() -> None:
    # Arrange — 신규 DocType 2종이 계약에서 유효한지(다중 항목 표 문서)
    base_payload = {
        "report_id": "r",
        "ocr_result_id": "o",
        "job_id": "j",
        "user_ref": "u",
        "created_at": "2026-06-17T05:31:10Z",
    }

    # Act
    hospitalization = ReportJob.model_validate(
        {**base_payload, "doc_type": "hospitalization_cert"}
    )
    receipt = ReportJob.model_validate({**base_payload, "doc_type": "medical_receipt"})

    # Assert
    assert hospitalization.doc_type is DocType.HOSPITALIZATION_CERT
    assert receipt.doc_type is DocType.MEDICAL_RECEIPT


def test_report_job_rejects_unknown_doc_type() -> None:
    # Arrange
    payload = {
        "report_id": "r",
        "ocr_result_id": "o",
        "job_id": "j",
        "doc_type": "invoice",  # enum 밖
        "user_ref": "u",
        "created_at": "2026-06-17T05:31:10Z",
    }

    # Act / Assert
    with pytest.raises(ValidationError):
        ReportJob.model_validate(payload)


def test_report_job_ocr_quality_defaults_to_ok() -> None:
    # Arrange — ocr_quality 생략 시 기본값
    payload = {
        "report_id": "r",
        "ocr_result_id": "o",
        "job_id": "j",
        "doc_type": "diagnosis",
        "user_ref": "u",
        "created_at": "2026-06-17T05:31:10Z",
    }

    # Act
    job = ReportJob.model_validate(payload)

    # Assert
    assert job.ocr_quality == "ok"


def test_report_job_accepts_needs_reupload() -> None:
    # Arrange
    payload = {
        "report_id": "r",
        "ocr_result_id": "o",
        "job_id": "j",
        "doc_type": "diagnosis",
        "user_ref": "u",
        "created_at": "2026-06-17T05:31:10Z",
        "ocr_quality": "needs_reupload",
    }

    # Act
    job = ReportJob.model_validate(payload)

    # Assert
    assert job.ocr_quality == "needs_reupload"


def test_report_job_rejects_unknown_ocr_quality() -> None:
    # Arrange
    payload = {
        "report_id": "r",
        "ocr_result_id": "o",
        "job_id": "j",
        "doc_type": "diagnosis",
        "user_ref": "u",
        "created_at": "2026-06-17T05:31:10Z",
        "ocr_quality": "rejected",  # enum 밖
    }

    # Act / Assert
    with pytest.raises(ValidationError):
        ReportJob.model_validate(payload)


def test_citation_has_no_source_url() -> None:
    # Arrange / Act
    fields = set(Citation.model_fields)

    # Assert: 보유 테이블에 출처 URL 컬럼 없음 → 계약에서 제외
    assert fields == {"clause_no", "exhibit"}


def test_rag_result_round_trips() -> None:
    # Arrange
    result = RagResult(
        ranked_chunks=[],
        citations=[Citation(clause_no="제3조", exhibit=None)],
    )

    # Act
    restored = RagResult.model_validate_json(result.model_dump_json())

    # Assert
    assert restored.citations[0].clause_no == "제3조"


def test_output_guard_result_defaults_to_no_failures() -> None:
    # Arrange / Act
    result = OutputGuardResult(final_text="...", judge_failures=[])

    # Assert
    assert result.judge_failures == []
