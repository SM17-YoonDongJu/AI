"""워커·모듈 간 메시지 계약 (단일 출처).

Spring↔Python·워커 간 Kafka 페이로드를 pydantic v2로 정의·검증한다. 이 모듈은
`.claude/docs/contracts.md` 스펙의 코드 구현이며, 변경 시 **발행자·소비자 양쪽과 합의**한다.

규약: UTF-8 JSON · 시각은 ISO-8601(UTC) 문자열 · 식별자는 UUIDv4 문자열 · PII 평문 금지.
"""

from enum import StrEnum

from pydantic import BaseModel

# ── Kafka 토픽 상수 (계약 기본값; 런타임 override는 core.config) ──
OCR_JOB_TOPIC = "ocr-job-queue"
REPORT_JOB_TOPIC = "report-job"


class DocType(StrEnum):
    """문서 유형 분류 결과."""

    DIAGNOSIS = "diagnosis"  # 진단서
    POLICY = "policy"  # 보험증권
    PAYOUT_NOTICE = "payout_notice"  # 지급결과안내문
    CLAIM = "claim"  # 청구서
    OTHER = "other"  # 기타


class OcrJob(BaseModel):
    """Spring → ocr-job-queue → ocr_worker. 업로드 시 발행되는 OCR 작업."""

    job_id: str  # UUID, 멱등 키
    s3_key: str  # 업로드 파일 S3 키(파일명 UUID 치환)
    content_type: str  # application/pdf | image/jpeg | image/png | image/tiff
    user_ref: str  # 내부 사용자 참조(PII 아님)
    doc_type_hint: str | None = None  # 업로드 시 사용자가 고른 유형 힌트
    claim_id: str | None = None  # USER_CLAIMS.id 참조(옵셔널) — report_worker가 조회
    uploaded_at: str  # ISO-8601 (UTC)


class ReportJob(BaseModel):
    """ocr_worker → report-job → report_worker. OCR·마스킹 완료 후 리포트 생성 트리거."""

    report_id: str  # UUID(신규 생성), 멱등 키
    ocr_result_id: str  # ocr_results.id 참조
    job_id: str  # 원 OCR 작업 추적용
    doc_type: DocType  # 분류된 문서 유형
    user_ref: str  # 사용자 참조
    claim_id: str | None = None  # USER_CLAIMS.id 패스스루(옵셔널)
    created_at: str  # ISO-8601 (UTC)
