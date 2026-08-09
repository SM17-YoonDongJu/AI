"""워커·모듈 간 통합 경계 계약 (단일 출처).

`.claude/docs/contracts.md`를 코드화한 것이다. 이 모듈은 **데이터 모델만** 정의한다
(함수 시그니처 search/guard_*/chat/embed 는 각 모듈 소관). 발행자·소비자가 동일한
스키마로 직렬화/검증하도록 강제해 통합 버그(계약 불일치)를 진입점에서 차단한다.

규약:
- 모든 메시지는 UTF-8 JSON, 시각은 ISO-8601(UTC), 식별자는 UUIDv4 문자열.
- PII는 계약에 평문으로 싣지 않는다(마스킹된 값만).
- 변경 시 contracts.md → 본 파일 → 관련 테스트 순으로 갱신하고 양쪽 담당자와 합의한다.
"""

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel

__all__ = [
    "OCR_JOB_TOPIC",
    "REPORT_JOB_TOPIC",
    "Chunk",
    "Citation",
    "ContentType",
    "DocType",
    "InputGuardResult",
    "OcrJob",
    "OutputGuardResult",
    "RagResult",
    "ReportJob",
]

# Kafka 토픽명 — 발행부(ocr_worker)·소비부(report_worker) 공유 상수.
OCR_JOB_TOPIC = "ocr-job-queue"
REPORT_JOB_TOPIC = "report-job"


# 업로드 허용 MIME 타입. Spring 게이트웨이가 검증하지만 워커도 진입점에서 재확인한다.
type ContentType = Literal[
    "application/pdf",
    "image/jpeg",
    "image/png",
    "image/tiff",
]


class DocType(StrEnum):
    """분류된 문서 유형. `ReportJob.doc_type` 및 `ocr_results.doc_type`에 공유된다."""

    DIAGNOSIS = "diagnosis"  # 진단서
    POLICY = "policy"  # 보험증권
    PAYOUT_NOTICE = "payout_notice"  # 지급결과안내문
    CLAIM = "claim"  # 청구서
    HOSPITALIZATION_CERT = "hospitalization_cert"  # 입퇴원확인서
    MEDICAL_RECEIPT = "medical_receipt"  # 진료비계산서·영수증
    OTHER = "other"  # 기타


# --------------------------------------------------------------------------- #
# Kafka 토픽 페이로드
# --------------------------------------------------------------------------- #


class OcrJob(BaseModel):
    """Spring → `ocr-job-queue` → `ocr_worker`. 문서 1건마다 발행되는 OCR 작업.

    토픽 `ocr-job-queue`, 파티션 키 `job_id`, at-least-once + `job_id` 멱등 처리.

    소유권(2026-07-10, 발행측 `OcrJob.java` 정본): Spring이 `reports`·`report_attachments`
    shell 행을 먼저 생성하고, 워커는 OCR·AI 결과로 그 행을 UPDATE(생성 아님)한다 →
    `report_id`·`attachment_id`가 그 참조 키다. 한 청구의 문서를 1건씩 발행하므로
    `doc_index`/`doc_total`로 워커가 리포트 생성(fan-in) 시점을 판별한다.
    """

    job_id: str  # OCR 작업 식별자(UUID). 멱등 키
    s3_key: str  # 업로드 파일 S3 키(파일명은 UUID로 치환됨)
    content_type: ContentType
    user_ref: str  # 사용자 참조(내부 식별자, PII 아님)
    doc_type_hint: str | None = None  # 업로드 시 사용자가 고른 문서 유형 힌트
    claim_id: str | None = None  # USER_CLAIMS.id 참조(옵셔널) — report_worker가 조회
    report_id: str  # REPORTS.id(UUID) — 결과 UPDATE 대상(Spring이 shell 생성)
    attachment_id: str  # REPORT_ATTACHMENTS.id(UUID) — 결과 UPDATE 대상
    doc_index: int | None = None  # 청구 내 문서 순번(1-based)
    doc_total: int | None = None  # 청구 총 문서 수 — 워커의 fan-in(리포트 생성) 판별용
    uploaded_at: datetime  # 업로드 시각(UTC, ISO-8601)


class ReportJob(BaseModel):
    """`ocr_worker` → `report-job` → `report_worker`. OCR·마스킹 완료 후 리포트 트리거.

    토픽 `report-job`, 파티션 키 `report_id`, `report_id` 멱등 처리.
    """

    report_id: str  # 리포트 식별자(UUID, 신규 생성). 멱등 키
    ocr_result_id: str  # `ocr_results.id` 참조(UUID)
    job_id: str  # 원 OCR 작업 추적용(UUID)
    doc_type: DocType
    user_ref: str  # 사용자 참조
    claim_id: str | None = None  # USER_CLAIMS.id 패스스루(옵셔널)
    created_at: datetime  # 발행 시각(UTC, ISO-8601)
    # ocr_worker의 자동 품질 판정(저신뢰도 + 이름/도메인 정보 미검출). needs_reupload면
    # report_worker가 리포트 생성을 건너뛰어야 한다는 신호 — 실제 사용자 알림은 게이트웨이 몫.
    ocr_quality: Literal["ok", "needs_reupload"] = "ok"


# --------------------------------------------------------------------------- #
# RAG 모듈 (`src/rag`) — report_worker·chatbot이 함수 호출로 사용
# --------------------------------------------------------------------------- #


class Chunk(BaseModel):
    """Hybrid RAG 검색 결과 청크. 임베딩 차원은 1024 고정(config.EMBEDDING_DIM)."""

    text: str  # 청크 원문 (POLICY_CHUNKS.content / CASE_CHUNKS.content)
    # 검색한 소스 테이블로 부여되는 파생값. 현재 유효값: "terms"(POLICY_CHUNKS) ·
    # "case"(CASE_CHUNKS). "level"(장해분류)·"medical"(수가·KCD)은 테이블 미존재 → 향후 확장.
    namespace: str
    score: float  # RRF 통합 점수
    source_ref: str  # 원문 위치 참조 (chunk_id)


class Citation(BaseModel):
    """인용 근거(메타데이터 역추적). 출처 URL 컬럼은 보유 테이블에 없어 계약에서 제외."""

    clause_no: str | None = None  # 조항/사례 번호 (article_number / case_number)
    exhibit: str | None = None  # 별표·항목 (POLICY_CHUNKS.section, 있으면)


class RagResult(BaseModel):
    """`rag.search`의 반환 계약. 비신체보험 쿼리는 빈 결과로 반환된다."""

    ranked_chunks: list[Chunk]
    citations: list[Citation]


# --------------------------------------------------------------------------- #
# 가드레일 모듈 (`src/guardrail`) — 단계별 함수 결과 모델
# --------------------------------------------------------------------------- #


class InputGuardResult(BaseModel):
    """입력 가드레일 결과. PII 마스킹 + 도메인 외 질문 차단."""

    masked_text: str  # PII 마스킹된 텍스트(주민번호 앞 6자리만 보존 등)
    blocked: bool  # 도메인 외 질문 차단 여부
    reason: str | None = None  # 차단 사유


class OutputGuardResult(BaseModel):
    """출력 가드레일 결과. 법적 고지문 삽입 + (리포트 한정) LLM Judge 인용 검증."""

    final_text: str  # 고지문 삽입된 최종 텍스트
    judge_failures: list[str]  # 인용 검증 실패 섹션(리포트만, run_judge=True)
