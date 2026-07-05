"""ocr_results 영속화 (이슈 #19) — job_id 멱등 업서트.

OCR·분류·추출·마스킹의 산출물을 ``ocr_results``(contracts.md §3)에 한 행으로 저장하고
생성된 ``id``(``ReportJob.ocr_result_id``)를 돌려준다. 파이프라인(#15)이 소비→처리
후 이 함수를 호출하고, 반환 id를 ReportJob에 실어 발행한다.

설계 메모:
- **멱등**: at-least-once 재소비 시 같은 ``job_id``는 한 행만 유지한다. ``ON CONFLICT
  (job_id)``로 업서트하고 기존 id를 그대로 돌려준다 — 중복 소비가 새 행/새 리포트를
  만들지 않게 한다(정식 at-least-once의 진입부 멱등 보강).
- **PII 안전(§13)**: ``masked_text``·``masked_lines``의 텍스트는 마스킹본만 담는다.
  좌표·confidence는 PII가 아니라 원형 보존한다(이미지 마스킹 좌표 재사용).
- **jsonb**: dict/list는 ``json.dumps`` 문자열로 넘기고 SQL에서 ``::jsonb`` 캐스트한다
  (asyncpg 기본 코덱은 dict→jsonb 자동 변환을 하지 않는다).
"""

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

import asyncpg

from core.contracts import DocType
from core.logging import get_logger
from ocr_worker.ocr import OcrResult

logger = get_logger(__name__)

# 진입부 멱등 단락(#15)용 조회 — 이미 처리된 job_id면 무거운 OCR을 건너뛰고
# 기존 id·doc_type으로 ReportJob만 재발행한다.
_SELECT_BY_JOB_SQL = "SELECT id, doc_type FROM ocr_results WHERE job_id = $1"

# 여러 문 없이 단일 업서트 — job_id 충돌 시 내용 갱신 + 기존 id 반환(RETURNING이
# INSERT/UPDATE 어느 경로든 행을 돌려주도록 DO UPDATE를 쓴다). created_at·id는 불변.
_UPSERT_SQL = """
INSERT INTO ocr_results (
    job_id, doc_type, doc_type_confidence, ocr_confidence,
    masked_text, masked_lines, entities, masked_image_s3_keys
) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb)
ON CONFLICT (job_id) DO UPDATE SET
    doc_type = EXCLUDED.doc_type,
    doc_type_confidence = EXCLUDED.doc_type_confidence,
    ocr_confidence = EXCLUDED.ocr_confidence,
    masked_text = EXCLUDED.masked_text,
    masked_lines = EXCLUDED.masked_lines,
    entities = EXCLUDED.entities,
    masked_image_s3_keys = EXCLUDED.masked_image_s3_keys
RETURNING id
"""


@dataclass(frozen=True, slots=True)
class OcrResultRecord:
    """``ocr_results`` 한 행에 저장할 값 묶음(순수 데이터).

    Attributes:
        job_id: OCR 작업 식별자(UUID 문자열, 멱등 키).
        doc_type: 분류된 문서 유형.
        doc_type_confidence: 분류 신뢰도 0~1.
        ocr_confidence: OCR 라인 평균 신뢰도 0~1(QA 게이팅).
        masked_text: PII 마스킹된 전체 OCR 텍스트(downstream 입력).
        masked_lines: 라인 단위 ``{masked_text, bbox, polygon, confidence}`` 목록
            (텍스트는 마스킹본, 좌표·신뢰도는 원형 — 이미지 마스킹 좌표 재사용).
        entities: 비-PII 추출 엔티티(jsonb).
        masked_image_s3_keys: 검은블럭 비식별 이미지 사본의 페이지별 S3 키.
    """

    job_id: str
    doc_type: DocType
    doc_type_confidence: float
    ocr_confidence: float
    masked_text: str
    masked_lines: list[dict[str, object]] = field(default_factory=list)
    entities: dict[str, object] = field(default_factory=dict)
    masked_image_s3_keys: list[str] = field(default_factory=list)


def build_masked_lines(result: OcrResult, mask: Callable[[str], str]) -> list[dict[str, object]]:
    """OCR 라인 좌표를 보존하되 텍스트만 마스킹해 ``masked_lines`` 구조를 만든다.

    라인별로 독립 마스킹한다 — 좌표·confidence는 PII가 아니라 원형으로 남기고,
    텍스트는 반드시 마스킹본으로 저장한다(§13). 이미지 마스킹 트랙은 이 좌표를
    재사용해 검은블럭 위치를 잡는다.

    Args:
        result: 문서 전체 OCR 결과(페이지·라인 좌표 보유).
        mask: 텍스트 → 마스킹 텍스트 함수(예: ``Masker.mask``).

    Returns:
        ``[{masked_text, bbox, polygon, confidence}, ...]`` (jsonb 직렬화 가능).
    """
    lines: list[dict[str, object]] = []
    for page in result.pages:
        for line in page.lines:
            lines.append(
                {
                    "masked_text": mask(line.text),
                    "bbox": list(line.bbox),
                    "polygon": [list(point) for point in line.polygon],
                    "confidence": line.confidence,
                }
            )
    return lines


async def find_ocr_result(pool: asyncpg.Pool, job_id: str) -> tuple[str, DocType] | None:
    """``job_id``로 이미 저장된 결과의 ``(id, doc_type)``을 찾는다(없으면 None).

    파이프라인(#15)의 진입부 멱등 단락에 쓴다 — at-least-once 재소비로 같은 작업이
    다시 들어오면, 이미 한 행이 있으므로 OCR·마스킹을 반복하지 않고 이 id·doc_type으로
    ``ReportJob``만 재발행한다(발행 후 커밋 규약상 crash 시 재발행이 안전).

    Args:
        pool: asyncpg 연결 풀(core.db).
        job_id: OCR 작업 식별자(UUID 문자열, 멱등 키).

    Returns:
        ``(ocr_results.id, doc_type)`` 튜플 또는 미존재 시 ``None``.
    """
    row = await pool.fetchrow(_SELECT_BY_JOB_SQL, uuid.UUID(job_id))
    if row is None:
        return None
    return str(row["id"]), DocType(row["doc_type"])


async def save_ocr_result(pool: asyncpg.Pool, record: OcrResultRecord) -> str:
    """``ocr_results``에 업서트하고 생성/기존 ``id``를 반환한다(job_id 멱등).

    Args:
        pool: asyncpg 연결 풀(core.db).
        record: 저장할 행 값.

    Returns:
        ``ocr_results.id``(UUID 문자열) — ``ReportJob.ocr_result_id``로 사용.
    """
    row = await pool.fetchrow(
        _UPSERT_SQL,
        uuid.UUID(record.job_id),  # uuid 컬럼: 명시 변환으로 코덱 모호성 제거
        str(record.doc_type),  # StrEnum → text
        record.doc_type_confidence,
        record.ocr_confidence,
        record.masked_text,
        json.dumps(record.masked_lines, ensure_ascii=False),
        json.dumps(record.entities, ensure_ascii=False),
        json.dumps(record.masked_image_s3_keys, ensure_ascii=False),
    )
    if row is None:  # RETURNING은 항상 한 행 → 방어적 계약 검증
        raise RuntimeError(f"ocr_results 업서트가 id를 반환하지 않았습니다: job_id={record.job_id}")
    result_id = str(row["id"])
    logger.info(
        "ocr_result_saved",
        job_id=record.job_id,
        ocr_result_id=result_id,
        doc_type=str(record.doc_type),
    )
    return result_id
