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
- **원본 삭제 outbox**: 마스킹 검증을 통과해 지워야 할 원본 S3 키와 그 시도 상태를
  같은 행(``original_delete_*``)에 담는다. 저장과 같은 트랜잭션이라 삭제 전에 crash가
  나도 ``pending`` 행이 남아 워커 스윕(``pipeline.sweep_pending_deletes``)이 이어받는다.
- **실패 저널**(``ai.ocr_job_failures``, 마이그레이션 008): 처리에 실패한 작업을 한 행으로
  남긴다. ``job_id`` 업서트라 반복 실패가 ``attempts``로 쌓이고, 회복하면 파이프라인이
  행을 지운다. ``terminal``은 단방향(false→true) — 확정 실패 판정이 재전달 한 번으로
  되돌아가지 않게 한다. 예외 **메시지는 저장하지 않는다**(클래스명만, §9).
"""

import json
import uuid
from dataclasses import dataclass, field

import asyncpg

from core.contracts import DocType, OcrJob
from core.logging import get_logger
from ocr_worker.masking.image_masker import (
    DetectFn,
    line_local_spans,
    reading_order,
    reading_order_text,
)
from ocr_worker.masking.spans import apply_mask
from ocr_worker.ocr import OcrResult

logger = get_logger(__name__)

# 진입부 멱등 단락(#15)용 조회 — 이미 처리된 job_id면 무거운 OCR을 건너뛰고
# 기존 id·doc_type·ocr_quality로 ReportJob만 재발행한다.
_SELECT_BY_JOB_SQL = "SELECT id, doc_type, ocr_quality FROM ocr_results WHERE job_id = $1"

# 여러 문 없이 단일 업서트 — job_id 충돌 시 내용 갱신 + 기존 id 반환(RETURNING이
# INSERT/UPDATE 어느 경로든 행을 돌려주도록 DO UPDATE를 쓴다). created_at·id는 불변.
# original_s3_key·original_delete_status는 삭제 outbox의 시작점이다 — 저장과 같은
# 문(=같은 트랜잭션)에서 'pending'을 남겨야 삭제 전에 crash가 나도 스윕이 이어받는다.
#
# original_delete_next_attempt_at은 **NULL(즉시 due)이 아니라 한 주기 뒤**로 채운다:
# 저장 직후 호출자가 띄우는 즉시 삭제 task가 아직 S3 왕복 중인데 그 사이 스윕 사이클이
# 돌면 같은 in-flight 키를 중복 집행해 attempts 예산을 두 배로 태운다(실측). 첫 시도
# 창은 즉시 task가 독점하고, 그게 실패하거나 crash로 사라지면 한 주기 뒤 스윕이
# 인수한다 — crash 복구 근거(행이 pending으로 남음)는 그대로다.
# attempts는 INSERT 목록에도 충돌 갱신에도 없다(기본값 0 · 재소비가 리셋하지 않음).
_UPSERT_SQL = """
INSERT INTO ocr_results (
    job_id, doc_type, doc_type_confidence, ocr_confidence,
    masked_text, masked_lines, entities, masked_image_s3_keys, ocr_quality,
    original_s3_key, original_delete_status, original_delete_next_attempt_at
) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9, $10, $11,
    CASE WHEN $11 = 'pending' THEN now() + make_interval(secs => $12) END)
ON CONFLICT (job_id) DO UPDATE SET
    doc_type = EXCLUDED.doc_type,
    doc_type_confidence = EXCLUDED.doc_type_confidence,
    ocr_confidence = EXCLUDED.ocr_confidence,
    masked_text = EXCLUDED.masked_text,
    masked_lines = EXCLUDED.masked_lines,
    entities = EXCLUDED.entities,
    masked_image_s3_keys = EXCLUDED.masked_image_s3_keys,
    ocr_quality = EXCLUDED.ocr_quality,
    original_s3_key = EXCLUDED.original_s3_key,
    original_delete_status = EXCLUDED.original_delete_status,
    original_delete_next_attempt_at = EXCLUDED.original_delete_next_attempt_at
RETURNING id
"""

# 삭제 성공(종결). 종결 상태에는 다음 시도 시각이 의미 없으므로 함께 비운다.
_MARK_DELETE_SUCCESS_SQL = """
UPDATE ocr_results
SET original_delete_status = 'deleted',
    original_delete_next_attempt_at = NULL
WHERE id = $1
"""

# 삭제 실패: attempts++ 후 상한 도달이면 'exhausted'(종결), 아니면 'pending'을 유지하고
# 다음 시도 시각을 뒤로 민다. 시각은 **SQL의 now() 기준**으로 계산한다 — 앱 서버와 DB의
# 시계가 어긋나면 스윕이 영영 안 집거나 즉시 재시도하는 편향이 생긴다.
# 단일 UPDATE라 읽기-수정-쓰기 경합(동시 실패 두 건이 같은 attempts를 읽는 것)이 없다.
# RETURNING으로 **갱신 후 실제 상태**를 돌려준다 — 호출측이 attempts를 미리 계산해
# 'exhausted' 여부를 추정하면 조회~UPDATE 사이의 갱신을 놓쳐 로그와 DB가 어긋난다.
_MARK_DELETE_FAILURE_SQL = """
UPDATE ocr_results
SET original_delete_attempts = original_delete_attempts + 1,
    original_delete_status = CASE
        WHEN original_delete_attempts + 1 >= $2 THEN 'exhausted'
        ELSE 'pending'
    END,
    original_delete_next_attempt_at = CASE
        WHEN original_delete_attempts + 1 >= $2 THEN NULL
        ELSE now() + make_interval(secs => $3)
    END
WHERE id = $1
RETURNING original_delete_status, original_delete_attempts
"""

# 스윕 대상 조회 — 부분 인덱스(ocr_results_pending_delete_idx)를 그대로 탄다.
# NULLS FIRST: 최초 pending(아직 한 번도 실패하지 않은 행)을 가장 먼저 집는다.
# FOR UPDATE SKIP LOCKED는 여러 워커가 같은 행을 동시에 집는 창을 줄이기 위한 것이고,
# 완전한 배타 보장은 아니다(트랜잭션이 SELECT 직후 끝나 잠금이 풀린다). 중복 집행이
# 나도 S3 DeleteObject가 멱등이라 무해하다 — 긴 트랜잭션으로 S3 왕복을 감싸는 대가가
# 더 크다고 보고 짧은 조회를 택했다.
_FETCH_DUE_DELETIONS_SQL = """
SELECT id, original_s3_key, original_delete_attempts
FROM ocr_results
WHERE original_delete_status = 'pending'
  AND (original_delete_next_attempt_at IS NULL OR original_delete_next_attempt_at <= now())
ORDER BY original_delete_next_attempt_at NULLS FIRST
LIMIT $1
FOR UPDATE SKIP LOCKED
"""

# ── 실패 저널(ai.ocr_job_failures, 마이그레이션 008) ─────────────
# 스키마를 명시(ai.)한다 — search_path에 기대지 않는다(#48~#52 소유권 사건 이후 관례).
#
# job_id 업서트라 같은 작업의 반복 실패가 새 행을 만들지 않고 attempts로 쌓인다.
# ON CONFLICT DO UPDATE의 SET 절에서 **기존 행은 스키마 없는 테이블명**으로 참조한다
# (`ocr_job_failures.attempts`) — range table 엔트리 이름이 `ocr_job_failures`라
# `ai.ocr_job_failures.attempts`로 쓰면 "missing FROM-clause entry"가 난다.
#
# terminal은 `EXCLUDED.terminal OR ocr_job_failures.terminal`로 **단방향**(false→true)이다.
# 확정 실패로 판정된 작업이 뒤늦은 재전달 한 번으로 "아직 진행 중"으로 되돌아가면,
# 사용자에게 이미 노출한 실패가 근거 없이 사라진다.
# first_failed_at은 갱신하지 않는다(최초 실패 시각은 불변 — 체류 시간 산출 근거).
# 시각은 전부 SQL의 now() 기준(앱-DB 시계 스큐 회피 — outbox 관례와 동일).
_UPSERT_JOB_FAILURE_SQL = """
INSERT INTO ai.ocr_job_failures (
    job_id, s3_key, user_ref, content_type, doc_type_hint, claim_id,
    report_id, attachment_id, failure_class, error_type, attempts, terminal
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, $11)
ON CONFLICT (job_id) DO UPDATE SET
    attempts = ocr_job_failures.attempts + 1,
    last_failed_at = now(),
    terminal = EXCLUDED.terminal OR ocr_job_failures.terminal,
    failure_class = EXCLUDED.failure_class,
    error_type = EXCLUDED.error_type,
    s3_key = EXCLUDED.s3_key,
    user_ref = EXCLUDED.user_ref,
    content_type = EXCLUDED.content_type,
    doc_type_hint = EXCLUDED.doc_type_hint,
    claim_id = EXCLUDED.claim_id,
    report_id = EXCLUDED.report_id,
    attachment_id = EXCLUDED.attachment_id
"""

# 회복(재전달 후 성공)한 작업의 저널 정리. 남겨두면 이미 처리된 작업이 실패로 조회된다.
_CLEAR_JOB_FAILURE_SQL = "DELETE FROM ai.ocr_job_failures WHERE job_id = $1"

# poison 훅 전용 — 확정 실패 도장. failure_class를 SET 절에 **넣지 않아** 기존 행의
# 분류(예: masking_residual)를 보존한다. 행이 없을 때만 'unknown'으로 삽입된다.
# attempts는 GREATEST로 뒤로 가지 않게 한다 — receive_count(SQS 전달 횟수)와 저널의
# attempts(핸들러 실패 횟수)는 세는 대상이 달라 어느 쪽이 클지 정해져 있지 않다.
_MARK_JOB_FAILURE_TERMINAL_SQL = """
INSERT INTO ai.ocr_job_failures (
    job_id, message_id, s3_key, user_ref, content_type, doc_type_hint, claim_id,
    report_id, attachment_id, failure_class, attempts, terminal
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'unknown', $10, true)
ON CONFLICT (job_id) DO UPDATE SET
    terminal = true,
    message_id = EXCLUDED.message_id,
    attempts = GREATEST(ocr_job_failures.attempts, EXCLUDED.attempts),
    last_failed_at = now()
"""

# 역직렬화조차 안 되는 poison — job_id를 모르니 message_id로만 추적한다.
# job_id가 NULL이면 UNIQUE 제약이 걸리지 않아(NULL은 서로 충돌하지 않음) 업서트가
# 불가능하다 → 순수 INSERT다. 훅 성공 후에만 메시지가 삭제되므로 보통 1건이지만,
# 삭제 자체가 실패해 재전달되면 같은 message_id로 행이 하나 더 생길 수 있다.
# 추적 목적상 중복은 무해하고(같은 message_id로 묶어 보면 된다), 이를 막자고
# 유니크 인덱스를 더하면 "기록조차 못 남기는" 실패 모드가 새로 생긴다.
_INSERT_UNPARSED_FAILURE_SQL = """
INSERT INTO ai.ocr_job_failures (message_id, failure_class, attempts, terminal)
VALUES ($1, 'schema_invalid', $2, true)
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
        ocr_quality: 자동 품질 판정("ok" | "needs_reupload"). ``ReportJob.ocr_quality``로
            패스스루된다.
        original_s3_key: 원본 파일의 S3 키(``OcrJob.s3_key``). 삭제 outbox의 대상 —
            스윕이 재시도할 때 이 값으로 ``DeleteObject``를 부른다. PII가 아닌 내부
            식별자(파일명은 UUID로 치환됨).
        original_delete_eligible: 이미지 마스킹 검증(5.1+5.2)을 전 페이지가 통과해
            원본을 지워도 되는가. ``True``면 ``original_delete_status='pending'``으로,
            ``False``면 ``'not_eligible'``(스윕 제외)로 저장된다.
    """

    job_id: str
    doc_type: DocType
    doc_type_confidence: float
    ocr_confidence: float
    masked_text: str
    masked_lines: list[dict[str, object]] = field(default_factory=list)
    entities: dict[str, object] = field(default_factory=dict)
    masked_image_s3_keys: list[str] = field(default_factory=list)
    ocr_quality: str = "ok"
    original_s3_key: str = ""
    original_delete_eligible: bool = False


@dataclass(frozen=True, slots=True)
class DeleteRetryState:
    """실패 기록 **후** outbox 행의 실제 상태(``record_delete_failure`` 반환).

    로그 레벨(운영 개입 신호 여부)을 이 값으로 정한다 — 앱에서 attempts를 미리 세지
    않는다. 즉시 삭제 경로는 애초에 attempts를 모르고, 스윕 경로는 조회 시점 값이
    stale일 수 있어(그 사이 다른 주체가 증가) 둘 다 DB 판정을 그대로 따르는 게 맞다.

    Attributes:
        status: 갱신 후 ``original_delete_status``(``pending`` | ``exhausted``).
        attempts: 갱신 후 누적 시도 횟수.
    """

    status: str
    attempts: int

    @property
    def exhausted(self) -> bool:
        """시도 상한을 소진해 종결됐는가(= 자동 재시도가 더는 없다)."""
        return self.status == "exhausted"


@dataclass(frozen=True, slots=True)
class PendingDeletion:
    """스윕이 재시도할 원본 삭제 1건(``ocr_results``의 outbox 행).

    Attributes:
        id: ``ocr_results.id``(성공/실패 기록 시 다시 쓰는 키).
        original_s3_key: 지워야 할 원본 S3 키.
        original_delete_attempts: 조회 시점까지 실패한 횟수(관측용 스냅샷).
            **``exhausted`` 판정에 쓰지 말 것** — 조회~UPDATE 사이에 즉시 삭제 task나
            다른 워커가 증가시켰을 수 있어 stale이다. 상한 도달 여부는
            ``record_delete_failure``가 돌려주는 ``DeleteRetryState``를 쓴다.
    """

    id: str
    original_s3_key: str
    original_delete_attempts: int


def build_masked_lines(result: OcrResult, detect: DetectFn) -> list[dict[str, object]]:
    """OCR 라인 좌표를 보존하되 텍스트만 마스킹해 ``masked_lines`` 구조를 만든다.

    라인을 완전히 독립적으로 재검출하면 라벨과 값이 다른 라인으로 쪼개진 PII(예:
    "환자의 성명" 라인과 실제 이름 라인이 분리)를 놓친다 — 값만 있는 라인은 라벨
    컨텍스트가 없어 앵커 정규식이 매칭되지 않을 수 있다(실측 확인). 그래서 페이지
    단위(리딩오더 정렬)로 한 번만 탐지하고, 그 스팬을 라인별 로컬 오프셋으로 잘라
    (``line_local_spans``) **재검출 없이 그대로** 치환(``apply_mask``)한다 — "어느
    라인이 PII인가" 판정과 "그 라인의 어느 구간을 지울까"가 같은 스팬에서 나오므로
    이미지 마스킹(``ImageMasker.redact_pages``)과도 항상 같은 기준을 공유한다.

    Args:
        result: 문서 전체 OCR 결과(페이지·라인 좌표 보유).
        detect: 텍스트 → PII 스팬 함수(예: ``Masker.detect``). 페이지 단위 탐지에 쓴다.

    Returns:
        ``[{masked_text, bbox, polygon, confidence}, ...]`` (jsonb 직렬화 가능).
    """
    lines: list[dict[str, object]] = []
    for page in result.pages:
        order = reading_order(page)
        spans = detect(reading_order_text(page, order))
        local_spans = line_local_spans(page, spans, order)
        for index, line in enumerate(page.lines):
            text = apply_mask(line.text, local_spans[index]) if index in local_spans else line.text
            lines.append(
                {
                    "masked_text": text,
                    "bbox": list(line.bbox),
                    "polygon": [list(point) for point in line.polygon],
                    "confidence": line.confidence,
                }
            )
    return lines


async def find_ocr_result(pool: asyncpg.Pool, job_id: str) -> tuple[str, DocType, str] | None:
    """``job_id``로 이미 저장된 결과의 ``(id, doc_type, ocr_quality)``를 찾는다(없으면 None).

    파이프라인(#15)의 진입부 멱등 단락에 쓴다 — at-least-once 재소비로 같은 작업이
    다시 들어오면, 이미 한 행이 있으므로 OCR·마스킹을 반복하지 않고 이 값들로
    ``ReportJob``만 재발행한다(발행 후 커밋 규약상 crash 시 재발행이 안전).

    Args:
        pool: asyncpg 연결 풀(core.db).
        job_id: OCR 작업 식별자(UUID 문자열, 멱등 키).

    Returns:
        ``(ocr_results.id, doc_type, ocr_quality)`` 튜플 또는 미존재 시 ``None``.
    """
    row = await pool.fetchrow(_SELECT_BY_JOB_SQL, uuid.UUID(job_id))
    if row is None:
        return None
    return str(row["id"]), DocType(row["doc_type"]), row["ocr_quality"]


async def save_ocr_result(
    pool: asyncpg.Pool, record: OcrResultRecord, *, delete_retry_interval_seconds: float
) -> str:
    """``ocr_results``에 업서트하고 생성/기존 ``id``를 반환한다(job_id 멱등).

    Args:
        pool: asyncpg 연결 풀(core.db).
        record: 저장할 행 값.
        delete_retry_interval_seconds: 삭제 outbox의 첫 스윕 인수 시점을 미루는 간격(초).
            호출자가 저장 직후 띄우는 즉시 삭제 task와 스윕이 같은 키를 중복 집행하지
            않도록 ``original_delete_next_attempt_at``을 이만큼 뒤로 잡는다(설정값
            ``ocr_delete_retry_interval_seconds``를 그대로 넘긴다 — 이 값은 행마다
            달라지는 데이터가 아니라 정책이라 ``OcrResultRecord``에 담지 않는다).

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
        record.ocr_quality,
        record.original_s3_key,
        # 검증 통과분만 스윕 대상('pending')으로 남긴다 — 'not_eligible'은 스윕이 절대
        # 집지 않으므로, 검증 실패 원본이 나중에 조용히 지워지는 일이 없다.
        "pending" if record.original_delete_eligible else "not_eligible",
        delete_retry_interval_seconds,
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


async def record_delete_success(pool: asyncpg.Pool, ocr_result_id: str) -> None:
    """원본 삭제 성공을 outbox에 기록한다(``original_delete_status='deleted'``, 종결).

    S3 ``DeleteObject``는 멱등이라, 즉시 삭제와 스윕이 같은 키를 겹쳐 지워도 둘 다
    성공으로 기록될 수 있다 — 같은 종결 상태를 두 번 쓰는 것이라 무해하다.

    Args:
        pool: asyncpg 연결 풀(core.db).
        ocr_result_id: 대상 ``ocr_results.id``(UUID 문자열).
    """
    await pool.execute(_MARK_DELETE_SUCCESS_SQL, uuid.UUID(ocr_result_id))


async def record_delete_failure(
    pool: asyncpg.Pool, ocr_result_id: str, max_attempts: int, retry_interval_seconds: float
) -> DeleteRetryState | None:
    """원본 삭제 실패를 outbox에 기록하고 **갱신 후 상태**를 돌려준다(attempts++).

    상한 미도달이면 ``pending``을 유지하고 다음 시도 시각을 ``now() +
    retry_interval_seconds``로 민다(고정 백오프 — 스윕 주기와 같은 값). 상한에
    도달하면 ``exhausted``로 종결하고 스윕 대상에서 빠진다 — 이후 남은 원본은 S3
    라이프사이클 정책이 정리한다(운영 알림 대상).

    Args:
        pool: asyncpg 연결 풀(core.db).
        ocr_result_id: 대상 ``ocr_results.id``(UUID 문자열).
        max_attempts: 총 시도 상한(증가 후 값이 이 값 이상이면 ``exhausted``).
        retry_interval_seconds: 다음 시도까지의 간격(초).

    Returns:
        갱신 후 상태. 대상 행이 없으면(있을 수 없지만 방어적으로) ``None``.
    """
    row = await pool.fetchrow(
        _MARK_DELETE_FAILURE_SQL,
        uuid.UUID(ocr_result_id),
        max_attempts,
        retry_interval_seconds,
    )
    if row is None:
        return None
    return DeleteRetryState(
        status=row["original_delete_status"], attempts=row["original_delete_attempts"]
    )


async def fetch_due_deletions(pool: asyncpg.Pool, limit: int) -> list[PendingDeletion]:
    """재시도할 때가 된 원본 삭제 건을 최대 ``limit``개 가져온다.

    ``not_eligible``(검증 실패로 애초에 삭제 대상이 아닌 행)과 종결 상태
    (``deleted``·``exhausted``)는 조회되지 않는다. ``pending`` 행은 항상
    ``original_s3_key``가 채워져 있다(``save_ocr_result``가 둘을 한 문에서 쓴다).

    Args:
        pool: asyncpg 연결 풀(core.db).
        limit: 한 배치 상한.

    Returns:
        시도 대상 목록(가장 오래 기다린 순 — 미시도 행 우선).
    """
    rows = await pool.fetch(_FETCH_DUE_DELETIONS_SQL, limit)
    return [
        PendingDeletion(
            id=str(row["id"]),
            original_s3_key=row["original_s3_key"],
            original_delete_attempts=row["original_delete_attempts"],
        )
        for row in rows
    ]


# ── 실패 저널 ────────────────────────────────────────────────────
def _as_uuid(value: str | None, *, field_name: str) -> uuid.UUID | None:
    """UUID 문자열을 관대하게 변환한다(빈 값·형식 오류면 ``None``).

    실패 저널은 **다른 실패를 기록하는 마지막 방어선**이라, 여기서 예외를 던지면
    원래 실패까지 통째로 삼켜 무음 실패가 된다. 식별자 하나가 형식에 안 맞으면
    그 컬럼만 비우고 나머지는 남긴다 — "일부만 아는 기록"이 "기록 없음"보다 낫다.
    값 자체는 로그에 남기지 않고 어떤 필드였는지만 남긴다.

    ``job_id``가 UUID가 아니면(``OcrJob.job_id``는 형식 검증 없는 ``str``이라 통과할 수
    있다) 멱등 키가 NULL이 되고, NULL은 UNIQUE 충돌을 일으키지 않아 재실패마다 행이
    하나씩 늘어난다(실측). 수신 횟수 상한이 상한선이라 메시지당 몇 행 수준이고, 이를
    막자고 예외를 던지면 위의 "기록 없음"으로 되돌아간다 — 중복을 감수한다.
    """
    if not value:
        return None
    try:
        return uuid.UUID(value)
    except ValueError:
        logger.warning("ocr_job_failure_invalid_uuid", field=field_name)
        return None


async def record_job_failure(
    pool: asyncpg.Pool, job: OcrJob, *, failure_class: str, error_type: str, terminal: bool
) -> None:
    """OCR 작업 실패를 ``ai.ocr_job_failures``에 업서트한다(``job_id`` 멱등).

    같은 작업이 재전달돼 또 실패하면 새 행이 아니라 ``attempts``가 늘고
    ``last_failed_at``이 갱신된다. ``terminal``은 단방향이라, 이미 확정 실패로
    기록된 작업은 이후 호출이 ``terminal=False``여도 확정 상태를 유지한다.

    Args:
        pool: asyncpg 연결 풀(core.db).
        job: 실패한 작업(식별자·S3 키 등 비-PII 컨텍스트를 함께 남긴다).
        failure_class: 실패 분류(마이그레이션 008의 CHECK 목록 중 하나).
        error_type: 예외 **클래스명만**. 메시지 문자열은 넘기지 않는다(§9) —
            정형화되지 않은 예외 문자열에 OCR 원문 조각이 섞일 수 있다.
        terminal: 확정 실패(사용자 노출 대상)인가.
    """
    await pool.execute(
        _UPSERT_JOB_FAILURE_SQL,
        _as_uuid(job.job_id, field_name="job_id"),
        job.s3_key,
        job.user_ref,
        job.content_type,
        job.doc_type_hint,
        job.claim_id,
        _as_uuid(job.report_id, field_name="report_id"),
        _as_uuid(job.attachment_id, field_name="attachment_id"),
        failure_class,
        error_type,
        terminal,
    )
    logger.info(
        "ocr_job_failure_recorded",
        job_id=job.job_id,
        failure_class=failure_class,
        error_type=error_type,
        terminal=terminal,
    )


async def clear_job_failure(pool: asyncpg.Pool, job_id: str) -> None:
    """회복한 작업의 실패 저널 행을 지운다(없으면 아무 일도 하지 않는다).

    일시 실패(``terminal=false``)로 기록됐다가 재전달에서 성공한 작업이 계속
    "실패"로 조회되지 않게 한다. 확정 실패(``terminal=true``) 행도 함께 지워지지만,
    그 경로는 애초에 성공으로 끝나지 않으므로 여기에 도달하지 않는다.

    Args:
        pool: asyncpg 연결 풀(core.db).
        job_id: OCR 작업 식별자(UUID 문자열).
    """
    job_uuid = _as_uuid(job_id, field_name="job_id")
    if job_uuid is None:
        return  # 애초에 기록될 수 없는 키 — 지울 행도 없다
    await pool.execute(_CLEAR_JOB_FAILURE_SQL, job_uuid)


async def mark_failure_terminal(
    pool: asyncpg.Pool, *, job: OcrJob | None, message_id: str, receive_count: int
) -> None:
    """poison 메시지를 확정 실패로 도장 찍는다(``SqsConsumer.on_poison`` 훅용).

    컨슈머가 수신 횟수 상한을 넘긴 메시지를 큐에서 걷어내기 **직전**에 부른다 —
    이게 마지막 기록 기회다. 여기서 실패하면 컨슈머가 삭제를 보류하므로, 이 함수가
    성공했다는 것은 곧 "추적 근거를 남겼다"는 뜻이 된다.

    두 갈래를 다룬다:
    - ``job``이 있으면(본문은 유효했으나 처리가 계속 실패) ``job_id`` 업서트로
      ``terminal=true``를 확정한다. 기존 행이 있으면 그 ``failure_class``를
      **보존**한다 — 파이프라인이 남긴 구체적 분류(예: ``ocr_error``)가
      ``unknown``으로 덮이면 왜 실패했는지 알 수 없게 된다.
    - ``job``이 없으면(역직렬화조차 실패) ``job_id``를 모르므로 ``message_id``로만
      ``schema_invalid`` 행을 남긴다.

    Args:
        pool: asyncpg 연결 풀(core.db).
        job: 파싱된 작업. 스키마 위반이면 ``None``.
        message_id: SQS MessageId(``job``이 없을 때의 유일한 추적 키).
        receive_count: ``ApproximateReceiveCount``(재전달 횟수).
    """
    if job is None:
        await pool.execute(_INSERT_UNPARSED_FAILURE_SQL, message_id, receive_count)
        logger.error(
            "ocr_job_failure_terminal", message_id=message_id, failure_class="schema_invalid"
        )
        return
    await pool.execute(
        _MARK_JOB_FAILURE_TERMINAL_SQL,
        _as_uuid(job.job_id, field_name="job_id"),
        message_id,
        job.s3_key,
        job.user_ref,
        job.content_type,
        job.doc_type_hint,
        job.claim_id,
        _as_uuid(job.report_id, field_name="report_id"),
        _as_uuid(job.attachment_id, field_name="attachment_id"),
        receive_count,
    )
    logger.error(
        "ocr_job_failure_terminal",
        job_id=job.job_id,
        message_id=message_id,
        receive_count=receive_count,
    )
