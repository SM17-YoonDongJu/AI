"""repository.py 업서트·직렬화·삭제 outbox 테스트 (이슈 #19, #18 후속).

실제 PG 없이(외부 의존 격리) 페이크 풀로 SQL 인자 직렬화·멱등 계약·반환 id 추출을
검증한다. 실제 upsert/스키마는 docker-compose 기동 후 통합 테스트(#20)로 다룬다.

원본 삭제 outbox(``original_delete_*``)의 상태 전이는 **SQL 안의 CASE**로 표현된다
(앱-DB 시계 스큐를 피하려고 시각도 SQL ``now()`` 기준). 페이크 풀은 SQL을 실행하지
않으므로 여기서는 "어떤 SQL·인자로 부르는가"를 고정하고, 전이 결과 자체는 실 PG를
쓰는 통합 테스트가 확인한다.

실패 저널(``ai.ocr_job_failures``, 마이그레이션 008)도 같은 분업이다 — 업서트 전이
(attempts 증가·terminal 단방향·failure_class 보존)는 SQL 문자열로 고정한다.
"""

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from core.contracts import DocType, OcrJob
from ocr_worker.masking.spans import PiiLabel, Span
from ocr_worker.ocr import OcrLine, OcrPage, OcrResult
from ocr_worker.repository import (
    ClaimDocument,
    ClaimReadiness,
    OcrResultRecord,
    PendingDeletion,
    build_masked_lines,
    clear_job_failure,
    fetch_claim_documents,
    fetch_claim_readiness,
    fetch_due_deletions,
    mark_claim_blocked,
    mark_claim_published,
    mark_failure_terminal,
    record_delete_failure,
    record_delete_success,
    record_document_terminal,
    record_job_failure,
    save_ocr_result,
)


class FakePool:
    """asyncpg.Pool 흉내 — 넘어온 SQL·인자를 포착하고 고정 행(들)을 돌려준다."""

    def __init__(
        self, row: dict[str, Any] | None, rows: list[dict[str, Any]] | None = None
    ) -> None:
        self._row = row
        self._rows = rows or []
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        self.calls.append((sql, args))
        return self._row

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((sql, args))
        return self._rows

    async def execute(self, sql: str, *args: Any) -> None:
        self.calls.append((sql, args))


def _record(**overrides: Any) -> OcrResultRecord:
    base: dict[str, Any] = {
        "job_id": "11111111-1111-1111-1111-111111111111",
        "doc_type": DocType.POLICY,
        "doc_type_confidence": 0.9,
        "ocr_confidence": 0.98,
        "masked_text": "보험계약자 [이름]",
        "masked_lines": [{"masked_text": "보험계약자 [이름]", "bbox": [1, 2, 3, 4]}],
        "entities": {"insurer": "현대해상", "product": None},
        "masked_image_s3_keys": ["masked/job/page-0.png"],
        "original_s3_key": "uploads/x.pdf",
        "original_delete_eligible": False,
    }
    base.update(overrides)
    return OcrResultRecord(**base)


# 삭제 재시도 간격 — 저장 시 첫 스윕 인수 시점을 미루는 데 쓰인다(설정값 패스스루).
_RETRY_INTERVAL = 900.0


async def _save(pool: FakePool, record: OcrResultRecord) -> str:
    """``save_ocr_result`` 호출 헬퍼(필수 키워드 인자를 한 곳에서 고정)."""
    return await save_ocr_result(
        pool,  # type: ignore[arg-type]
        record,
        delete_retry_interval_seconds=_RETRY_INTERVAL,
    )


async def test_save_returns_generated_id() -> None:
    # Arrange
    generated = uuid.UUID("22222222-2222-2222-2222-222222222222")
    pool = FakePool({"id": generated})

    # Act
    result_id = await _save(pool, _record())

    # Assert
    assert result_id == str(generated)


async def test_save_serializes_jsonb_and_uuid() -> None:
    # Arrange
    pool = FakePool({"id": uuid.uuid4()})
    record = _record()

    # Act
    await _save(pool, record)

    # Assert: 인자 순서·타입 계약
    _, args = pool.calls[0]
    assert args[0] == uuid.UUID(record.job_id)  # uuid 컬럼 → UUID 객체
    assert args[1] == "policy"  # StrEnum → text
    assert args[2] == 0.9
    assert args[3] == 0.98
    assert args[4] == record.masked_text
    # jsonb 필드는 JSON 문자열로 직렬화되어야 한다(dict/list 원형 금지)
    assert json.loads(args[5]) == record.masked_lines
    assert json.loads(args[6]) == {"insurer": "현대해상", "product": None}
    assert json.loads(args[7]) == record.masked_image_s3_keys
    assert args[9] == "uploads/x.pdf"  # 삭제 outbox 대상 키(원본)


async def test_save_binds_claim_columns_after_existing_params() -> None:
    # 청구 컬럼(마이그레이션 009)은 파라미터 목록 **맨 뒤**($13~$17)에 붙는다. 중간에
    # 끼우면 뒤따르는 인자가 한 칸씩 밀리고, 그 밀림은 타입이 우연히 맞는 컬럼끼리
    # (text↔text) 조용히 뒤바뀌는 형태로만 드러난다 — 기존 $1~$12가 그대로인지까지 본다.
    pool = FakePool({"id": uuid.uuid4()})
    report_id = "77777777-7777-7777-7777-777777777777"
    attachment_id = "88888888-8888-8888-8888-888888888888"

    await _save(
        pool,
        _record(
            claim_id="claim-9",
            report_id=report_id,
            attachment_id=attachment_id,
            doc_index=2,
            doc_total=3,
        ),
    )

    sql, args = pool.calls[0]
    assert args[9] == "uploads/x.pdf"  # 기존 $10(원본 키)이 밀리지 않았다
    assert args[10] == "not_eligible"  # 기존 $11(삭제 상태)
    assert args[11] == _RETRY_INTERVAL  # 기존 $12(재시도 간격)
    assert args[12:] == (
        "claim-9",
        uuid.UUID(report_id),  # uuid 컬럼 → UUID 객체
        uuid.UUID(attachment_id),
        2,
        3,
    )
    # 컬럼 목록·VALUES·충돌 갱신 어디에서도 빠지면 안 된다(재소비 시 옛 값에 갇힌다).
    assert "$13, $14, $15, $16, $17" in sql
    assert "claim_id = EXCLUDED.claim_id" in sql.split("DO UPDATE SET")[1]
    assert "doc_total = EXCLUDED.doc_total" in sql.split("DO UPDATE SET")[1]


async def test_save_leaves_claim_columns_null_for_standalone_document() -> None:
    # 청구에 안 묶인 단독 문서(하위 호환) — 전부 NULL로 들어가야 한다.
    pool = FakePool({"id": uuid.uuid4()})

    await _save(pool, _record())

    _, args = pool.calls[0]
    assert args[12:] == (None, None, None, None, None)


async def test_save_tolerates_malformed_claim_uuids() -> None:
    # 부가 컬럼 하나의 형식 오류로 OCR·마스킹까지 끝난 결과를 통째로 못 저장하게 되면,
    # 재전달해도 같은 값이 다시 와서 영원히 저장되지 않는다 → 그 컬럼만 비운다.
    pool = FakePool({"id": uuid.uuid4()})

    await _save(pool, _record(claim_id="claim-9", report_id="not-a-uuid", attachment_id=""))

    _, args = pool.calls[0]
    assert args[12] == "claim-9"  # 나머지 컨텍스트는 남는다
    assert args[13] is None and args[14] is None


async def test_save_jsonb_keeps_korean_readable() -> None:
    # Arrange: 한글이 \uXXXX로 이스케이프되지 않아야 한다(ensure_ascii=False)
    pool = FakePool({"id": uuid.uuid4()})

    # Act
    await _save(pool, _record())

    # Assert
    _, args = pool.calls[0]
    assert "현대해상" in args[6]


async def test_save_raises_when_no_row_returned() -> None:
    # Arrange: RETURNING이 행을 안 주는 계약 위반 상황
    pool = FakePool(None)

    # Act / Assert
    with pytest.raises(RuntimeError, match="id를 반환하지 않"):
        await _save(pool, _record())


# ── 원본 삭제 outbox(original_delete_*) ──────────────────────────
_RESULT_ID = "44444444-4444-4444-4444-444444444444"


async def test_save_marks_delete_pending_when_eligible() -> None:
    # Arrange: 이미지 마스킹 검증을 전 페이지 통과 → 원본을 지워도 되는 작업.
    pool = FakePool({"id": uuid.uuid4()})

    # Act
    await _save(pool, _record(original_delete_eligible=True))

    # Assert: 저장과 **같은 문**에서 'pending'을 남겨야 crash 후에도 스윕이 이어받는다.
    sql, args = pool.calls[0]
    assert args[10] == "pending"
    # 첫 시도 창은 호출자의 즉시 삭제 task가 독점한다 — next_attempt_at을 NULL(즉시
    # due)로 두면 즉시 task가 S3 왕복 중인 사이 스윕이 같은 키를 중복 집행해 시도
    # 예산을 두 배로 태운다. 시각 계산은 여기서도 DB의 now() 기준.
    assert args[11] == _RETRY_INTERVAL
    assert "CASE WHEN $11 = 'pending' THEN now() + make_interval(secs => $12) END" in sql


async def test_save_marks_delete_not_eligible_when_verification_failed() -> None:
    # Arrange: 검증 실패(또는 사본 없음) → 애초에 삭제 대상이 아니다.
    pool = FakePool({"id": uuid.uuid4()})

    # Act
    await _save(pool, _record(original_delete_eligible=False))

    # Assert: 'not_eligible'은 스윕 조회에서 빠진다 — 검증 실패 원본이 나중에 조용히
    # 지워지는 일이 없어야 한다(게이트 우회 방지).
    _, args = pool.calls[0]
    assert args[10] == "not_eligible"


async def test_save_upsert_overwrites_delete_outbox_columns_on_conflict() -> None:
    # Arrange / Act: 재소비(같은 job_id) 시에도 다른 컬럼과 같은 방식으로 덮어써야
    # 한다 — 한 컬럼만 INSERT 전용으로 남으면 재처리 시 outbox가 옛 값에 갇힌다.
    pool = FakePool({"id": uuid.uuid4()})
    await _save(pool, _record())

    # Assert
    sql, _ = pool.calls[0]
    assert "original_s3_key = EXCLUDED.original_s3_key" in sql
    assert "original_delete_status = EXCLUDED.original_delete_status" in sql
    assert "original_delete_next_attempt_at = EXCLUDED.original_delete_next_attempt_at" in sql
    # attempts는 충돌 갱신 대상이 **아니다**(재소비가 시도 횟수를 리셋하면 무한 재시도).
    # 특정 표현(`= EXCLUDED`)만 막으면 리터럴 리셋(`= 0`)이 빠져나가므로, DO UPDATE SET
    # 절 안에 컬럼명이 아예 등장하지 않는지로 본다.
    assert "original_delete_attempts" not in sql.split("DO UPDATE SET")[1]


async def test_record_delete_success_marks_terminal_state() -> None:
    # Arrange
    pool = FakePool(None)

    # Act
    await record_delete_success(pool, _RESULT_ID)  # type: ignore[arg-type]

    # Assert: 종결 상태 + 다음 시도 시각 해제. id는 uuid 컬럼이라 UUID 객체로 넘긴다.
    sql, args = pool.calls[0]
    assert "original_delete_status = 'deleted'" in sql
    assert "original_delete_next_attempt_at = NULL" in sql
    assert args == (uuid.UUID(_RESULT_ID),)


async def test_record_delete_failure_passes_limits_and_uses_db_clock() -> None:
    # Arrange
    pool = FakePool({"original_delete_status": "pending", "original_delete_attempts": 1})

    # Act
    await record_delete_failure(pool, _RESULT_ID, 5, 900.0)  # type: ignore[arg-type]

    # Assert: 단일 UPDATE로 attempts++ + 상태·다음 시각 계산까지 끝낸다(읽기-수정-쓰기 없음).
    sql, args = pool.calls[0]
    assert args == (uuid.UUID(_RESULT_ID), 5, 900.0)
    assert sql.count("UPDATE ocr_results") == 1
    assert "original_delete_attempts = original_delete_attempts + 1" in sql
    # 다음 시도 시각은 **DB의 now() 기준**이어야 한다 — 앱에서 계산해 넘기면 워커·DB
    # 시계가 어긋날 때 스윕이 영영 안 집거나 즉시 재시도하는 편향이 생긴다.
    assert "now() + make_interval(secs => $3)" in sql
    assert not any(isinstance(arg, datetime) for arg in args)


async def test_record_delete_failure_branches_on_max_attempts() -> None:
    # Arrange / Act
    pool = FakePool({"original_delete_status": "exhausted", "original_delete_attempts": 3})
    await record_delete_failure(pool, _RESULT_ID, 3, 60.0)  # type: ignore[arg-type]

    # Assert: 상한 도달이면 exhausted(종결·다음 시각 없음), 미달이면 pending 유지 +
    # 다음 시각 갱신. 판정 기준은 **증가 후** 값($2와 attempts+1 비교)이어야 한다 —
    # 증가 전 값과 비교하면 상한을 한 번 더 넘겨 시도한다.
    sql, _ = pool.calls[0]
    assert "WHEN original_delete_attempts + 1 >= $2 THEN 'exhausted'" in sql
    assert "ELSE 'pending'" in sql
    assert "WHEN original_delete_attempts + 1 >= $2 THEN NULL" in sql


async def test_record_delete_failure_returns_state_after_update() -> None:
    # Arrange: 호출측 로그 레벨(운영 개입 신호)은 **갱신 후 DB 상태**로 정해진다.
    # 앱이 attempts를 미리 세면 (a) 즉시 삭제 경로는 애초에 값을 모르고 (b) 스윕
    # 경로는 조회~UPDATE 사이 갱신을 놓쳐 로그와 DB가 어긋난다.
    pool = FakePool({"original_delete_status": "exhausted", "original_delete_attempts": 5})

    # Act
    state = await record_delete_failure(pool, _RESULT_ID, 5, 900.0)  # type: ignore[arg-type]

    # Assert: UPDATE ... RETURNING이라 값은 **갱신 후** 값이어야 한다.
    assert sql_returns_new_state(pool.calls[0][0])
    assert state is not None
    assert (state.status, state.attempts, state.exhausted) == ("exhausted", 5, True)


async def test_record_delete_failure_returns_none_when_row_missing() -> None:
    # Arrange: 대상 행이 없으면(있을 수 없지만) 상태를 모른다 — 호출측이 안전한
    # 기본값(warning)으로 떨어지도록 None을 준다.
    pool = FakePool(None)

    # Act / Assert
    assert await record_delete_failure(pool, _RESULT_ID, 5, 900.0) is None  # type: ignore[arg-type]


def sql_returns_new_state(sql: str) -> bool:
    """UPDATE가 갱신 후 상태를 돌려주는 형태인지(RETURNING 절 존재)."""
    return "RETURNING original_delete_status, original_delete_attempts" in sql


async def test_fetch_due_deletions_maps_rows() -> None:
    # Arrange
    row_id = uuid.UUID("55555555-5555-5555-5555-555555555555")
    pool = FakePool(
        None,
        rows=[{"id": row_id, "original_s3_key": "uploads/x.pdf", "original_delete_attempts": 2}],
    )

    # Act
    due = await fetch_due_deletions(pool, 50)  # type: ignore[arg-type]

    # Assert: attempts는 **지금까지 실패한 횟수**(이번 시도 미포함) — 호출측이 이번
    # 실패로 상한에 닿는지 판단하는 데 쓴다.
    assert due == [
        PendingDeletion(id=str(row_id), original_s3_key="uploads/x.pdf", original_delete_attempts=2)
    ]
    _, args = pool.calls[0]
    assert args == (50,)


async def test_fetch_due_deletions_filters_pending_and_due_only() -> None:
    # Arrange / Act
    pool = FakePool(None, rows=[])
    assert await fetch_due_deletions(pool, 10) == []  # type: ignore[arg-type]

    # Assert: not_eligible·종결 상태는 애초에 조회되지 않고, 아직 때가 안 된 행도 빠진다.
    sql, _ = pool.calls[0]
    assert "original_delete_status = 'pending'" in sql
    due_filter = (
        "original_delete_next_attempt_at IS NULL OR original_delete_next_attempt_at <= now()"
    )
    assert due_filter in sql
    # 미시도(NULL) 행 우선 + 다중 워커 동시 집행 창을 줄이는 잠금 힌트.
    assert "ORDER BY original_delete_next_attempt_at NULLS FIRST" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql


def _detect_all(text: str) -> list[Span]:
    """전체 텍스트를 PII로 표시하는 페이크 detect(해당 라인에 항상 치환 적용됨)."""
    return [Span(0, len(text), PiiLabel.NAME)] if text else []


def test_build_masked_lines_masks_text_and_keeps_coords() -> None:
    # Arrange: detect가 전체를 PII로 표시 → apply_mask가 라벨 토큰으로 치환.
    line = OcrLine(
        text="hello",
        bbox=(1.0, 2.0, 3.0, 4.0),
        polygon=((1.0, 2.0), (3.0, 2.0), (3.0, 4.0), (1.0, 4.0)),
        confidence=0.95,
    )
    page = OcrPage(index=0, width=100, height=200, lines=(line,))
    result = OcrResult(pages=(page,))

    # Act
    masked_lines = build_masked_lines(result, detect=_detect_all)

    # Assert
    assert masked_lines == [
        {
            "masked_text": "[이름]",
            "bbox": [1.0, 2.0, 3.0, 4.0],
            "polygon": [[1.0, 2.0], [3.0, 2.0], [3.0, 4.0], [1.0, 4.0]],
            "confidence": 0.95,
        }
    ]


def test_build_masked_lines_is_json_serializable() -> None:
    # Arrange
    line = OcrLine(text="x", bbox=(0.0, 0.0, 1.0, 1.0), polygon=((0.0, 0.0),), confidence=0.5)
    result = OcrResult(pages=(OcrPage(index=0, width=1, height=1, lines=(line,)),))

    # Act
    masked_lines = build_masked_lines(result, detect=_detect_all)

    # Assert: jsonb 적재 전 직렬화가 깨지지 않아야 한다
    dumped = json.dumps(masked_lines, ensure_ascii=False)
    assert "[이름]" in dumped


def test_build_masked_lines_catches_span_split_across_lines() -> None:
    # 라벨과 값이 서로 다른 라인에 있을 때, 페이지 조인 텍스트 기준으로만 검출되는
    # 상황(라인별 독립 검출로는 못 잡는 앵커 패턴)을 흉내낸다. fake_detect는 두 라인이
    # 합쳐진 텍스트에서만 스팬을 찾으므로, 만약 구현이 라인을 다시 통째로 재검출한다면
    # (라벨 컨텍스트가 없는) 값 라인만으로는 스팬을 못 찾아 이 테스트가 실패한다 —
    # 페이지에서 검출한 스팬을 재검출 없이 그대로 재사용하는지 검증한다.
    label_line = OcrLine(
        text="환자성명:",
        bbox=(0.0, 0.0, 10.0, 5.0),
        polygon=((0.0, 0.0), (10.0, 0.0), (10.0, 5.0), (0.0, 5.0)),
        confidence=0.9,
    )
    value_line = OcrLine(
        text="홍길동",
        bbox=(0.0, 6.0, 10.0, 10.0),
        polygon=((0.0, 6.0), (10.0, 6.0), (10.0, 10.0), (0.0, 10.0)),
        confidence=0.9,
    )
    page = OcrPage(index=0, width=100, height=50, lines=(label_line, value_line))
    result = OcrResult(pages=(page,))

    def fake_detect(text: str) -> list[Span]:
        if "환자성명:" not in text or "홍길동" not in text:
            return []
        start = text.index("홍길동")
        return [Span(start, start + len("홍길동"), PiiLabel.NAME)]

    masked_lines = build_masked_lines(result, detect=fake_detect)

    assert masked_lines[0]["masked_text"] == "환자성명:"  # 라벨 라인엔 PII 없음 → 원문 유지
    assert masked_lines[1]["masked_text"] == "[이름]"  # 값 라인은 마스킹됨


# ── 실패 저널(ai.ocr_job_failures, 마이그레이션 008) ─────────────
# upsert 전이 자체(attempts 증가·terminal 단방향·failure_class 보존)는 **SQL 안**에서
# 일어나므로 페이크 풀로는 실행할 수 없다. 여기서는 (a) 어떤 SQL·인자로 부르는가와
# (b) 그 SQL이 전이 규칙을 실제로 담고 있는가를 문자열로 고정하고, 전이 결과는 실 PG를
# 쓰는 통합 검증이 확인한다(save_ocr_result의 outbox CASE와 같은 분업).
_FAILURE_JOB_ID = "44444444-4444-4444-4444-444444444444"
_FAILURE_REPORT_ID = "55555555-5555-5555-5555-555555555555"
_FAILURE_ATTACHMENT_ID = "66666666-6666-6666-6666-666666666666"


def _job(**overrides: Any) -> OcrJob:
    base: dict[str, Any] = {
        "job_id": _FAILURE_JOB_ID,
        "s3_key": "uploads/x.pdf",
        "content_type": "application/pdf",
        "user_ref": "user-1",
        "doc_type_hint": "diagnosis",
        "claim_id": "claim-9",
        "report_id": _FAILURE_REPORT_ID,
        "attachment_id": _FAILURE_ATTACHMENT_ID,
        "uploaded_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    base.update(overrides)
    return OcrJob(**base)


async def test_record_job_failure_upserts_with_job_context() -> None:
    # Arrange
    pool = FakePool(None)

    # Act
    await record_job_failure(
        pool,  # type: ignore[arg-type]
        _job(),
        failure_class="masking_residual",
        error_type="MaskingError",
        terminal=True,
    )

    # Assert: 스키마 한정 테이블 + 인자 순서 고정(컬럼 목록과 $n이 어긋나면 조용히 뒤섞인다).
    sql, args = pool.calls[0]
    assert "INSERT INTO ai.ocr_job_failures" in sql
    assert args == (
        uuid.UUID(_FAILURE_JOB_ID),
        "uploads/x.pdf",
        "user-1",
        "application/pdf",
        "diagnosis",
        "claim-9",
        uuid.UUID(_FAILURE_REPORT_ID),
        uuid.UUID(_FAILURE_ATTACHMENT_ID),
        "masking_residual",
        "MaskingError",
        True,
    )


async def test_record_job_failure_sql_accumulates_attempts_and_pins_terminal() -> None:
    # 같은 job의 재실패가 새 행을 만들지 않고 attempts로 쌓여야 하고(멱등), 한 번 확정된
    # terminal이 뒤늦은 재전달로 false로 되돌아가면 안 된다(단방향).
    pool = FakePool(None)

    await record_job_failure(
        pool,  # type: ignore[arg-type]
        _job(),
        failure_class="ocr_error",
        error_type="OcrError",
        terminal=False,
    )

    sql = pool.calls[0][0]
    assert "ON CONFLICT (job_id) DO UPDATE" in sql
    assert "attempts = ocr_job_failures.attempts + 1" in sql
    assert "terminal = EXCLUDED.terminal OR ocr_job_failures.terminal" in sql
    assert "last_failed_at = now()" in sql  # 시각은 앱이 아니라 DB 기준(시계 스큐 회피)
    # 최초 실패 시각은 불변 — SET에 들어가면 체류 시간 산출 근거가 매 실패마다 리셋된다.
    assert "first_failed_at =" not in sql


async def test_record_job_failure_tolerates_malformed_identifiers() -> None:
    # 저널은 **다른 실패를 기록하는 마지막 방어선**이라 식별자 하나 때문에 예외를 던지면
    # 원래 실패까지 통째로 사라진다. 형식이 깨진 UUID는 그 컬럼만 NULL로 비운다.
    pool = FakePool(None)

    await record_job_failure(
        pool,  # type: ignore[arg-type]
        _job(job_id="not-a-uuid", report_id="", attachment_id="also-bad"),
        failure_class="unknown",
        error_type="RuntimeError",
        terminal=False,
    )

    args = pool.calls[0][1]
    assert args[0] is None and args[6] is None and args[7] is None
    assert args[1] == "uploads/x.pdf"  # 나머지 컨텍스트는 그대로 남는다


async def test_clear_job_failure_deletes_by_job_id() -> None:
    pool = FakePool(None)

    await clear_job_failure(pool, _FAILURE_JOB_ID)  # type: ignore[arg-type]

    sql, args = pool.calls[0]
    assert sql.strip().startswith("DELETE FROM ai.ocr_job_failures")
    assert args == (uuid.UUID(_FAILURE_JOB_ID),)


async def test_clear_job_failure_skips_db_for_malformed_job_id() -> None:
    # 기록될 수 없었던 키라 지울 행도 없다 — 굳이 왕복하지 않는다(또 예외를 만들지도 않는다).
    pool = FakePool(None)

    await clear_job_failure(pool, "not-a-uuid")  # type: ignore[arg-type]

    assert pool.calls == []


async def test_mark_failure_terminal_preserves_existing_failure_class() -> None:
    # poison 훅은 "확정" 도장만 찍는다. 파이프라인이 남긴 구체적 분류(ocr_error 등)를
    # unknown으로 덮으면 왜 실패했는지 알 수 없게 된다 → SET 절에 failure_class가 없어야 한다.
    pool = FakePool(None)

    await mark_failure_terminal(
        pool,  # type: ignore[arg-type]
        job=_job(),
        message_id="m-1",
        receive_count=6,
    )

    sql, args = pool.calls[0]
    update_clause = sql.split("DO UPDATE SET", 1)[1]
    assert "failure_class" not in update_clause  # 기존 분류 보존
    assert "terminal = true" in update_clause
    assert "'unknown'" in sql.split("ON CONFLICT", 1)[0]  # 신규 행일 때만 unknown
    # attempts는 뒤로 가지 않는다 — receive_count와 저널 attempts는 세는 대상이 다르다.
    assert "attempts = GREATEST(ocr_job_failures.attempts, EXCLUDED.attempts)" in update_clause
    assert args == (
        uuid.UUID(_FAILURE_JOB_ID),
        "m-1",
        "uploads/x.pdf",
        "user-1",
        "application/pdf",
        "diagnosis",
        "claim-9",
        uuid.UUID(_FAILURE_REPORT_ID),
        uuid.UUID(_FAILURE_ATTACHMENT_ID),
        6,
    )


async def test_mark_failure_terminal_without_job_records_schema_invalid() -> None:
    # 역직렬화조차 실패한 poison — job_id를 모르니 message_id만으로 추적한다.
    pool = FakePool(None)

    await mark_failure_terminal(
        pool,  # type: ignore[arg-type]
        job=None,
        message_id="m-2",
        receive_count=7,
    )

    sql, args = pool.calls[0]
    assert "'schema_invalid'" in sql
    assert "ON CONFLICT" not in sql  # job_id가 NULL이면 UNIQUE가 안 걸려 업서트 불가
    assert args == ("m-2", 7)


# ── 청구 fan-in(ai.claim_readiness, 마이그레이션 010) ────────────
# 카운터 증가 자체는 SQL 안(ON CONFLICT DO UPDATE)에서 원자적으로 일어나므로 페이크
# 풀로는 실행할 수 없다. 여기서는 (a) 어떤 SQL·인자로 부르는가와 (b) 그 SQL이 원자적
# 증가 형태인가를 문자열로 고정하고, 실제 전이는 실 PG 검증이 확인한다.
_CLAIM_ID = "claim-9"
_CLAIM_REPORT_ID = "99999999-9999-9999-9999-999999999999"
_CLAIM_JOB_ID = "aaaaaaaa-1111-2222-3333-444444444444"


async def test_record_document_terminal_adds_job_id_to_terminal_set() -> None:
    # Arrange: 갱신 후 종결 문서 수를 DB가 돌려준다(앱이 세지 않는다).
    pool = FakePool({"docs_terminal": 2})

    # Act
    count = await record_document_terminal(
        pool,  # type: ignore[arg-type]
        claim_id=_CLAIM_ID,
        job_id=_CLAIM_JOB_ID,
        report_id=uuid.UUID(_CLAIM_REPORT_ID),
        doc_total=3,
    )

    # Assert
    assert count == 2
    sql, args = pool.calls[0]
    assert "INSERT INTO ai.claim_readiness" in sql
    assert args == (_CLAIM_ID, uuid.UUID(_CLAIM_REPORT_ID), 3, _CLAIM_JOB_ID)
    # 여전히 **단일 업서트**여야 한다 — 서로 다른 문서를 동시에 끝낸 두 워커의 증가분이
    # 유실되지 않고, doc_total에 닿는 호출자가 정확히 하나다.
    assert "ON CONFLICT (claim_id) DO UPDATE" in sql
    assert "RETURNING docs_terminal" in sql
    # 개수 증가(+1)가 아니라 **집합 추가**여야 과다 카운트를 구조적으로 막는다:
    # 같은 문서가 두 번 종결로 보고돼도(동시 중복 전달·ack 실패 재전달·poison 중복)
    # containment 검사에 걸려 수가 늘지 않는다.
    update_clause = sql.split("DO UPDATE SET")[1]
    assert "terminal_job_ids" in update_clause
    assert "@> ARRAY[$4]::text[]" in update_clause
    assert "+ 1" not in update_clause
    # 진행 중에 기준(doc_total)·발행 키(report_id)가 흔들리면 안 된다 → 충돌 갱신 제외.
    assert "doc_total" not in update_clause
    assert "report_id" not in update_clause


async def test_record_document_terminal_takes_report_id_as_uuid_object() -> None:
    # 문자열을 여기서 변환하면 형식이 어긋난 값이 **매 재시도마다 같은 지점에서**
    # ValueError를 내 그 청구의 fan-in이 결정적으로 영구 정지한다. 유효성 판정은
    # 호출측(pipeline._claim_context)이 진입 시점에 끝내고, 여기는 타입으로 보장받는다.
    pool = FakePool({"docs_terminal": 1})

    await record_document_terminal(
        pool,  # type: ignore[arg-type]
        claim_id=_CLAIM_ID,
        job_id=_CLAIM_JOB_ID,
        report_id=uuid.UUID(_CLAIM_REPORT_ID),
        doc_total=3,
    )

    assert isinstance(pool.calls[0][1][1], uuid.UUID)


async def test_record_document_terminal_raises_when_no_row_returned() -> None:
    # RETURNING은 항상 한 행 → 계약 위반이면 조용히 0을 반환하지 말고 터뜨린다
    # (0을 돌려주면 "아직 문서가 남았다"로 오인해 발행이 영영 안 일어난다).
    pool = FakePool(None)

    with pytest.raises(RuntimeError, match="docs_terminal"):
        await record_document_terminal(
            pool,  # type: ignore[arg-type]
            claim_id=_CLAIM_ID,
            job_id=_CLAIM_JOB_ID,
            report_id=uuid.UUID(_CLAIM_REPORT_ID),
            doc_total=3,
        )


async def test_fetch_claim_documents_maps_rows_in_doc_index_order() -> None:
    # Arrange
    policy_id = uuid.UUID("aaaaaaaa-0000-0000-0000-00000000000a")
    diagnosis_id = uuid.UUID("bbbbbbbb-0000-0000-0000-00000000000b")
    pool = FakePool(
        None,
        rows=[
            {"id": policy_id, "doc_type": "policy", "ocr_quality": "ok"},
            {"id": diagnosis_id, "doc_type": "diagnosis", "ocr_quality": "needs_reupload"},
        ],
    )

    # Act
    docs = await fetch_claim_documents(pool, _CLAIM_ID)  # type: ignore[arg-type]

    # Assert
    assert docs == [
        ClaimDocument(ocr_result_id=str(policy_id), doc_type=DocType.POLICY, ocr_quality="ok"),
        ClaimDocument(
            ocr_result_id=str(diagnosis_id),
            doc_type=DocType.DIAGNOSIS,
            ocr_quality="needs_reupload",
        ),
    ]
    sql, args = pool.calls[0]
    assert args == (_CLAIM_ID,)
    assert "FROM ai.ocr_results" in sql
    # doc_index는 nullable(단독 문서 호환)이라 NULL이 앞으로 튀어나오면 대표 문서 선택이
    # 뒤집힌다 — 뒤로 밀고 그 안에서는 저장 순서로 안정 정렬한다.
    assert "ORDER BY doc_index NULLS LAST, created_at" in sql


async def test_fetch_claim_documents_returns_empty_when_all_documents_failed() -> None:
    # 실패한 문서는 ocr_results에 행이 없다 — 필수 유형 판정이 "무엇이 실제로 인식됐나"만
    # 보게 하는 것이 이 조회의 목적이다.
    pool = FakePool(None, rows=[])

    assert await fetch_claim_documents(pool, _CLAIM_ID) == []  # type: ignore[arg-type]


async def test_mark_claim_blocked_records_missing_types_and_judged_at() -> None:
    # Arrange
    pool = FakePool(None)

    # Act
    await mark_claim_blocked(
        pool,  # type: ignore[arg-type]
        claim_id=_CLAIM_ID,
        missing_doc_types=["diagnosis", "policy"],
    )

    # Assert: 빠진 유형은 사용자 안내("그 문서를 다시 촬영") 근거라 반드시 남아야 한다.
    sql, args = pool.calls[0]
    assert "UPDATE ai.claim_readiness" in sql
    assert "status = 'blocked'" in sql
    assert "judged_at = now()" in sql  # 시각은 DB 기준(앱-DB 시계 스큐 회피)
    assert args == (_CLAIM_ID, ["diagnosis", "policy"])


async def test_mark_claim_published_stamps_status() -> None:
    # Arrange
    pool = FakePool(None)

    # Act
    await mark_claim_published(pool, _CLAIM_ID)  # type: ignore[arg-type]

    # Assert
    sql, args = pool.calls[0]
    assert "UPDATE ai.claim_readiness" in sql
    assert "status = 'published'" in sql
    assert args == (_CLAIM_ID,)


async def test_fetch_claim_readiness_returns_status_and_progress() -> None:
    # Arrange / Act: 상태만으로는 "아직 문서가 남은 pending"과 "다 모였는데 판정 도중
    # 죽은 pending"을 구분할 수 없다 — 진행도를 함께 읽어야 후자를 재개할 수 있다.
    pool = FakePool({"status": "pending", "docs_terminal": 3, "doc_total": 3})

    readiness = await fetch_claim_readiness(pool, _CLAIM_ID)  # type: ignore[arg-type]

    # Assert
    assert readiness == ClaimReadiness(status="pending", docs_terminal=3, doc_total=3)
    assert readiness is not None and readiness.all_documents_terminal is True
    sql, args = pool.calls[0]
    assert "FROM ai.claim_readiness" in sql
    assert "status, docs_terminal, doc_total" in sql
    assert args == (_CLAIM_ID,)


async def test_fetch_claim_readiness_reports_documents_still_pending() -> None:
    pool = FakePool({"status": "pending", "docs_terminal": 1, "doc_total": 3})

    readiness = await fetch_claim_readiness(pool, _CLAIM_ID)  # type: ignore[arg-type]

    assert readiness is not None and readiness.all_documents_terminal is False


async def test_fetch_claim_readiness_returns_none_when_no_document_terminal_yet() -> None:
    # 아직 이 청구의 문서가 하나도 종결되지 않았으면 행 자체가 없다.
    assert await fetch_claim_readiness(FakePool(None), _CLAIM_ID) is None  # type: ignore[arg-type]
