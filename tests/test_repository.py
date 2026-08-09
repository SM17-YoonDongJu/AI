"""repository.py 업서트·직렬화·삭제 outbox 테스트 (이슈 #19, #18 후속).

실제 PG 없이(외부 의존 격리) 페이크 풀로 SQL 인자 직렬화·멱등 계약·반환 id 추출을
검증한다. 실제 upsert/스키마는 docker-compose 기동 후 통합 테스트(#20)로 다룬다.

원본 삭제 outbox(``original_delete_*``)의 상태 전이는 **SQL 안의 CASE**로 표현된다
(앱-DB 시계 스큐를 피하려고 시각도 SQL ``now()`` 기준). 페이크 풀은 SQL을 실행하지
않으므로 여기서는 "어떤 SQL·인자로 부르는가"를 고정하고, 전이 결과 자체는 실 PG를
쓰는 통합 테스트가 확인한다.
"""

import json
import uuid
from datetime import datetime
from typing import Any

import pytest

from core.contracts import DocType
from ocr_worker.masking.spans import PiiLabel, Span
from ocr_worker.ocr import OcrLine, OcrPage, OcrResult
from ocr_worker.repository import (
    OcrResultRecord,
    PendingDeletion,
    build_masked_lines,
    fetch_due_deletions,
    record_delete_failure,
    record_delete_success,
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
