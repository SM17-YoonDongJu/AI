"""repository.py 업서트·직렬화 테스트 (이슈 #19).

실제 PG 없이(외부 의존 격리) 페이크 풀로 SQL 인자 직렬화·멱등 계약·반환 id 추출을
검증한다. 실제 upsert/스키마는 docker-compose 기동 후 통합 테스트(#20)로 다룬다.
"""

import json
import uuid
from typing import Any

import pytest

from core.contracts import DocType
from ocr_worker.ocr import OcrLine, OcrPage, OcrResult
from ocr_worker.repository import (
    OcrResultRecord,
    build_masked_lines,
    save_ocr_result,
)


class FakePool:
    """asyncpg.Pool.fetchrow 흉내 — 넘어온 SQL·인자를 포착하고 고정 행을 돌려준다."""

    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        self.calls.append((sql, args))
        return self._row


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
    }
    base.update(overrides)
    return OcrResultRecord(**base)


async def test_save_returns_generated_id() -> None:
    # Arrange
    generated = uuid.UUID("22222222-2222-2222-2222-222222222222")
    pool = FakePool({"id": generated})

    # Act
    result_id = await save_ocr_result(pool, _record())  # type: ignore[arg-type]

    # Assert
    assert result_id == str(generated)


async def test_save_serializes_jsonb_and_uuid() -> None:
    # Arrange
    pool = FakePool({"id": uuid.uuid4()})
    record = _record()

    # Act
    await save_ocr_result(pool, record)  # type: ignore[arg-type]

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


async def test_save_jsonb_keeps_korean_readable() -> None:
    # Arrange: 한글이 \uXXXX로 이스케이프되지 않아야 한다(ensure_ascii=False)
    pool = FakePool({"id": uuid.uuid4()})

    # Act
    await save_ocr_result(pool, _record())  # type: ignore[arg-type]

    # Assert
    _, args = pool.calls[0]
    assert "현대해상" in args[6]


async def test_save_raises_when_no_row_returned() -> None:
    # Arrange: RETURNING이 행을 안 주는 계약 위반 상황
    pool = FakePool(None)

    # Act / Assert
    with pytest.raises(RuntimeError, match="id를 반환하지 않"):
        await save_ocr_result(pool, _record())  # type: ignore[arg-type]


def test_build_masked_lines_masks_text_and_keeps_coords() -> None:
    # Arrange: 라인 텍스트를 대문자로 바꾸는 페이크 마스킹(좌표 보존만 확인)
    line = OcrLine(
        text="hello",
        bbox=(1.0, 2.0, 3.0, 4.0),
        polygon=((1.0, 2.0), (3.0, 2.0), (3.0, 4.0), (1.0, 4.0)),
        confidence=0.95,
    )
    page = OcrPage(index=0, width=100, height=200, lines=(line,))
    result = OcrResult(pages=(page,))

    # Act
    masked_lines = build_masked_lines(result, mask=str.upper)

    # Assert
    assert masked_lines == [
        {
            "masked_text": "HELLO",
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
    masked_lines = build_masked_lines(result, mask=lambda _text: "[이름]")

    # Assert: jsonb 적재 전 직렬화가 깨지지 않아야 한다
    dumped = json.dumps(masked_lines, ensure_ascii=False)
    assert "[이름]" in dumped
