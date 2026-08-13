"""corpus_worker.backfill 테스트 — 옛 .pdf 하드코딩 오염 데이터 재라벨링.

plan_backfill(순수 함수)이 이미 올바른 키/판정 불가 파일명을 건너뛰고, 잘못 라벨링된
파트만 올바른 키·ContentType으로 계획하는지 검증한다. 타입 판정은 filetype.detect()에
위임하므로(PDF 전용 정책), PDF가 아닌 첨부는 재라벨링 대상이 아니라 스킵된다.
apply_item은 페이크 S3 client·DB pool로 CopyObject→DB 갱신→구 키 삭제 순서를
검증한다(중간 실패 시 데이터 유실 없는 순서).
"""

import uuid
from typing import Any

from core.config import Settings
from corpus_worker.backfill import BackfillItem, apply_item, plan_backfill

PART0 = "aaaaaaaa-0000-0000-0000-000000000000"
PART1 = "bbbbbbbb-0000-0000-0000-000000000000"
PART3 = "dddddddd-0000-0000-0000-000000000000"
SHA0 = "0" * 64
SHA1 = "1" * 64
SHA3 = "3" * 64


def _settings() -> Settings:
    return Settings(s3_corpus_prefix="corpus/", s3_corpus_sse="AES256")


def test_plan_backfill_skips_already_correct_pdf() -> None:
    # Arrange: 진짜 PDF는 옛 로직으로도 키가 이미 올바르다
    rows = [
        {
            "id": uuid.UUID(PART0),
            "s3_key": f"corpus/terms/{SHA0}.pdf",
            "notion_file_name": "표준약관.pdf",
            "sha256": SHA0,
            "category": "terms",
        }
    ]

    # Act
    items, skipped = plan_backfill(rows, settings=_settings())

    # Assert
    assert items == []
    assert skipped[0].part_id == PART0
    assert skipped[0].reason == "이미 올바른 키"


def test_plan_backfill_skips_non_pdf_attachment() -> None:
    # Arrange: 코퍼스는 PDF만 허용 — HWP 첨부는 재라벨링 대상이 아니라 수동 검토로 넘어간다
    rows = [
        {
            "id": uuid.UUID(PART1),
            "s3_key": f"corpus/terms/{SHA1}.pdf",
            "notion_file_name": "특별약관.hwp",
            "sha256": SHA1,
            "category": "terms",
        }
    ]

    # Act
    items, skipped = plan_backfill(rows, settings=_settings())

    # Assert
    assert items == []
    assert skipped[0].part_id == PART1


def test_plan_backfill_skips_unknown_file_name_for_manual_review() -> None:
    # Arrange: 파일명이 비었으면 타입을 추측하지 않고 수동 검토로 넘긴다
    rows = [
        {
            "id": uuid.UUID(PART3),
            "s3_key": f"corpus/terms/{SHA3}.pdf",
            "notion_file_name": None,
            "sha256": SHA3,
            "category": "terms",
        }
    ]

    # Act
    items, skipped = plan_backfill(rows, settings=_settings())

    # Assert
    assert items == []
    assert skipped[0].part_id == PART3


class FakeS3Client:
    """boto3 S3 client 흉내 — copy_object/delete_object 호출을 공유 이벤트 로그에 기록한다."""

    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.copy_kwargs: dict[str, Any] | None = None
        self.delete_kwargs: dict[str, Any] | None = None

    def copy_object(self, **kwargs: Any) -> None:
        self.copy_kwargs = kwargs
        self._events.append("copy_object")

    def delete_object(self, **kwargs: Any) -> None:
        self.delete_kwargs = kwargs
        self._events.append("delete_object")


class FakePool:
    """asyncpg.Pool.execute 흉내 — s3_key 갱신 호출을 공유 이벤트 로그에 기록한다."""

    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, sql: str, *args: Any) -> str:
        self.executed.append((sql, args))
        self._events.append("db_update")
        return "UPDATE 1"


async def test_apply_item_copies_then_updates_db_then_deletes_old_key() -> None:
    # Arrange: 순서가 중요하다 — 복사·DB갱신이 끝나기 전엔 구 키를 지우면 안 된다
    # (중간에 실패해도 데이터 유실 없이 구·신 키가 잠시 중복 존재하는 선에서 끝나야 함)
    item = BackfillItem(
        part_id=PART1,
        old_key=f"corpus/terms/{SHA1}.pdf",
        new_key=f"corpus/terms/{SHA1}.hwp",
        content_type="application/x-hwp",
    )
    events: list[str] = []
    s3_client = FakeS3Client(events)
    pool = FakePool(events)

    # Act
    await apply_item(item, s3_client=s3_client, bucket="test-bucket", sse="AES256", pool=pool)  # type: ignore[arg-type]

    # Assert: 복사 → DB 갱신 → 구 키 삭제 순서
    assert events == ["copy_object", "db_update", "delete_object"]
    assert s3_client.copy_kwargs == {
        "Bucket": "test-bucket",
        "CopySource": {"Bucket": "test-bucket", "Key": f"corpus/terms/{SHA1}.pdf"},
        "Key": f"corpus/terms/{SHA1}.hwp",
        "ContentType": "application/x-hwp",
        "MetadataDirective": "REPLACE",
        "ServerSideEncryption": "AES256",
    }
    assert s3_client.delete_kwargs == {"Bucket": "test-bucket", "Key": f"corpus/terms/{SHA1}.pdf"}
    assert pool.executed[0][1] == (uuid.UUID(PART1), f"corpus/terms/{SHA1}.hwp")
