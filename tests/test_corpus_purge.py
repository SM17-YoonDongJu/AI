"""corpus_worker.purge 테스트 — PDF 전용 정책 전환 전 업로드된 비-PDF 데이터 정리.

plan_purge(순수 함수)가 S3 키가 .pdf로 끝나지 않는 파트만 골라내는지 검증한다.
apply_purge_item은 페이크 DB pool·S3 client로 DB 정리→S3 삭제 순서를 검증한다
(중간 실패 시 댕글링 참조가 남지 않는 순서).
"""

import uuid
from typing import Any

from corpus_worker.purge import PurgeItem, apply_purge_item, plan_purge

PART0 = "aaaaaaaa-0000-0000-0000-000000000000"
PART1 = "bbbbbbbb-0000-0000-0000-000000000000"
PART2 = "cccccccc-0000-0000-0000-000000000000"
SHA0 = "0" * 64
SHA1 = "1" * 64
SHA2 = "2" * 64


def test_plan_purge_keeps_pdf() -> None:
    # Arrange: 진짜 PDF는 삭제 대상이 아니다
    rows = [
        {
            "id": uuid.UUID(PART0),
            "s3_key": f"corpus/terms/{SHA0}.pdf",
            "notion_file_name": "표준약관.pdf",
        }
    ]

    # Act
    items = plan_purge(rows)

    # Assert
    assert items == []


def test_plan_purge_selects_non_pdf() -> None:
    # Arrange: HWP·zip·md 등 비-PDF는 전부 삭제 대상
    rows = [
        {
            "id": uuid.UUID(PART0),
            "s3_key": f"corpus/terms/{SHA0}.pdf",
            "notion_file_name": "약관.pdf",
        },
        {
            "id": uuid.UUID(PART1),
            "s3_key": f"corpus/terms/{SHA1}.hwp.zip",
            "notion_file_name": "특별약관.hwp.zip",
        },
        {
            "id": uuid.UUID(PART2),
            "s3_key": f"corpus/terms/{SHA2}.md",
            "notion_file_name": "01_insurance-business-act.md",
        },
    ]

    # Act
    items = plan_purge(rows)

    # Assert
    assert items == [
        PurgeItem(
            part_id=PART1,
            s3_key=f"corpus/terms/{SHA1}.hwp.zip",
            notion_file_name="특별약관.hwp.zip",
        ),
        PurgeItem(
            part_id=PART2,
            s3_key=f"corpus/terms/{SHA2}.md",
            notion_file_name="01_insurance-business-act.md",
        ),
    ]


class FakeS3Client:
    """boto3 S3 client 흉내 — delete_object 호출을 공유 이벤트 로그에 기록한다."""

    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.delete_kwargs: dict[str, Any] | None = None

    def delete_object(self, **kwargs: Any) -> None:
        self.delete_kwargs = kwargs
        self._events.append("delete_object")


class FakePool:
    """asyncpg.Pool.execute 흉내 — DB 정리 호출을 공유 이벤트 로그에 기록한다."""

    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, sql: str, *args: Any) -> str:
        self.executed.append((sql, args))
        self._events.append("db_update")
        return "UPDATE 1"


async def test_apply_purge_item_updates_db_before_deleting_s3() -> None:
    # Arrange: 순서가 중요하다 — S3를 먼저 지우면 DB 갱신 실패 시 댕글링 참조가 남는다
    item = PurgeItem(
        part_id=PART1, s3_key=f"corpus/terms/{SHA1}.hwp.zip", notion_file_name="특별약관.hwp.zip"
    )
    events: list[str] = []
    s3_client = FakeS3Client(events)
    pool = FakePool(events)

    # Act
    await apply_purge_item(item, s3_client=s3_client, bucket="test-bucket", pool=pool)  # type: ignore[arg-type]

    # Assert: DB 정리 → S3 삭제 순서
    assert events == ["db_update", "delete_object"]
    assert pool.executed[0][0].count("status = 'failed'") == 1
    assert "s3_key = NULL" in pool.executed[0][0]
    assert pool.executed[0][1] == (uuid.UUID(PART1),)
    assert s3_client.delete_kwargs == {
        "Bucket": "test-bucket",
        "Key": f"corpus/terms/{SHA1}.hwp.zip",
    }
