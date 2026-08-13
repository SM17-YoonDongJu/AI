"""PDF 전용 정책 전환 후 기존 비-PDF 코퍼스 데이터 정리.

corpus_worker가 PDF만 받도록 바뀌면서(``filetype.detect``), 그 전에 이미
``uploaded``로 커밋된 비-PDF 객체(HWP·zip·txt·법령 md·판례 json 등)는 자동으로
없어지지 않는다 — 새 정책은 신규 업로드만 막을 뿐 과거 데이터는 그대로 남는다.
이 모듈은 그 정리(S3 삭제 + DB 정리)의 "계획"과 "적용"을 분리해 제공한다.
CLI 진입점은 ``scripts/purge_non_pdf_corpus.py``.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any

import asyncpg

from core.logging import get_logger

logger = get_logger(__name__)

SELECT_UPLOADED_SQL = """
SELECT id, s3_key, notion_file_name
FROM corpus_file_part
WHERE status = 'uploaded' AND s3_key IS NOT NULL
"""

# 삭제 순서(DB 먼저 → S3 나중)와 반대로 하지 않는다 — S3를 먼저 지우고 DB 갱신이
# 실패하면 status='uploaded'인데 가리키는 객체가 없는 댕글링 참조가 남는다. DB를
# 먼저 정리해두면 S3 삭제가 중간에 실패해도 최악의 경우 고아 객체(무해)만 남는다.
_MARK_PURGED_SQL = "UPDATE corpus_file_part SET status = 'failed', s3_key = NULL WHERE id = $1"


@dataclass(slots=True, frozen=True)
class PurgeItem:
    """삭제 대상 1건 — PDF 전용 정책 전에 올라간 비-PDF S3 객체."""

    part_id: str
    s3_key: str
    notion_file_name: str | None


def plan_purge(rows: list[Any]) -> list[PurgeItem]:
    """행 목록에서 비-PDF(``.pdf``로 끝나지 않는 키) 삭제 대상을 계획한다(순수 함수).

    Args:
        rows: ``SELECT_UPLOADED_SQL`` 결과 행(딕셔너리류 접근 가능 — asyncpg.Record 포함).

    Returns:
        S3 키가 ``.pdf``로 끝나지 않는 파트 목록.
    """
    return [
        PurgeItem(
            part_id=str(row["id"]),
            s3_key=row["s3_key"],
            notion_file_name=row["notion_file_name"],
        )
        for row in rows
        if not row["s3_key"].lower().endswith(".pdf")
    ]


async def apply_purge_item(
    item: PurgeItem, *, s3_client: Any, bucket: str, pool: asyncpg.Pool
) -> None:
    """DB 정리(status='failed', s3_key=NULL) → S3 DeleteObject 순서로 삭제한다.

    Args:
        item: ``plan_purge``가 계획한 삭제 대상.
        s3_client: boto3 S3 client(호출자가 lazy 생성해 주입).
        bucket: 대상 S3 버킷.
        pool: asyncpg 연결 풀.

    Raises:
        asyncpg.PostgresError: DB 갱신 실패.
        botocore.exceptions.BotoCoreError | ClientError: S3 삭제 실패.
    """
    await pool.execute(_MARK_PURGED_SQL, uuid.UUID(item.part_id))
    await asyncio.to_thread(s3_client.delete_object, Bucket=bucket, Key=item.s3_key)
    logger.info("corpus_purge_deleted", part_id=item.part_id, s3_key=item.s3_key)
