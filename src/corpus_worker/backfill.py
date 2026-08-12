"""옛 ``.pdf`` 하드코딩 시절 오염 데이터 백필 (이슈 #35 P2 사후 조치).

``corpus_key()``·``put_file()``이 한때 전부 ``.pdf``/``application/pdf``로 고정돼 있었다 —
이미 ``status='uploaded'``로 커밋된 파트는 파이프라인 재처리 대상에서 제외되므로(멱등
재개), 코드를 고쳐도 기존 S3 객체는 자동으로 재라벨링되지 않는다. 이 모듈은 그 재라벨링
"계획"(순수 함수, I/O 없음)과 "적용"(CopyObject+DB 갱신+구 키 삭제)을 분리해 제공한다.
CLI 진입점은 ``scripts/backfill_corpus_content_type.py``.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any

import asyncpg

from core.config import Settings
from core.exceptions import CorpusSyncError
from core.logging import get_logger
from corpus_worker.filetype import detect
from corpus_worker.s3 import corpus_key

logger = get_logger(__name__)

SELECT_UPLOADED_SQL = """
SELECT p.id, p.s3_key, p.notion_file_name, p.sha256, f.category
FROM corpus_file_part p
JOIN corpus_file f ON f.notion_page_id = p.file_page_id
WHERE p.status = 'uploaded' AND p.sha256 IS NOT NULL AND p.s3_key IS NOT NULL
"""

_UPDATE_S3_KEY_SQL = "UPDATE corpus_file_part SET s3_key = $2 WHERE id = $1"


@dataclass(slots=True, frozen=True)
class BackfillItem:
    """재라벨링 대상 1건 — 옛(그른) 키에서 파일명 기준으로 판정한 올바른 키로 이동한다."""

    part_id: str
    old_key: str
    new_key: str
    content_type: str


@dataclass(slots=True, frozen=True)
class SkippedItem:
    """건너뛴 대상 — 이미 올바르거나(no-op), 타입 판정 불가(수동 검토 필요)."""

    part_id: str
    reason: str


def plan_backfill(
    rows: list[Any], *, settings: Settings
) -> tuple[list[BackfillItem], list[SkippedItem]]:
    """행 목록에서 재라벨링 대상을 계획한다(순수 함수 — S3·DB 호출 없음).

    Args:
        rows: ``SELECT_UPLOADED_SQL`` 결과 행(딕셔너리류 접근 가능 — asyncpg.Record 포함).
        settings: S3 키 접두사 등 설정(``corpus_key`` 조립에 사용).

    Returns:
        ``(재라벨링 대상, 건너뛴 대상)``. 이미 올바른 키(진짜 PDF였던 경우)이거나
        ``notion_file_name``에서 타입을 판정할 수 없으면 건너뜀 목록에 사유와 함께 담는다.
    """
    items: list[BackfillItem] = []
    skipped: list[SkippedItem] = []
    for row in rows:
        part_id = str(row["id"])
        try:
            file_type = detect(row["notion_file_name"])
        except CorpusSyncError as exc:
            skipped.append(SkippedItem(part_id=part_id, reason=str(exc)))
            continue
        old_key = row["s3_key"]
        new_key = corpus_key(row["category"], row["sha256"], file_type.ext, settings=settings)
        if new_key == old_key:
            skipped.append(SkippedItem(part_id=part_id, reason="이미 올바른 키"))
            continue
        items.append(
            BackfillItem(
                part_id=part_id,
                old_key=old_key,
                new_key=new_key,
                content_type=file_type.content_type,
            )
        )
    return items, skipped


async def apply_item(
    item: BackfillItem, *, s3_client: Any, bucket: str, sse: str, pool: asyncpg.Pool
) -> None:
    """S3 CopyObject(ContentType 교정) → DB s3_key 갱신 → 구 키 삭제.

    이 순서를 지켜야 한다 — 복사·DB 갱신이 끝나기 전엔 구 키를 지우지 않아 중간 실패 시에도
    데이터가 유실되지 않는다(최악의 경우 구·신 키가 잠시 중복 존재할 뿐).

    Args:
        item: ``plan_backfill``이 계획한 재라벨링 대상.
        s3_client: boto3 S3 client(호출자가 lazy 생성해 주입).
        bucket: 대상 S3 버킷.
        sse: 서버측 암호화(예: ``AES256``).
        pool: asyncpg 연결 풀.

    Raises:
        botocore.exceptions.BotoCoreError | ClientError: S3 복사·삭제 실패.
        asyncpg.PostgresError: DB 갱신 실패.
    """
    await asyncio.to_thread(
        s3_client.copy_object,
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": item.old_key},
        Key=item.new_key,
        ContentType=item.content_type,
        MetadataDirective="REPLACE",
        ServerSideEncryption=sse,
    )
    await pool.execute(_UPDATE_S3_KEY_SQL, uuid.UUID(item.part_id), item.new_key)
    await asyncio.to_thread(s3_client.delete_object, Bucket=bucket, Key=item.old_key)
    logger.info(
        "corpus_backfill_relabeled",
        part_id=item.part_id,
        old_key=item.old_key,
        new_key=item.new_key,
    )
