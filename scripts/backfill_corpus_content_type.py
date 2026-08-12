"""corpus_file_part(uploaded) 재라벨링 백필 CLI — 옛 .pdf 하드코딩 오염 데이터 정리.

로직(계획·적용)은 ``corpus_worker.backfill``에 있다. 이 스크립트는 DB 풀·boto3 client
배선만 담당하는 얇은 진입점이다. 기본은 dry-run(계획만 로그로 출력, 아무것도 바꾸지 않음).
S3·DB를 실제로 바꾸는 배포 대상 작업이니 반드시 --apply 없이 먼저 돌려 계획을 확인하라.

    PYTHONPATH=src python scripts/backfill_corpus_content_type.py           # dry-run
    PYTHONPATH=src python scripts/backfill_corpus_content_type.py --apply   # 실제 적용
"""

from __future__ import annotations

import argparse
import asyncio

from core.config import get_settings
from core.db import db_pool
from core.logging import get_logger
from corpus_worker.backfill import SELECT_UPLOADED_SQL, apply_item, plan_backfill

logger = get_logger(__name__)


async def run(*, apply: bool) -> None:
    settings = get_settings()
    async with db_pool(settings) as pool:
        rows = await pool.fetch(SELECT_UPLOADED_SQL)
        items, skipped = plan_backfill(rows, settings=settings)

        logger.info(
            "corpus_backfill_planned", total=len(rows), to_relabel=len(items), skipped=len(skipped)
        )
        for skip in skipped:
            logger.info("corpus_backfill_skip", part_id=skip.part_id, reason=skip.reason)
        for item in items:
            logger.info(
                "corpus_backfill_plan_item",
                part_id=item.part_id,
                old_key=item.old_key,
                new_key=item.new_key,
                content_type=item.content_type,
            )

        if not apply:
            logger.info("corpus_backfill_dry_run_done", note="--apply 없이 실행 — 실제 변경 없음")
            return

        import boto3  # lazy: 배포(ocr extra)에만 설치
        from botocore.exceptions import BotoCoreError, ClientError  # lazy

        s3_client = boto3.client("s3", region_name=settings.aws_region)
        succeeded = 0
        failed = 0
        for item in items:
            try:
                await apply_item(
                    item,
                    s3_client=s3_client,
                    bucket=settings.s3_bucket,
                    sse=settings.s3_corpus_sse,
                    pool=pool,
                )
                succeeded += 1
            except (BotoCoreError, ClientError) as exc:
                # 배치 중 개별 실패로 전체를 멈추지 않는다 — 나머지는 계속 진행하고 사유를 남긴다.
                failed += 1
                logger.error("corpus_backfill_item_failed", part_id=item.part_id, error=str(exc))
        logger.info("corpus_backfill_apply_done", succeeded=succeeded, failed=failed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="실제로 S3·DB를 변경한다(기본은 dry-run)"
    )
    args = parser.parse_args()
    asyncio.run(run(apply=args.apply))


if __name__ == "__main__":
    main()
