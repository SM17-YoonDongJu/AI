"""비-PDF 코퍼스 데이터 정리 CLI — PDF 전용 정책 전환 전 업로드된 객체 삭제.

로직(계획·적용)은 ``corpus_worker.purge``에 있다. 이 스크립트는 DB 풀·boto3 client
배선만 담당하는 얇은 진입점이다. 기본은 dry-run(삭제 대상만 로그로 출력, 아무것도
지우지 않음). **영구 삭제**(버킷 버저닝 없으면 복구 불가)이니 반드시 --apply 없이
먼저 돌려 대상 목록을 확인하라.

    PYTHONPATH=src python scripts/purge_non_pdf_corpus.py           # dry-run
    PYTHONPATH=src python scripts/purge_non_pdf_corpus.py --apply   # 실제 삭제
"""

from __future__ import annotations

import argparse
import asyncio

import asyncpg

from core.config import get_settings
from core.db import db_pool
from core.logging import get_logger
from corpus_worker.purge import SELECT_UPLOADED_SQL, apply_purge_item, plan_purge

logger = get_logger(__name__)


async def run(*, apply: bool) -> None:
    settings = get_settings()
    async with db_pool(settings) as pool:
        rows = await pool.fetch(SELECT_UPLOADED_SQL)
        items = plan_purge(rows)

        logger.info("corpus_purge_planned", total_uploaded=len(rows), to_delete=len(items))
        for item in items:
            logger.info(
                "corpus_purge_plan_item",
                part_id=item.part_id,
                s3_key=item.s3_key,
                notion_file_name=item.notion_file_name,
            )

        if not apply:
            logger.info("corpus_purge_dry_run_done", note="--apply 없이 실행 — 실제 변경 없음")
            return

        import boto3  # lazy: 배포(ocr extra)에만 설치
        from botocore.exceptions import BotoCoreError, ClientError  # lazy

        s3_client = boto3.client("s3", region_name=settings.aws_region)
        succeeded = 0
        failed = 0
        for item in items:
            try:
                await apply_purge_item(
                    item, s3_client=s3_client, bucket=settings.s3_bucket, pool=pool
                )
                succeeded += 1
            except (BotoCoreError, ClientError, asyncpg.PostgresError) as exc:
                # 배치 중 개별 실패로 전체를 멈추지 않는다 — 나머지는 계속 진행하고 사유를 남긴다.
                failed += 1
                logger.error("corpus_purge_item_failed", part_id=item.part_id, error=str(exc))
        logger.info("corpus_purge_apply_done", succeeded=succeeded, failed=failed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="실제로 S3·DB를 변경한다(기본은 dry-run)"
    )
    args = parser.parse_args()
    asyncio.run(run(apply=args.apply))


if __name__ == "__main__":
    main()
