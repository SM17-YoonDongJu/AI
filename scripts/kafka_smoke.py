"""Kafka e2e 스모크 — seed → report-job 발행 → 워커 소비(report_drafts) 확인 → 정리.

전제: report-worker가 별도 프로세스로 기동 중 + redpanda + tempVectorDB + Ollama.
    PYTHONPATH=src python scripts/kafka_smoke.py
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import datetime, timezone

from battery import SCENARIOS, _cleanup, _seed
from core.contracts import REPORT_JOB_TOPIC, DocType, ReportJob
from core.db import close_pool, get_pool, init_pool
from core.kafka import KafkaProducer

sys.stdout.reconfigure(encoding="utf-8")


async def main() -> None:
    await init_pool()
    sc = next(s for s in SCENARIOS if s[0].startswith("G"))  # 장해 명시 시나리오
    rid, oid, cid, uid = await _seed(*sc)
    job = ReportJob(
        report_id=rid,
        ocr_result_id=oid,
        job_id=str(uuid.uuid4()),
        doc_type=DocType.DIAGNOSIS,
        user_ref=str(uid),
        claim_id=cid,
        created_at=datetime.now(timezone.utc),
    )
    async with KafkaProducer() as p:
        await p.publish(REPORT_JOB_TOPIC, job, key=rid)
    print(f"PRODUCED report_id={rid}", flush=True)

    pool = get_pool()
    saved = 0
    for i in range(30):
        await asyncio.sleep(6)
        async with pool.acquire() as c:
            saved = await c.fetchval(
                "SELECT count(*) FROM report_drafts WHERE report_id=$1", uuid.UUID(rid)
            )
        if saved:
            async with pool.acquire() as c:
                rate = await c.fetchval(
                    "SELECT draft->'disability'->>'combined_rate' "
                    "FROM report_drafts WHERE report_id=$1",
                    uuid.UUID(rid),
                )
            print(
                f"CONSUMED_OK draft saved after ~{(i + 1) * 6}s, disability_rate={rate}", flush=True
            )
            break
    else:
        print("TIMEOUT: no draft after 180s", flush=True)

    await _cleanup(rid, oid, cid, uid)
    await close_pool()


asyncio.run(main())
