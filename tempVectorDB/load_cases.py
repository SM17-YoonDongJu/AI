"""판례·분쟁조정 청크 → case_chunks 적재 (임베딩 포함).

대상:
  - prec_chunks.jsonl 중 '보험 직접관련' 버킷만 (보험금청구/구상금/자동차손배/채무부존재)
  - fss_chunks.jsonl 전체 (금감원 분쟁조정, 전부 보험)
임베딩: qwen3-embedding:0.6b (약관 policy_chunks와 동일 1024d).

    python tempVectorDB/load_cases.py
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import re

import asyncpg
import httpx
from kiwipiepy import Kiwi

CORPUS = r"C:\Users\wkdrn\test\_workspace\raw_dataset\rag_corpus"
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/aiengine")
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "qwen3-embedding:0.6b")
BATCH = 32

_kiwi = Kiwi()

# 한글 section → chunk_type enum
_SEC = {
    "판시사항": "holding", "판결요지": "summary", "주문": "order",
    "이유": "reasoning", "청구취지": "fact", "참조조문": "general", "본문": "decision",
}


def _bucket(nm: str, jong: str) -> str:
    n = (nm or "").replace(" ", "")
    if "보험금" in n:
        return "INS"
    if "채무부존재" in n:
        return "INS"
    if "보험" in n and "구상" in n:
        return "INS"
    if re.search(r"손해배상\(자", n) or "자동차손해" in n:
        return "INS"
    return "SKIP"


def _date(s: str):
    s = (s or "").strip()
    m = re.match(r"(\d{4})[.\-]?(\d{2})[.\-]?(\d{2})", s)
    if not m:
        return None
    try:
        return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _load_prec_ids() -> set[str]:
    """보험 관련 판례 doc_id 집합 (1차 스캔)."""
    keep, seen = set(), {}
    for line in open(os.path.join(CORPUS, "prec_chunks.jsonl"), encoding="utf-8"):
        try:
            r = json.loads(line)
        except Exception:
            continue
        d = r.get("doc_id")
        if d not in seen:
            seen[d] = _bucket(r.get("사건명", ""), r.get("사건종류명", ""))
        if seen[d] == "INS":
            keep.add(d)
    return keep


def _iter_records():
    """(case_chunks 컬럼 dict) 제너레이터."""
    prec_ids = _load_prec_ids()
    # 판례
    for line in open(os.path.join(CORPUS, "prec_chunks.jsonl"), encoding="utf-8"):
        try:
            r = json.loads(line)
        except Exception:
            continue
        if r.get("doc_id") not in prec_ids:
            continue
        sec = r.get("section", "")
        yield {
            "chunk_id": r["id"], "content": r["text"], "chunk_type": _SEC.get(sec, "general"),
            "doc_hash": str(r.get("doc_id")), "source_type": "court_precedent",
            "institution": r.get("법원명"), "case_no": r.get("사건번호"),
            "case_title": r.get("사건명"), "decision_date": _date(r.get("선고일자")),
            "source_url": "https://www.law.go.kr", "accident_type": None,
            "tags": [t for t in [r.get("outcome"), r.get("사건종류명")] if t],
            "section": sec, "chunk_index": r.get("chunk_idx"),
        }
    # 분쟁조정
    for line in open(os.path.join(CORPUS, "fss_chunks.jsonl"), encoding="utf-8"):
        try:
            r = json.loads(line)
        except Exception:
            continue
        yield {
            "chunk_id": r["id"], "content": r["text"], "chunk_type": "decision",
            "doc_hash": str(r.get("doc_id")), "source_type": "fss_mediation",
            "institution": "금융감독원", "case_no": None,
            "case_title": r.get("제목"), "decision_date": _date(r.get("등록일")),
            "source_url": "https://www.fss.or.kr", "accident_type": None,
            "tags": [t for t in [r.get("outcome"), r.get("유형")] if t],
            "section": r.get("section"), "chunk_index": r.get("chunk_idx"),
        }


async def _embed_batch(client: httpx.AsyncClient, texts: list[str]) -> list[str]:
    resp = await client.post(f"{OLLAMA_URL}/api/embed", json={"model": EMBED_MODEL, "input": texts})
    resp.raise_for_status()
    embs = resp.json()["embeddings"]
    return ["[" + ",".join(map(str, e)) + "]" for e in embs]


async def main() -> None:
    records = list(_iter_records())
    # chunk_id 중복 제거(파일 간 혹시 모를 충돌)
    uniq = {}
    for r in records:
        uniq[r["chunk_id"]] = r
    records = list(uniq.values())
    print(f"적재 대상: {len(records)} 청크 (임베딩 시작)")

    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("TRUNCATE case_chunks")
    done = 0
    async with httpx.AsyncClient(timeout=120) as client:
        for i in range(0, len(records), BATCH):
            batch = records[i : i + BATCH]
            embs = await _embed_batch(client, [b["content"] for b in batch])
            rows = []
            for b, emb in zip(batch, embs):
                toks = " ".join(t.form for t in _kiwi.tokenize(b["content"][:2000]))
                rows.append((
                    b["chunk_id"], b["content"], toks, emb, b["chunk_type"], b["doc_hash"],
                    b["source_type"], b["institution"], b["case_no"], b["case_title"],
                    b["decision_date"], b["source_url"], b["accident_type"], b["tags"],
                    b["section"], b["chunk_index"],
                ))
            await conn.executemany(
                """INSERT INTO case_chunks
                   (chunk_id, content, content_tokens, embedding, chunk_type, doc_hash,
                    source_type, institution, case_no, case_title, decision_date, source_url,
                    accident_type, tags, section, chunk_index)
                   VALUES ($1,$2,$3,$4::halfvec,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                   ON CONFLICT (chunk_id) DO NOTHING""",
                rows,
            )
            done += len(batch)
            if done % 640 == 0 or done == len(records):
                print(f"  {done}/{len(records)}")
    n = await conn.fetchval("SELECT count(*) FROM case_chunks")
    by = await conn.fetch("SELECT source_type, count(*) FROM case_chunks GROUP BY source_type")
    await conn.close()
    print(f"완료: case_chunks {n} 행")
    for r in by:
        print(f"  {r['source_type']}: {r['count']}")


if __name__ == "__main__":
    asyncio.run(main())
