"""약관 청크(chunks_merged.json) → policy_chunks 적재 (임베딩 포함).

Policy-Chunker converter의 `*_chunks_merged.json`(조/항/호/특약 구조화 + VLM 표 병합)을 읽어
qwen3-embedding으로 임베딩 후 policy_chunks에 적재한다. GPU 박스(Ollama·Postgres localhost)에서
주피터/CLI로 실행하는 것을 전제로 한다.

보험사·상품명·시행일·doc_hash는 청크 파일에 없다(노션 행 메타) → **인자로 넘긴다.**
문서는 raw로 임베딩한다(쿼리만 instruction 프리픽스 — src/rag/search.py 계약).

단일 파일:
    python tempVectorDB/load_policy_chunks.py \
      --json 187baa36..._chunks_merged.json \
      --insurer 현대해상 \
      --product "무배당현대해상행복가득생활보장보험(Hi2601) 2종(세만기)" \
      --effective-date 2026-01-01 \
      --doc-hash 160368ab8296b895...   # 노션 SHA256(생략 시 json 파일 sha256)

여러 파일(권장 — 노션 행별 메타를 한 줄씩):
    python tempVectorDB/load_policy_chunks.py --manifest files.jsonl
    # files.jsonl 각 줄: {"json": "...", "insurer": "...", "product": "...",
    #                    "effective_date": "2026-01-01", "doc_hash": "...", "product_code": ""}

전제: Ollama(EMBED_MODEL)·Postgres(policy_chunks) 접근 가능. 상품 필터는 insurer+product_name
정확일치이므로, **벤치마크 시나리오가 쓰는 product_name과 철자·표기를 반드시 일치**시킬 것.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import gzip
import hashlib
import json
import os
import ssl
from typing import Any

import asyncpg
import httpx
from kiwipiepy import Kiwi

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/aiengine"
)
RDS_CA_PATH = os.environ.get("RDS_CA_PATH")  # RDS면 CA 번들 경로(TLS 검증). 로컬이면 미설정→SSL off
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "qwen3-embedding:0.6b")
EMBED_DIM = int(os.environ.get("EMBED_DIM", "1024"))
BATCH = int(os.environ.get("EMBED_BATCH", "32"))
TOKEN_CAP = 4000  # Kiwi 형태소 분석 입력 상한(긴 표 청크의 CPU 폭주 방지)

_kiwi = Kiwi()


def _ssl() -> ssl.SSLContext | bool:
    """RDS면 CA 검증 SSL 컨텍스트, 로컬이면 False(SSL off) — core.db와 동일 규약."""
    return ssl.create_default_context(cafile=RDS_CA_PATH) if RDS_CA_PATH else False


_INSERT = """
INSERT INTO policy_chunks
    (chunk_id, content, content_tokens, embedding, token_count, chunk_type, doc_hash,
     page_number, insurer, product_name, product_code, effective_date,
     article_number, article_title, section, chunk_index)
VALUES ($1,$2,$3,$4::halfvec,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
ON CONFLICT (chunk_id) DO NOTHING
"""


def _article_number(a: Any) -> str | None:
    """청크의 article_no(int|str|None) → policy_chunks.article_number('제N조')."""
    if a is None or a == "":
        return None
    if isinstance(a, int):
        return f"제{a}조"
    s = str(a).strip()
    return s if s.startswith("제") else f"제{s}조"


def _date(s: str | None) -> datetime.date | None:
    if not s:
        return None
    try:
        return datetime.date.fromisoformat(str(s).strip()[:10])
    except ValueError:
        return None


def _file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for blk in iter(lambda: f.read(1 << 20), b""):
            h.update(blk)
    return h.hexdigest()


def _tokens(text: str) -> str:
    """Kiwi 형태소 표층형을 공백 결합 → tsvector 전문검색용 content_tokens."""
    return " ".join(t.form for t in _kiwi.tokenize(text[:TOKEN_CAP]))


async def _embed_batch(client: httpx.AsyncClient, texts: list[str]) -> list[str]:
    """qwen3-embedding 배치 호출 → halfvec 리터럴 문자열 리스트. 문서는 raw(프리픽스 X)."""
    resp = await client.post(f"{OLLAMA_URL}/api/embed", json={"model": EMBED_MODEL, "input": texts})
    resp.raise_for_status()
    embs = resp.json()["embeddings"]
    for e in embs:
        if len(e) != EMBED_DIM:
            raise ValueError(
                f"임베딩 차원 불일치: {len(e)} != {EMBED_DIM} (EMBED_MODEL={EMBED_MODEL})"
            )
    return ["[" + ",".join(map(str, e)) + "]" for e in embs]


def _prepare(
    chunks: list[dict],
    insurer: str,
    product: str,
    product_code: str,
    eff: datetime.date | None,
    doc_hash: str,
) -> list[dict]:
    """청크 dict 리스트 → policy_chunks 행 dict 리스트(임베딩 전). chunk_id 중복 제거·빈 본문 스킵."""
    seen: dict[str, dict] = {}
    for i, c in enumerate(chunks):
        text = (c.get("text") or "").strip()
        cid = c.get("chunk_id")
        if not text or not cid or cid in seen:
            continue
        seen[cid] = {
            "chunk_id": cid,
            "content": text,
            "chunk_type": c.get("chunk_type") or "general",
            "page_number": c.get("page"),
            "article_number": _article_number(c.get("article_no")),
            "article_title": c.get("article_title"),
            "section": c.get("section"),
            "chunk_index": i,
            "insurer": insurer,
            "product_name": product,
            "product_code": product_code or None,
            "effective_date": eff,
            "doc_hash": doc_hash,
        }
    return list(seen.values())


async def load_file(
    conn: asyncpg.Connection,
    client: httpx.AsyncClient,
    *,
    json_path: str,
    insurer: str,
    product: str,
    effective_date: str | None,
    doc_hash: str | None = None,
    product_code: str = "",
    limit: int | None = None,
) -> int:
    """단일 chunks_merged.json 적재. doc_hash 기준 기존 행 삭제 후 재적재(멱등)."""
    opener = gzip.open if json_path.endswith(".gz") else open  # 노션 첨부(.gz) 직접 읽기
    with opener(json_path, "rt", encoding="utf-8") as f:
        chunks = json.load(f)
    if not isinstance(chunks, list):
        raise ValueError(f"{json_path}: 최상위가 list가 아님({type(chunks).__name__})")
    if limit:
        chunks = chunks[:limit]

    dh = doc_hash or _file_sha256(json_path)
    eff = _date(effective_date)
    rows = _prepare(chunks, insurer, product, product_code, eff, dh)
    print(
        f"  [{product}] 원본 {len(chunks)} → 적재대상 {len(rows)} (insurer={insurer}, doc_hash={dh[:12]}…)"
    )

    # 멱등: 이 문서(doc_hash)의 기존 행 제거 후 재적재
    await conn.execute("DELETE FROM policy_chunks WHERE doc_hash = $1", dh)

    done = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i : i + BATCH]
        embs = await _embed_batch(client, [r["content"] for r in batch])
        records = []
        for r, emb in zip(batch, embs):
            toks = _tokens(r["content"])
            records.append(
                (
                    r["chunk_id"],
                    r["content"],
                    toks,
                    emb,
                    len(toks.split()),
                    r["chunk_type"],
                    r["doc_hash"],
                    r["page_number"],
                    r["insurer"],
                    r["product_name"],
                    r["product_code"],
                    r["effective_date"],
                    r["article_number"],
                    r["article_title"],
                    r["section"],
                    r["chunk_index"],
                )
            )
        await conn.executemany(_INSERT, records)
        done += len(batch)
        if done % 320 == 0 or done == len(rows):
            print(f"    {done}/{len(rows)}")
    return len(rows)


def _jobs_from_args(args: argparse.Namespace) -> list[dict]:
    if args.manifest:
        jobs = []
        with open(args.manifest, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    jobs.append(json.loads(line))
        return jobs
    if not (args.json and args.insurer and args.product):
        raise SystemExit("단일 모드는 --json --insurer --product 필수(또는 --manifest 사용)")
    return [
        {
            "json": args.json,
            "insurer": args.insurer,
            "product": args.product,
            "effective_date": args.effective_date,
            "doc_hash": args.doc_hash,
            "product_code": args.product_code,
        }
    ]


async def main() -> None:
    p = argparse.ArgumentParser(description="약관 chunks_merged.json → policy_chunks 적재")
    p.add_argument("--json", help="단일 chunks_merged.json 경로")
    p.add_argument("--insurer", help="보험사(노션 회사) — insurer 컬럼")
    p.add_argument("--product", help="상품명(노션 상품명) — product_name 컬럼(정확일치 필터 키)")
    p.add_argument("--effective-date", help="시행일 YYYY-MM-DD(노션 시행일)")
    p.add_argument("--doc-hash", help="PDF SHA256(노션). 생략 시 json 파일 sha256")
    p.add_argument("--product-code", default="", help="상품코드(노션, 선택)")
    p.add_argument(
        "--manifest", help="여러 파일 배치: 각 줄 {json,insurer,product,effective_date,doc_hash}"
    )
    p.add_argument("--limit", type=int, help="파일당 앞 N개만(스모크 테스트)")
    args = p.parse_args()

    jobs = _jobs_from_args(args)
    conn = await asyncpg.connect(DATABASE_URL, ssl=_ssl())
    total = 0
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            for j in jobs:
                total += await load_file(
                    conn,
                    client,
                    json_path=j["json"],
                    insurer=j["insurer"],
                    product=j["product"],
                    effective_date=j.get("effective_date"),
                    doc_hash=j.get("doc_hash"),
                    product_code=j.get("product_code", ""),
                    limit=args.limit,
                )
        n = await conn.fetchval("SELECT count(*) FROM policy_chunks")
        by = await conn.fetch(
            "SELECT insurer, product_name, count(*) AS n FROM policy_chunks GROUP BY 1,2 ORDER BY 1,2"
        )
    finally:
        await conn.close()
    print(f"\n완료: 이번 적재 {total} 행 · policy_chunks 총 {n} 행")
    for r in by:
        print(f"  {r['insurer']} | {r['product_name']}: {r['n']}")


if __name__ == "__main__":
    asyncio.run(main())
