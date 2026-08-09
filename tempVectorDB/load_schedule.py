"""후유장해분류표(금감원 시행세칙 별표) → schedule_chunks 적재 (임베딩 포함).

버전별(개정판) 원문 마크다운을 "## 신체부위" 헤더 단위로 청킹해, 계약 체결일 기준 버전
매칭이 가능하도록 (version_label, applies_from, applies_to) 메타와 함께 적재한다.
임베딩은 policy_chunks·case_chunks 와 동일한 qwen3-embedding 1024d 로, 벡터공간을 맞춘다.

입력:
  - 매니페스트(YAML): 버전별 version_label·applies_from·applies_to·source_url·파일 경로.
    형식은 tempVectorDB/schedule_manifest.example.yaml 참조.
  - 버전별 원문 마크다운: "## 신체부위" 헤더 + 그 아래 마크다운 표(장해등급/지급률/내용).

청킹 전략:
  - 신체부위(body_part) 섹션 단위로 청크를 만든다("## 헤더" 경계). 표 행 그룹은 한 청크
    안에 통째로 유지해, 등급·지급률 행이 청크 경계로 잘리지 않게 한다.
  - disability_grade·rate 는 섹션이 단일 지급률 항목일 때만 채우고, 여러 행이면 NULL 로 둔다
    (본문 표에 등급·지급률이 함께 담기므로 검색·인용에는 섹션 content 로 충분).

실행:
  python tempVectorDB/load_schedule.py --manifest tempVectorDB/schedule_manifest.yaml
  # --manifest 생략 시 schedule_manifest.example.yaml 을 읽는다(스모크용).

원문 데이터가 아직 미확보라면, 매니페스트가 가리키는 마크다운 파일만 채우면 그대로 돌아간다.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import hashlib
import os
import pathlib
import re

import asyncpg
import httpx
import yaml
from kiwipiepy import Kiwi

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/aiengine"
)
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "qwen3-embedding:0.6b")
BATCH = 32

_DEFAULT_MANIFEST = pathlib.Path(__file__).with_name("schedule_manifest.example.yaml")

_kiwi = Kiwi()

# 마크다운 헤더 "## 신체부위" 매칭 (부위 라벨 캡처).
_HEADER_RE = re.compile(r"^##\s+(.+?)\s*$")
# 단일 지급률 감지용 — 섹션에 지급률(%)이 한 번만 나오면 그 값을 rate 로 채운다.
_RATE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")


def _parse_date(value: object) -> datetime.date | None:
    """YAML 값(YYYY-MM-DD 문자열 또는 date)을 date 로 정규화한다. None/빈값은 None."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime.date):
        return value
    return datetime.date.fromisoformat(str(value).strip())


def _parse_markdown(text: str) -> list[tuple[str, str]]:
    """'## 신체부위' 헤더로 분할해 (body_part, section_body) 리스트를 만든다.

    표 행은 섹션 본문에 그대로 남아 한 청크로 묶인다(행 그룹 유지). 헤더 앞의 서문(전문)은
    무시한다.

    Args:
        text: 버전별 원문 마크다운 전체.

    Returns:
        (body_part, 섹션 본문) 튜플 리스트(문서 순서 보존).
    """
    sections: list[tuple[str, str]] = []
    current_part: str | None = None
    buffer: list[str] = []
    for line in text.splitlines():
        header = _HEADER_RE.match(line)
        if header:
            if current_part is not None:
                sections.append((current_part, "\n".join(buffer).strip()))
            current_part = header.group(1).strip()
            buffer = []
        elif current_part is not None:
            buffer.append(line)
    if current_part is not None:
        sections.append((current_part, "\n".join(buffer).strip()))
    return sections


def _single_rate(body: str) -> float | None:
    """섹션 본문에 지급률(%)이 정확히 하나면 그 값을 반환한다(단일 항목 섹션). 아니면 None."""
    rates = _RATE_RE.findall(body)
    if len(rates) == 1:
        return float(rates[0])
    return None


def _iter_records(manifest_path: pathlib.Path):
    """매니페스트를 읽어 schedule_chunks 컬럼 dict 제너레이터를 만든다.

    파일 경로는 매니페스트 위치 기준 상대경로 또는 절대경로로 해석한다.
    """
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    base = manifest_path.parent
    for version in manifest.get("versions", []):
        version_label = version["version_label"]
        applies_from = _parse_date(version["applies_from"])
        applies_to = _parse_date(version.get("applies_to"))
        source_url = version.get("source_url")
        for rel in version.get("files", []):
            path = pathlib.Path(rel)
            if not path.is_absolute():
                path = base / path
            text = path.read_text(encoding="utf-8")
            for idx, (body_part, body) in enumerate(_parse_markdown(text)):
                content = f"## {body_part}\n{body}".strip()
                # doc_hash: 버전+부위+본문 기반 sha256 — 동일 판 재적재 시 중복 방지.
                doc_hash = hashlib.sha256(
                    f"{version_label}\x00{body_part}\x00{content}".encode()
                ).hexdigest()
                yield {
                    "chunk_id": f"{doc_hash[:16]}:{idx}",
                    "content": content,
                    "chunk_type": "schedule",
                    "doc_hash": doc_hash,
                    "body_part": body_part,
                    "disability_grade": None,
                    "rate": _single_rate(body),
                    "version_label": version_label,
                    "applies_from": applies_from,
                    "applies_to": applies_to,
                    "source_url": source_url,
                    "section": body_part,
                    "chunk_index": idx,
                }


async def _embed_batch(client: httpx.AsyncClient, texts: list[str]) -> list[str]:
    resp = await client.post(f"{OLLAMA_URL}/api/embed", json={"model": EMBED_MODEL, "input": texts})
    resp.raise_for_status()
    embs = resp.json()["embeddings"]
    return ["[" + ",".join(map(str, e)) + "]" for e in embs]


async def main() -> None:
    parser = argparse.ArgumentParser(description="후유장해분류표 → schedule_chunks 적재")
    parser.add_argument(
        "--manifest",
        type=pathlib.Path,
        default=_DEFAULT_MANIFEST,
        help="버전 매니페스트 YAML 경로 (기본: schedule_manifest.example.yaml)",
    )
    args = parser.parse_args()

    records = list(_iter_records(args.manifest))
    # chunk_id 중복 제거(파일 간 혹시 모를 충돌).
    uniq = {r["chunk_id"]: r for r in records}
    records = list(uniq.values())
    print(f"적재 대상: {len(records)} 청크 (매니페스트: {args.manifest})")
    if not records:
        print("적재할 청크가 없습니다(매니페스트 files 를 확인하세요).")
        return

    conn = await asyncpg.connect(DATABASE_URL)
    done = 0
    async with httpx.AsyncClient(timeout=120) as client:
        for i in range(0, len(records), BATCH):
            batch = records[i : i + BATCH]
            embs = await _embed_batch(client, [b["content"] for b in batch])
            rows = []
            for b, emb in zip(batch, embs, strict=True):
                toks = " ".join(t.form for t in _kiwi.tokenize(b["content"][:2000]))
                rows.append(
                    (
                        b["chunk_id"],
                        b["content"],
                        toks,
                        emb,
                        b["chunk_type"],
                        b["doc_hash"],
                        b["body_part"],
                        b["disability_grade"],
                        b["rate"],
                        b["version_label"],
                        b["applies_from"],
                        b["applies_to"],
                        b["source_url"],
                        b["section"],
                        b["chunk_index"],
                    )
                )
            await conn.executemany(
                """INSERT INTO schedule_chunks
                   (chunk_id, content, content_tokens, embedding, chunk_type, doc_hash,
                    body_part, disability_grade, rate,
                    version_label, applies_from, applies_to, source_url, section, chunk_index)
                   VALUES ($1,$2,$3,$4::halfvec,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                   ON CONFLICT (chunk_id) DO NOTHING""",
                rows,
            )
            done += len(batch)
            print(f"  {done}/{len(records)}")
    n = await conn.fetchval("SELECT count(*) FROM schedule_chunks")
    by = await conn.fetch(
        "SELECT version_label, count(*) FROM schedule_chunks GROUP BY version_label"
    )
    await conn.close()
    print(f"완료: schedule_chunks {n} 행")
    for r in by:
        print(f"  {r['version_label']}: {r['count']}")


if __name__ == "__main__":
    asyncio.run(main())
