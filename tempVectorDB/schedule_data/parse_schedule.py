"""b3_*.txt(부표3 추출본)에서 신체부위별 '가. 장해의 분류' 표를 항목|지급률로 페어링한다.

HWP 표에서 항목열과 지급률열이 하나의 셀로 병합돼 있어도(예: 눈), 항목은 'N)' 마커로,
지급률은 공백으로 분해해 위치 기준으로 1:1 매칭한다. 개수가 어긋나면 mismatch 로 표시(추측 금지).
"""

from __future__ import annotations

import json
import re
import sys

SRC = sys.argv[1]
OUT_JSON = sys.argv[2] if len(sys.argv) > 2 else None

# 13개 신체부위 헤더: "1. 눈의 장해" ... "13. 신경계.정신행동 장해"
SECTION_RE = re.compile(r"^(\d{1,2})\.\s*(.+?장해.*)$")
# 항목 마커: 줄 안에서 "N)" (1~2자리). 앞이 숫자가 아니어야 진짜 마커.
ITEM_SPLIT_RE = re.compile(r"(?<!\d)(\d{1,2})\)\s*")
# 지급률 토큰: 정수 또는 범위형 "10~100"
RATE_RE = re.compile(r"\d+(?:~\d+)?")

# 짧은 body_part 라벨 (md 헤더용)
LABEL = {
    "눈": "눈",
    "귀": "귀",
    "코": "코",
    "씹어먹거나": "씹어먹거나 말하는 장해",
    "외모": "외모",
    "척추": "척추",
    "체간골": "체간골",
    "팔": "팔",
    "다리": "다리",
    "손가락": "손가락",
    "발가락": "발가락",
    "흉": "흉복부장기 및 비뇨생식기",
    "신경계": "신경계·정신행동",
}


def short_label(title: str) -> str:
    for key, lab in LABEL.items():
        if title.startswith(key) or key in title[:4]:
            return lab
    return title


def read_blocks(path: str):
    """(kind, payload) 순차 블록. kind in {'p','table'}. table payload=행 리스트."""
    lines = open(path, encoding="utf-8").read().splitlines()
    blocks = []
    i = 0
    while i < len(lines):
        ln = lines[i]
        if ln.strip() == "[TABLE]":
            rows = []
            i += 1
            while i < len(lines) and lines[i].strip() != "[/TABLE]":
                row = lines[i]
                if row.startswith("|") and not re.match(r"^\|[-|]+\|?$", row.replace(" ", "")):
                    cells = [c.strip() for c in row.strip().strip("|").split("|")]
                    rows.append(cells)
                i += 1
            blocks.append(("table", rows))
        else:
            if ln.strip():
                blocks.append(("p", ln))
        i += 1
    return blocks


def split_items(item_text: str) -> list[str]:
    """'1) a 2) b ...' → ['1) a','2) b',...] (마커 보존)."""
    parts = ITEM_SPLIT_RE.split(item_text)
    # parts = [pre, num, body, num, body, ...]
    items = []
    j = 1
    while j < len(parts):
        num = parts[j]
        body = parts[j + 1].strip() if j + 1 < len(parts) else ""
        items.append((int(num), f"{num}) {body}".strip()))
        j += 2
    return items


def parse_schedule_table(rows: list[list[str]]) -> dict:
    """첫 표(가. 장해의 분류)를 (번호, 항목, 지급률) 리스트로. header 행 스킵."""
    item_chunks, rate_chunks = [], []
    for r in rows:
        if not r:
            continue
        first = r[0]
        last = r[-1]
        if first in ("장해의 분류", "유형", "항목") and last in (
            "지급률",
            "제한 정도에 따른 지급률",
            "점수",
            "내  용",
        ):
            continue
        item_chunks.append(first)
        rate_chunks.append(last)
    item_text = " ".join(c for c in item_chunks if c)
    rate_text = " ".join(c for c in rate_chunks if c)
    items = split_items(item_text)
    rates = RATE_RE.findall(rate_text)
    paired = []
    ok = len(items) == len(rates)
    for k, (num, itxt) in enumerate(items):
        rate = rates[k] if k < len(rates) else None
        paired.append({"no": num, "item": itxt, "rate": rate})
    return {
        "paired": paired,
        "n_items": len(items),
        "n_rates": len(rates),
        "match": ok,
        "rate_text": rate_text,
    }


def main():
    blocks = read_blocks(SRC)
    # 섹션 경계 인덱스
    sec_idx = []
    for bi, (kind, payload) in enumerate(blocks):
        if kind == "p":
            m = SECTION_RE.match(payload.strip())
            if m and int(m.group(1)) <= 13:
                sec_idx.append((bi, int(m.group(1)), m.group(2).strip()))
    result = {}
    for si, (bi, no, title) in enumerate(sec_idx):
        end = sec_idx[si + 1][0] if si + 1 < len(sec_idx) else len(blocks)
        # 이 섹션의 첫 table
        first_tbl = None
        for k in range(bi + 1, end):
            if blocks[k][0] == "table":
                first_tbl = blocks[k][1]
                break
        label = short_label(title)
        if first_tbl is None:
            result[label] = {"no": no, "title": title, "error": "no table"}
            continue
        parsed = parse_schedule_table(first_tbl)
        parsed["no"] = no
        parsed["title"] = title
        result[label] = parsed

    # 요약 출력
    print(f"# {SRC}")
    for label, d in result.items():
        if "error" in d:
            print(f"  [{d['no']:>2}] {label}: ERROR {d['error']}")
            continue
        flag = "OK" if d["match"] else "**MISMATCH**"
        print(f"  [{d['no']:>2}] {label}: items={d['n_items']} rates={d['n_rates']} {flag}")
        if not d["match"]:
            print(f"        rate_text={d['rate_text']!r}")
            for p in d["paired"]:
                print(f"          {p['no']}) rate={p['rate']} | {p['item'][:50]}")
    if OUT_JSON:
        json.dump(result, open(OUT_JSON, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print(f"  -> {OUT_JSON}")


if __name__ == "__main__":
    main()
