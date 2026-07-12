"""b3_*.txt 추출본 + parsed_*.json 을 load_schedule.py 파서 형식 md 로 변환한다.

출력: '## 총칙' + 13개 '## <부위>' 섹션. 각 부위는 '가. 장해의 분류'(항목|지급률 표) +
'나. 판정기준' 본문. HWP 표 병합셀에서 페어링한 항목·지급률을 마크다운 표로 낸다.

rename 인자(선택): current(2025.6) 판의 간질→뇌전증 개정을 rev2019 본문에 적용한다.
검증된 문자열 치환만 수행하며(추측 금지), 치환 목록은 VERIFICATION.md 에 기록한다.
"""

from __future__ import annotations

import json
import re
import sys

SRC = sys.argv[1]  # b3_*.txt
PARSED = sys.argv[2]  # parsed_*.json
OUT = sys.argv[3]  # 출력 md
RENAME = sys.argv[4] if len(sys.argv) > 4 else None  # 'epilepsy' → 간질→뇌전증

SECTION_RE = re.compile(r"^(\d{1,2})\.\s*(.+?장해.*)$")
LABELS = [
    "눈",
    "귀",
    "코",
    "씹어먹거나 말하는 장해",
    "외모",
    "척추",
    "체간골",
    "팔",
    "다리",
    "손가락",
    "발가락",
    "흉복부장기 및 비뇨생식기",
    "신경계·정신행동",
]

# 2025.6.30 개정: 간질→뇌전증 (검증된 치환, 순서 중요). '만성간질환'(肝질환) 보호.
EPILEPSY_SUBS = [
    ("만성간질환", "\x00LIVER\x00"),  # 보호
    ("뇌전증(간질)", "뇌전증"),
    ("항간질제(항전간제)", "항뇌전증제(항경련제)"),
    ("간질발작", "뇌전증발작"),
    ("간질 발작", "뇌전증 발작"),
    ("외상후 간질", "외상후 뇌전증"),
    ("조절되지 않는 간질", "조절되지 않는 뇌전증"),
    ("“간질”", "“뇌전증”"),
    ("‘간질’", "‘뇌전증’"),
    ("간질을 말한다", "뇌전증을 말한다"),
    ("\x00LIVER\x00", "만성간질환"),
]


def apply_rename(text: str) -> str:
    if RENAME == "epilepsy":
        for a, b in EPILEPSY_SUBS:
            text = text.replace(a, b)
    return text


def read_blocks(path: str):
    lines = open(path, encoding="utf-8").read().splitlines()
    blocks, i = [], 0
    while i < len(lines):
        s = lines[i].strip()
        if s == "[TABLE]":
            rows, i = [], i + 1
            while i < len(lines) and lines[i].strip() != "[/TABLE]":
                row = lines[i]
                if row.startswith("|") and set(row.replace(" ", "")) != set("|-"):
                    rows.append([c.strip() for c in row.strip().strip("|").split("|")])
                i += 1
            blocks.append(("table", rows))
        elif s:
            blocks.append(("p", lines[i].rstrip()))
        i += 1
    return blocks


def norm(s: str) -> str:
    return re.sub(r"\s+", "", s)


def table_cell_concat(rows):
    return norm("".join(c for r in rows for c in r))


def is_figure_table(rows):
    """<가슴뼈> 등 그림 캡션만 든 표 → 데이터 없음."""
    txt = "".join(c for r in rows for c in r).strip()
    return bool(re.fullmatch(r"[<>\s가-힣]*", txt)) and "<" in txt and "지급률" not in txt


def render_table(rows):
    if not rows:
        return ""
    ncol = max(len(r) for r in rows)
    rows = [r + [""] * (ncol - len(r)) for r in rows]
    out = ["| " + " | ".join(rows[0]) + " |", "|" + "|".join(["---"] * ncol) + "|"]
    for r in rows[1:]:
        out.append("| " + " | ".join(r) + " |")
    return "\n".join(out)


def schedule_md(paired):
    out = ["| 장해의 분류 | 지급률 |", "|---|---|"]
    for p in paired:
        item = apply_rename(p["item"])
        out.append(f"| {item} | {p['rate']} |")
    return "\n".join(out)


def main():
    blocks = read_blocks(SRC)
    parsed = json.load(open(PARSED, encoding="utf-8"))

    # 섹션 헤더 인덱스 (부위 1..13, '눈' 이후만)
    sec = []
    for bi, (k, pay) in enumerate(blocks):
        if k == "p":
            m = SECTION_RE.match(pay.strip())
            if m and int(m.group(1)) <= 13 and "장해의 정의" not in pay:
                sec.append((bi, int(m.group(1)), m.group(2).strip()))
    # 눈(=1.눈의 장해) 시작 인덱스: 첫 '눈' 헤더
    body_start = None
    for bi, no, title in sec:
        if title.startswith("눈"):
            body_start = bi
            break

    out = ["# 장해분류표 (부표3)", ""]

    # 총칙: 처음부터 body_start 전까지 p 블록 (표 없음)
    out.append("## 총칙")
    for k, pay in blocks[:body_start]:
        if k != "p":
            continue
        t = pay.strip()
        if t in ("장 해 분 류 표", "장해분류표"):
            continue
        if re.fullmatch(r"[󰠧\s]+", t):
            continue
        t = t.replace("󰊱", "").replace("󰊲", "").strip()
        if not t:
            continue
        out.append(apply_rename(t))
    out.append("")

    # 부위별
    body_sec = [(bi, no, title) for bi, no, title in sec if bi >= body_start]
    for si, (bi, no, title) in enumerate(body_sec):
        end = body_sec[si + 1][0] if si + 1 < len(body_sec) else len(blocks)
        label = LABELS[no - 1] if no - 1 < len(LABELS) else title
        out.append(f"## {label}")
        # 가. 장해의 분류 (페어링 표)
        out.append("### 가. 장해의 분류")
        out.append(schedule_md(parsed[label]["paired"]))
        out.append("")
        # 이 섹션 안 모든 표의 normed concat → 앞에 붙는 flatten 중복 문단 식별용
        sec_blocks = blocks[bi + 1 : end]
        tbl_concats = {table_cell_concat(pay) for k, pay in sec_blocks if k == "table"}
        # 나. 판정기준 등: 첫 schedule 표는 제외, 이후 본문/보조표
        seen_first_table = False
        for k, pay in sec_blocks:
            if k == "table":
                if not seen_first_table:
                    seen_first_table = True  # 가.장해분류 표 → 이미 렌더
                    continue
                if is_figure_table(pay):
                    continue
                out.append("")
                out.append(render_table([[apply_rename(c) for c in r] for r in pay]))
                out.append("")
                continue
            t = pay.strip()
            if not t:
                continue
            # 표를 flatten 한 중복 문단 제거 (표 concat 과 동일)
            if norm(t) in tbl_concats:
                continue
            # 우리가 직접 렌더한 '가. 장해의 분류' 헤더 중복 제거
            if re.fullmatch(r"가\.\s*장해의\s*분류", t):
                continue
            out.append(apply_rename(t))
        out.append("")

    open(OUT, "w", encoding="utf-8").write("\n".join(out) + "\n")
    print(f"OK -> {OUT} ({len(out)} lines)")


if __name__ == "__main__":
    main()
