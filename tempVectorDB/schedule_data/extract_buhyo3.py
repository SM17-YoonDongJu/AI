"""hwp5html 산출 xhtml에서 <부표 3> 장해분류표 구간을 순서대로 추출한다.

문단은 텍스트 줄로, 표는 마크다운 표로 렌더링해 하나의 텍스트 파일로 덤프한다.
표 구조(행/열)는 hwp5html이 <table><tr><td>로 보존하므로 그대로 읽는다.
"""

from __future__ import annotations

import re
import sys

from lxml import html as lxml_html

XHTML = sys.argv[1]
OUT = sys.argv[2]
# 부표3 구간 경계: 시작 = n번째 "장 해 분 류 표"(생보=1), 끝 = "재해분류표"/"부표 4"
NTH = int(sys.argv[3]) if len(sys.argv) > 3 else 1

doc = lxml_html.parse(XHTML).getroot()
body = doc.find(".//{*}body") if doc.find(".//{*}body") is not None else doc


def cell_text(el) -> str:
    txt = "".join(el.itertext())
    return re.sub(r"[​\r\n]+", " ", txt).strip()


def table_md(tbl) -> str:
    rows = []
    for tr in tbl.iter("{*}tr"):
        cells = [cell_text(td) for td in tr.iter("{*}td")]
        rows.append(cells)
    if not rows:
        return ""
    ncol = max(len(r) for r in rows)
    rows = [r + [""] * (ncol - len(r)) for r in rows]
    out = []
    for i, r in enumerate(rows):
        out.append("| " + " | ".join(c.replace("|", "/") for c in r) + " |")
        if i == 0:
            out.append("|" + "|".join(["---"] * ncol) + "|")
    return "\n".join(out)


# 문서 순서대로 최상위 p / table 을 수집 (표 안의 p 는 제외)
seq = []  # (kind, payload)
seen_tables = set()
for el in body.iter():
    tag = el.tag
    if not isinstance(tag, str):
        continue
    tag = tag.split("}")[-1]
    if tag == "table":
        # 중첩표 방지: 조상에 table 있으면 skip
        anc = el.getparent()
        nested = False
        while anc is not None:
            at = anc.tag
            if isinstance(at, str) and at.split("}")[-1] == "table":
                nested = True
                break
            anc = anc.getparent()
        if nested:
            continue
        seq.append(("table", table_md(el)))
    elif tag == "p":
        # 표 안의 p 는 제외
        anc = el.getparent()
        inside = False
        while anc is not None:
            at = anc.tag
            if isinstance(at, str) and at.split("}")[-1] == "table":
                inside = True
                break
            anc = anc.getparent()
        if inside:
            continue
        txt = cell_text(el)
        if txt:
            seq.append(("p", txt))

# 경계 찾기
starts = [i for i, (k, t) in enumerate(seq) if k == "p" and t.replace(" ", "") == "장해분류표"]
if len(starts) < NTH:
    print("WARN: 장해분류표 헤더를 찾지 못함, starts=", starts, file=sys.stderr)
start = starts[NTH - 1]
# 끝: start 이후 첫 "재해분류표"
end = len(seq)
for i in range(start + 1, len(seq)):
    k, t = seq[i]
    if k == "p" and ("재해분류표" in t or t.replace(" ", "").startswith("<부표4>")):
        end = i
        break

lines = []
for k, t in seq[start:end]:
    if k == "table":
        lines.append("\n[TABLE]")
        lines.append(t)
        lines.append("[/TABLE]\n")
    else:
        lines.append(t)
open(OUT, "w", encoding="utf-8").write("\n".join(lines))
print(f"OK {XHTML}: seq={len(seq)} start={start} end={end} -> {OUT} ({end - start} blocks)")
