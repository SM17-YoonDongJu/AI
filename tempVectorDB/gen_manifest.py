"""로컬 메리츠 body 약관 → 상품별 최신버전만 골라 ingest manifest YAML 생성.

파일명 패턴: {카테고리}_{상품명}_{버전번호}.pdf  (버전번호 큰 게 최신).
auto_doc_type 오분류(→product_summary) 회피 위해 doc_type=policy_terms 강제.
"""
import glob
import os
import re

import yaml

BODY = r"C:\Users\wkdrn\test\_workspace\raw_dataset\insurance_pdfs\meritz_yakkwan_body"
OUT = r"C:\Users\wkdrn\project\Ai\tempVectorDB\meritz_latest_manifest.yaml"

best = {}  # product_name -> (filename, version)
for path in glob.glob(os.path.join(BODY, "*.pdf")):
    f = os.path.basename(path)
    m = re.match(r"(.+)_(\d+)\.pdf$", f)
    if not m:
        continue
    prod, ver = m.group(1), int(m.group(2))
    if prod not in best or ver > best[prod][1]:
        best[prod] = (f, ver)

rows = []
for prod, (f, ver) in sorted(best.items()):
    # prod = "상해보험_다모아상해보험" 형태 → 뒤쪽이 상품명
    name = prod.split("_", 1)[-1]
    rows.append({
        "pdf": os.path.join(BODY, f).replace("\\", "/"),
        "doc_type": "policy_terms",
        "insurer": "메리츠화재",
        "product_name": name,
        "product_code": str(ver),
    })

yaml.safe_dump({"documents": rows}, open(OUT, "w", encoding="utf-8"),
               allow_unicode=True, sort_keys=False)
total_mb = round(sum(os.path.getsize(r["pdf"]) for r in rows) / 1e6, 1)
print(f"저장: {OUT} | 상품수: {len(rows)} | 총 {total_mb}MB")
