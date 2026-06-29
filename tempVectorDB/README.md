# tempVectorDB — RAG 실험용 로컬 pgvector

메리츠 약관 임베딩을 적재해 `rag.search()` / LangGraph 리포트 워커를 실험하기 위한 **임시** 로컬 DB.
운영 DB(AWS RDS)와 별개이며, 실험이 끝나면 폐기한다.

## 기동

```bash
docker compose -f tempVectorDB/docker-compose.yml up -d
# 접속: postgresql://postgres:postgres@localhost:5433/aiengine
```

- 포트 **5433** (메인 앱 5432와 충돌 회피)
- `init/01_schema.sql` 은 **빈 볼륨 최초 1회만** 자동 실행 (policy_chunks·search_terms + 인덱스)
- 스키마 재적용하려면 볼륨 삭제 후 재기동: `docker compose -f tempVectorDB/docker-compose.yml down -v`

## 약관 임베딩 적재 (Policy-Chunker)

별도 레포 [Policy-Chunker(insurance-chunker)](https://github.com/SM17-YoonDongJu/Policy-Chunker/tree/insurance-chunker)에서 실행:

```bash
# CPU만으로 실험하려면 BGE-M3 백엔드 (Ollama 불필요)
export EMBED_BACKEND=sentence_transformers   # 또는 ollama + qwen3:embedding
export DATABASE_URL=postgresql://postgres:postgres@localhost:5433/aiengine

python ingest.py \
  --pdf "질병보험_무배당 메리츠 실손의료비보험2605_30265.pdf" \
  --insurer 메리츠화재 \
  --product "메리츠 실손의료비보험2605" \
  --effective-date 2026-05-01
```

> 약관 PDF는 Notion "보험 약관 파일" DB(메리츠) 에서 받는다.

## 적재 검증

```sql
SELECT count(*), vector_dims(embedding::vector) FROM policy_chunks GROUP BY 2;
-- 행 수 > 0, 차원 = 1024 면 정상
```

## ⚠ 정합성

- 적재 임베딩 모델 == 질의(쿼리) 임베딩 모델 이어야 벡터공간이 일치한다.
  - 적재: Policy-Chunker `EMBED_MODEL`
  - 질의: AI 레포 `core.config.embedding_model`
  - 둘을 동일 모델·동일 1024d로 맞춘다.
