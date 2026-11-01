# tempVectorDB — RAG 실험용 로컬 pgvector

메리츠 약관 임베딩을 적재해 `rag.search()` / LangGraph 리포트 워커를 실험하기 위한 **임시** 로컬 DB.
운영 DB(AWS RDS)와 별개이며, 실험이 끝나면 폐기한다.

## 기동

```bash
docker compose -f tempVectorDB/docker-compose.yml up -d
# 접속: postgresql://postgres:postgres@localhost:5433/aiengine
```

- 포트 **5433** (메인 앱 5432와 충돌 회피)
- `init/*.sql` 은 **빈 볼륨 최초 1회만** 자동 실행 (policy/case/schedule_chunks·search_terms + 인덱스, 모두 `public` 스키마)
- 스키마 재적용하려면 볼륨 삭제 후 재기동: `docker compose -f tempVectorDB/docker-compose.yml down -v`

## 약관 임베딩 적재 (Policy-Chunker)

별도 레포 [Policy-Chunker(insurance-chunker)](https://github.com/SM17-YoonDongJu/Policy-Chunker/tree/insurance-chunker)에서 실행:

```bash
export EMBED_BACKEND=ollama            # ollama(qwen3-embedding) | sentence_transformers(BGE-M3)
export DATABASE_URL=postgresql://postgres:postgres@localhost:5433/aiengine

python ingest.py \
  --pdf "질병보험_무배당 메리츠 실손의료비보험2605_30265.pdf" \
  --insurer 메리츠화재 \
  --product "메리츠 실손의료비보험2605" \
  --effective-date 2026-05-01
```

> 약관 PDF는 Notion "보험 약관 파일" DB(메리츠) 에서 받는다.

Policy-Chunker에는 Voyage 백엔드가 없다. Voyage로 실험하려면 아무 백엔드로 적재한 다음, 아래 "모델 전환·재임베딩"으로 세 테이블을 함께 다시 임베딩한다.

## 판례·장해분류표 적재 (`load_cases.py` · `load_schedule.py`)

두 적재기는 공용 헬퍼 `doc_embedder.py`로 문서를 임베딩한다. 기본은 로컬 Ollama이고, `EMBED_PROVIDER=voyage`면 GPU 없이 Voyage로 적재한다.

| env | 기본 | 설명 |
|---|---|---|
| `EMBED_PROVIDER` | `ollama` | `ollama` \| `voyage` |
| `EMBED_MODEL` | ollama `qwen3-embedding:0.6b` / voyage `voyage-4-large` | 적재 모델 |
| `OLLAMA_URL` | `http://localhost:11434` | ollama `POST /api/embed` |
| `VOYAGE_API_KEY` | (없음 — voyage 필수) | 시크릿. 커밋·출력 금지 |
| `VOYAGE_BASE_URL` | `https://ai.mongodb.com/v1` | voyage `POST /embeddings` (`input_type=document`, `output_dimension=1024`) |
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5433/aiengine` | 적재 대상 DB |

```bash
# Ollama(기본)
uv run python tempVectorDB/load_schedule.py --manifest tempVectorDB/schedule_manifest.yaml

# Voyage — 키는 숨김 입력으로 받아 셸 기록에 남기지 않는다
read -rs VOYAGE_API_KEY && export VOYAGE_API_KEY
EMBED_PROVIDER=voyage uv run python tempVectorDB/load_schedule.py --manifest tempVectorDB/schedule_manifest.yaml
EMBED_PROVIDER=voyage uv run python tempVectorDB/load_cases.py
```

적재기는 테이블 하나만 채운다. 모델을 바꿨다면 나머지 테이블도 같은 모델로 맞춘다(아래).

## 모델 전환·재임베딩 (`python -m rag.reembed`)

이미 적재된 행의 `embedding`을 지금 임베딩 설정으로 다시 채운다. 세 테이블은 같은 쿼리 벡터로 검색되므로 **세 테이블 전부**를 같은 모델로 바꿔야 한다. 설정은 쿼리 쪽과 같은 `EMBEDDING_*` env를 쓴다(적재기의 `EMBED_*`와 이름이 다르다). 레포 루트에서 실행한다.

```bash
read -rs EMBEDDING_API_KEY && export EMBEDDING_API_KEY
export DATABASE_URL=postgresql://postgres:postgres@localhost:5433/aiengine \
       EMBEDDING_PROVIDER=voyage EMBEDDING_BASE_URL=https://ai.mongodb.com/v1 \
       EMBEDDING_MODEL=voyage-4-large EMBEDDING_DIM=1024

PYTHONPATH=src uv run python -m rag.reembed --schema public --dry-run   # 테이블별 대상 행 수
PYTHONPATH=src uv run python -m rag.reembed --schema public             # 세 테이블 전부
```

- 중단되면 `rag.reembed.aborted` 로그의 `resume_after`로 이어서 돌린다: `--tables <table> --after <resume_after>`.
- 레포 루트의 `.env`도 읽힌다(env 변수가 우선). `.env`에 `RDS_CA_PATH`가 있으면 로컬 DB에서 SSL을 시도하므로 비워 둔다.
- 운영 RDS(`corpus` 스키마)에서 돌리는 방법은 [src/rag/README](../src/rag/README.md)의 "코퍼스 재임베딩 CLI" 절에 있다.

## 적재 검증

```sql
SELECT count(*), vector_dims(embedding::vector) FROM policy_chunks GROUP BY 2;
-- 행 수 > 0, 차원 = 1024 면 정상 (case_chunks·schedule_chunks도 같다)
```

## ⚠ 정합성

- 적재 임베딩 모델 == 질의(쿼리) 임베딩 모델이어야 벡터공간이 일치한다. 둘 다 1024d라 다른 모델을 섞어도 오류가 나지 않고 검색 결과만 틀어진다.
  - 적재: Policy-Chunker `EMBED_BACKEND`·`EMBED_MODEL`, `load_*.py`의 `EMBED_PROVIDER`·`EMBED_MODEL`
  - 질의: AI 레포 `EMBEDDING_PROVIDER`·`EMBEDDING_MODEL`(core.config)
  - 쿼리 임베딩이 실패해도 다른 모델로 폴백하지 않는다(키워드 검색만으로 degrade).
- 모델을 바꾸는 순서: 세 테이블 재임베딩 → 그다음 쿼리 쪽 `EMBEDDING_PROVIDER` 전환.
