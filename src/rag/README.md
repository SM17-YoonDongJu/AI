# rag — Hybrid RAG 검색 파이프라인 (04)

`report_worker`가 **함수 호출**로 쓰는 공용 모듈이다. 독립 서비스가 아니다. 공개 진입점은 `search()` 하나이고, 결과 모델(`Chunk`·`Citation`·`RagResult`)은 `core.contracts`가 단일 출처다.

## 사용

```python
from rag import search

result = await search(
    "골절 진단비 지급 조건",
    namespaces=["terms", "case"],       # None이면 라우터가 결정
    top_k=8,
    insurer="OO생명", product="OO건강보험",  # terms 메타 필터(선택)
    contract_date=date(2021, 3, 1),      # level: 계약일이 속한 분류표 버전만
)
result.ranked_chunks  # list[Chunk] — text·namespace·score(RRF)·source_ref·article_number·product_name
result.citations      # list[Citation] — clause_no(조항/사례 번호)·exhibit(별표·분류표 버전)
```

## 파이프라인

| 단계 | 모듈 | 내용 |
|---|---|---|
| 1. 쿼리 라우터 | `router.py` | namespace 조합 결정. 비신체보험(자동차·화재 등)은 범위 밖이라 빈 결과 |
| 2. 오타 보정 | `typo.py` | kiwipiepy로 내용어 추출 → `search_terms`의 pg_trgm에서 `similarity > 0.4`인 정규 용어로 치환 |
| 3. 병렬 검색 | `search.py` | tsvector 키워드 검색과 pgvector 벡터 검색을 namespace별로 동시에 실행 |
| 4. RRF 통합 | `fusion.py` | `score = Σ weight / (60 + rank)`, 키워드:벡터 = 0.5:0.5 (순수 함수) |
| 5. 인용 역추적 | `search.py` | 상위 청크의 메타데이터로 `Citation` 생성 |

## Namespace

| namespace | 테이블 | 내용 | 비고 |
|---|---|---|---|
| `terms` | `policy_chunks` | 신체보험 약관 | 기본 대상. `insurer`/`product` 필터 적용 |
| `case` | `case_chunks` | 분쟁조정 사례 | 기본 대상 |
| `level` | `schedule_chunks` | 후유장해 분류표 | 명시할 때만 검색. `contract_date`로 버전 매칭(없으면 현행판) |

`medical`(수가·KCD)은 테이블이 아직 없어 향후 확장 대상이다. 테이블 스키마는 [`migrations/corpus/`](../../migrations/corpus)에 있다.

## 임베딩과 장애 시 동작

- 쿼리 임베딩은 `core.ai_client.embed(text, input_type="query")`이다(1024d). 업체는 env `EMBEDDING_PROVIDER`로 고른다.
  - `openai_compat`(기본, Ollama qwen3-embedding): 쿼리에 instruction 프리픽스(`_QUERY_INSTRUCT`)를 붙인다. 문서는 원문으로 색인한다.
  - `voyage`(voyage-4-large): 프리픽스 없이 원문을 보낸다. Voyage가 `input_type=query`에 검색용 프롬프트를 직접 붙이기 때문이다.
- 임베딩이 실패하면(`AiClientError`, 차원 불일치 포함) **키워드 검색만으로 degrade**한다. 경고 로그 `rag.embed.failed`에는 예외 타입만 남기고, 완료 로그(`rag.search.completed`)에 `degraded=True`를 남긴다. 검색 자체는 실패시키지 않는다.
- **다른 모델로 폴백하지 않는다.** 색인 모델과 다른 모델로 쿼리를 임베딩하면 둘 다 1024d라 차원 검사는 통과하지만 벡터 공간이 달라 결과가 조용히 틀어진다. 그래서 예전의 BGE-M3 폴백을 없앴다.

## 임베딩 모델 전환 (⚠ 순서)

코퍼스 세 테이블(`policy_chunks`·`case_chunks`·`schedule_chunks`)은 같은 쿼리 벡터 하나로 검색된다. 그래서 **문서 임베딩 모델과 쿼리 임베딩 모델이 같아야** 한다. 모델을 바꿀 때(예: qwen3-embedding → voyage-4-large)는 아래 순서를 지킨다. **섞이면 오류 없이 검색 결과만 틀어진다.**

1. 적재를 멈추거나 적재기도 새 모델로 맞춘다. 약관은 Policy-Chunker, 판례·장해분류표는 `tempVectorDB/load_*.py`가 적재한다(`EMBED_PROVIDER=voyage` 지원).
2. `python -m rag.reembed`로 **세 테이블 전부**를 새 모델로 재임베딩한다(아래).
3. 그다음에 쿼리 쪽(report 워커)의 `EMBEDDING_PROVIDER`·`EMBEDDING_BASE_URL`·`EMBEDDING_MODEL`·`EMBEDDING_API_KEY`를 바꾼다.

재임베딩하는 동안에는 테이블 안에 두 모델의 벡터가 섞여 있다. 그래서 리포트 트래픽이 적을 때 돌리고, 끝나면 바로 3번으로 넘어간다.

## 코퍼스 재임베딩 CLI (`python -m rag.reembed`)

적재된 청크의 `embedding`을 **지금 env의 임베딩 설정**으로 다시 채운다. 문서는 `content` 원문을 그대로 `input_type="document"`로 임베딩한다. chunk_id 순으로 페이지(기본 32행)를 읽고, 페이지마다 트랜잭션 하나로 UPDATE한다.

| 인자 | 기본 | 설명 |
|---|---|---|
| `--schema` | `corpus` | 운영 RDS는 `corpus`, tempVectorDB는 `public` |
| `--tables` | 세 테이블 전부 | `policy_chunks`·`case_chunks`·`schedule_chunks` 중에서만 고를 수 있다 |
| `--batch-size` | `32` | 한 트랜잭션의 행 수. 요청당 토큰 한도 오류(400)가 나면 줄여서 재개한다 |
| `--after` | 없음 | 이 chunk_id 다음부터 재개한다. `--tables`에 테이블을 하나만 줄 때만 쓸 수 있다 |
| `--dry-run` | — | 테이블별 대상 행 수만 센다(임베딩 호출·쓰기 없음) |

- **DB 계정**: 운영 RDS의 corpus 테이블은 `corpus_owner` 소유다. report 워커 계정(`ai_owner`)은 SELECT만 있어 UPDATE가 `InsufficientPrivilegeError`로 막힌다. `DATABASE_URL`은 corpus_owner 접속 URL로 준다.
- **로그**: 시작할 때 `rag.reembed.started`에 `embedding_provider`·`embedding_model`·`embedding_dim`을 남긴다. 배치를 커밋할 때마다 `rag.reembed.batch_committed`(table·updated·last_chunk_id)를 남긴다. 본문은 남기지 않는다.
- **중단·재개**: 임베딩·DB 오류가 나면 직전 배치까지 커밋된 상태로 멈추고 종료 코드 1을 낸다. `rag.reembed.aborted` 로그에 `table`·`resume_after`·`remaining_tables`·`error_type`·`http_status`가 남는다. 같은 모델로 다시 임베딩해도 결과가 같아서, 겹쳐 돌리거나 처음부터 다시 돌려도 안전하다(비용만 다시 든다).
  ```bash
  python -m rag.reembed --tables case_chunks --after '<resume_after>'   # 멈춘 테이블 이어서
  python -m rag.reembed --tables schedule_chunks                         # remaining_tables
  # resume_after가 null이면 --after 없이 그 테이블부터 다시 돌린다.
  ```
- content가 빈 행은 임베딩하지 않고 `embedding`을 NULL로 비운다. 이전 모델의 벡터가 남지 않게 하기 위해서다. 비운 수는 로그의 `nulled`에 남는다.

### 클러스터(report 이미지)에서 실행 — `kubectl exec`

report 이미지는 `WORKDIR=/app`·`PYTHONPATH=/app/src`이고 `PATH`에 `/app/.venv/bin`이 들어 있다. 그래서 파드 안에서 `python -m rag.reembed`가 바로 돈다. `RDS_CA_PATH`(RDS TLS CA)는 파드 env에 이미 있다.

이 CLI가 들어간 이미지가 배포된 뒤에만 쓸 수 있다. `src/rag` 변경이 머지되면 report가 재배포되고, 기본 env(`openai_compat`)에서는 런타임 동작이 그대로다. 먼저 파드에 모듈이 있는지 확인한다.

```bash
kubectl -n default exec deploy/report -c report -- python -m rag.reembed --help > /dev/null && echo ok
```

```bash
# (로컬) corpus_owner 접속 URL을 화면에 띄우지 않고 클립보드로 복사한다(macOS pbcopy).
kubectl -n default get secret corpus-worker-secrets -o jsonpath='{.data.DATABASE_URL}' | base64 -d | pbcopy

# report 파드에 들어간다(Deployment report, 컨테이너 report).
kubectl -n default exec -it deploy/report -c report -- bash
```

```bash
# (파드 안) 시크릿은 숨김 입력으로 받아 화면·셸 기록에 남기지 않는다.
# 이 export는 이 셸에만 적용되고, 실행 중인 워커(PID 1)의 env는 바뀌지 않는다.
read -rsp 'corpus_owner DATABASE_URL: ' DATABASE_URL; echo; export DATABASE_URL
read -rsp 'Voyage(Atlas) API key: ' EMBEDDING_API_KEY; echo; export EMBEDDING_API_KEY
export EMBEDDING_PROVIDER=voyage EMBEDDING_BASE_URL=https://ai.mongodb.com/v1 \
       EMBEDDING_MODEL=voyage-4-large EMBEDDING_DIM=1024

python -m rag.reembed --dry-run      # 시작 로그의 provider·model과 테이블별 행 수를 확인한다
python -m rag.reembed                # corpus 세 테이블 전부
```

- 파드가 재시작되면(이미지 배포 sync, Reloader의 Secret 변경 재시작) 실행 중인 재임베딩도 죽는다. 그때는 터미널에 남은 마지막 `last_chunk_id`로 재개한다. 실행하는 동안에는 report 배포를 미룬다.
- 실행 중인 워커와 파드의 메모리 한도(768Mi)를 나눠 쓴다.
- 끝난 뒤(선택, corpus_owner로): 대량 UPDATE로 생긴 dead tuple과 HNSW 그래프를 정리한다. pgvector는 HNSW를 REINDEX한 다음 VACUUM하라고 권장한다.
  ```sql
  REINDEX INDEX CONCURRENTLY corpus.idx_policy_hnsw;
  REINDEX INDEX CONCURRENTLY corpus.idx_case_hnsw;
  REINDEX INDEX CONCURRENTLY corpus.idx_schedule_hnsw;
  VACUUM (ANALYZE) corpus.policy_chunks;
  VACUUM (ANALYZE) corpus.case_chunks;
  VACUUM (ANALYZE) corpus.schedule_chunks;
  ```

로컬 tempVectorDB(`public` 스키마)에서 돌리는 방법은 [tempVectorDB/README](../../tempVectorDB/README.md)에 있다.

## 의존

- `core.db`(asyncpg + pgvector·tsvector·pg_trgm), `core.ai_client`, `core.contracts`
- `kiwipiepy`(base)
- 이 모듈을 바꾸면 report 서비스가 재배포된다(CI paths-filter).

## 참고

- [Notion 04 Hybrid RAG](../../.claude/docs/04_hybridRag.md) · [아키텍처: rag](../../docs/architecture/rag.md) · [컨벤션](../../.claude/CODE_CONVENTIONS.md)
- 점검 스크립트: `scripts/rag_check.py`
