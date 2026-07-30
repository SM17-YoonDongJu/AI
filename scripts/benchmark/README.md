# 모델 선정 벤치마크 하네스

`report_worker`(LangGraph 리포트 파이프라인)를 대상으로 여러 후보 챗 LLM의 **생성 속도**와
**RAG 정확성(근거성)**을 동일 조건에서 비교한다. 공개 벤치마크(스크리닝)와 합쳐 모델을 선정한다.

- **production 코드 무수정**: `settings.llm_model` 런타임 교체 + `core.ai_client` 계측 래핑으로만 동작.
- **Judge 고정**: 출력 가드레일 LLM Judge를 고정 레퍼런스 모델로 라우팅 → 자기채점 편향 제거.
- **정직한 골드**: 결정론 라벨만 기본 제공(`cases.GOLD`), 특약·지급률 등 도메인 라벨은 직접 채움.

## 1. 사전 준비

```bash
# 의존성(리포트 extra 포함)
uv sync --extra report --extra embeddings        # 필요 시 --extra ner

# tempVectorDB 기동 + 약관 임베딩 적재 (RAG가 비면 측정 무의미)
docker compose -f tempVectorDB/docker-compose.yml up -d
# → Policy-Chunker로 policy_chunks 적재 (tempVectorDB/README.md 참고)

# Ollama에 후보/임베딩/judge 모델 pull
ollama pull gemma3:12b && ollama pull exaone3.5:7.8b && ollama pull qwen2.5:32b
ollama pull bge-m3        # 임베딩(적재 모델과 동일해야 함)
```

`.env` (또는 export): `DATABASE_URL`은 tempVectorDB(`...:5433/aiengine`), `AI_BASE_URL`/`EMBEDDING_BASE_URL`은
Ollama(`http://localhost:11434/v1`), `EMBEDDING_MODEL`은 **적재 시 쓴 모델과 동일**하게.

> g6e.2xlarge(L40S 48GB): 12~32B 스윗스팟. 32B는 Q8까지, 70B Q4는 컨텍스트 여유가 빠듯(리스크).

## 2. 실행

```bash
# 검색 골드 라벨링용 후보 뽑기(최초 1회) → results/retrieval_candidates.json 보고 cases.RETRIEVAL_GOLD 채움
PYTHONPATH=src:scripts python -m benchmark.run --discover-retrieval

# 본 벤치마크
PYTHONPATH=src:scripts python -m benchmark.run \
  --models gemma3:12b,exaone3.5:7.8b,qwen2.5:14b,qwen2.5:32b \
  --judge-model qwen2.5:32b \
  --repeats 3 \
  --out scripts/benchmark/results
```

산출물(`results/`): `summary.md`(비교표), `summary.csv`, `summary.json`, `raw_runs.jsonl`(원자료).

주요 플래그: `--no-quality`(품질 LLM 채점 생략), `--no-speed-probe`(tok/s 프로브 생략),
`--top-k`(검색 컷오프, 기본 8), `--skip-preflight`(policy_chunks 확인 생략).

## 3. 측정 지표

| 계층 | 지표 | 소스 |
|---|---|---|
| 생성 속도 | e2e p50/p90/p95, 호출 지연, decode/prefill tok/s | astream 타이밍 + speed_probe(usage) |
| RAG 검색(임베딩 축) | Recall@k·MRR·nDCG@k·Hit@1 | `retrieval.py`(rag.search 직접), 챗 LLM 무관 |
| RAG 근거성(챗 축) | verified율·judge_failures·JSON실패·금액단정치환 | 파이프라인 결정론 백스톱(final_state) |
| 분류/라우팅 정확 | 사고유형·장해라우팅·차단·약관없음·PII 마스킹 | `cases.GOLD` 라벨 대비 |
| 특약/지급률 정확 | applicable/missing F1, 지급률 범위 | **도메인 라벨 채우면 자동 채점** |
| 리포트 품질 | grounding·금액안전·완결성·유창성·부합(1~5) | 고정 judge LLM |

## 4. 골드 채우기(도메인 전문 판단 필요)

`scripts/benchmark/cases.py`:
- **검색**: `RETRIEVAL_GOLD[*].relevant_chunk_ids`에 정답 `source_ref` 채움(`--discover-retrieval` 결과 참고).
- **케이스**: `GOLD[label]`의 `applicable_coverages`·`missing_coverages`(집합), `disability_min/max_rate`(%)를 채움.
  결정론 라벨(차단·장해라우팅·타보험유형·약관없음·PII)은 기본 제공됨. **A/B 시나리오의 사고유형·장해검토는
  입력만으로 단정 불가**해 비워둠 — 필요 시 손해사정사가 채움.

## 5. 의사결정

1. **정확도 하드 게이트** 통과 모델만 후보(예: JSON실패≈0, 장해라우팅·차단 정확 高, verified율 ≥ 목표, PII_ok=1).
2. 통과분을 **e2e p90 지연**으로 비교 → Pareto 최전선에서 선택.
3. 양자화(Q4/Q8)는 명시 변수 — top 후보는 두 레벨 모두 `--models`에 넣어 비교.
4. **챗봇(12번)은 사용자 대면·비스트리밍**이라 지연 예산이 훨씬 빡셈 → 별도 결정(더 작은 모델이 이길 수 있음).

## 6. 공개 벤치마크(스크리닝 prior)

후보를 4~6개로 추리는 용도(최종 결정엔 미사용): KMMLU(법·경제 서브셋)·HAE-RAE·CLIcK(한국어 지식),
Ko-IFEval(지시 따르기), BFCL(구조화 출력·함수호출), LogicKor·Ko-MT-Bench(생성 품질), RAGTruth(RAG 충실도).
리더보드는 변하므로 선정 시점 수치를 다시 조회할 것.
