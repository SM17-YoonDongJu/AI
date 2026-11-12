# 성능 실험 — 계측 → 기준선 → SLO → 순차 개선

리포트 워커(05)와 OCR 워커(02)의 처리 시간을 **실험 형태**로 줄인다. 먼저 재고(기준선), 그 분포로
목표(SLO)를 정하고, 개선안을 한 번에 하나씩 적용·측정해 SLO에 닿을 때까지 반복한다.

| 항목 | 결정 (2026-11-12) |
|---|---|
| 기준선·SLO·최종 확인 | **dev 실환경**(t3.large OCR·클러스터 vLLM·Claude·공용 RDS) |
| 개선안 A/B 반복 | **로컬 벤치**(같은 입력 고정) → 채택안만 dev 재검증 |
| 순서 | **리포트 먼저**(기존 `node_trace` 데이터로 바로 기준선), 그다음 OCR |
| 리포트 반복 규모 | 조건당 4시나리오 × 5회 = 20건 |
| **Claude 호출 실험** | **보류** — 비용 때문에 사용자 요청으로 멈춤. 재개 전 반드시 확인받는다 |
| 진행 순서 변경 | dev `ai.report_drafts` 0건·prod엔 PR #92 미반영이라 리포트 기준선이 없다 → **OCR 먼저** 기준선→SLO→개선 |
| OCR 기준선 환경 | dev 클러스터 워커(ip-10-0-137-77, **t3.large** — prod OCR 호스트와 같은 타입)에 벤치 파드. 큐·DB·Claude 미경유 |

---

## 0. 실험 규칙

1. **한 실험 = 한 변경.** 두 개를 섞으면 어느 쪽 효과인지 모른다.
2. **같은 입력으로 비교한다.** 기준선과 개선안은 같은 샘플·시나리오·반복 수로 돈다.
3. **품질 가드레일은 회귀하면 안 된다**(§4). 빨라져도 품질이 떨어지면 기각한다.
4. **비용이 드는 실험(Claude·Voyage 호출)은 실행 전 승인**받는다.
5. 결과는 §6 실험 기록에 남긴다(채택·기각 모두 — 기각 사유가 다음 실험의 근거다).

## 1. 하드 제약

| 제약 | 값 | 넘으면 |
|---|---|---|
| SQS 가시성 | `sqs_visibility_timeout=600`초, **처리 중 연장(heartbeat) 없음** | 처리 중 재전달 → 같은 작업을 두 번 처리(리포트는 Claude 비용도 두 번) |
| OCR 기동 예산 검사 | VLM 300초 + 비VLM 240초 + 여유 60초 ≤ 600초 (`ocr_worker/__main__.py`) | 기동 실패 — 단, 비VLM 240초는 **아직 실측 전**(`config.py` 주석) |
| 리포트 기동 예산 검사 | **없음** | Claude 호출 1회 타임아웃이 600초(`LLM_TIMEOUT_SECONDS`)라 한 번만 느려도 가시성 초과 가능 |

→ 어떤 SLO든 **p99 < 600초**는 최소 조건이다. 여유를 두려면 p99 ≤ 480초(80%)를 안전선으로 본다.

## 2. SLI와 계측 이벤트

이 브랜치(`perf/slo-baseline`)가 넣은 이벤트. 모두 JSON 로그 1줄이고 **식별자·분류값·수치만** 싣는다(§9 PII 경계).

| SLI | 이벤트 | 주요 필드 | 위치 |
|---|---|---|---|
| 워커 처리 시간(메시지당) | `sqs.message.finished` | `handle_ms`, `queue_wait_ms`, `outcome`(done·retry·terminal), `receive_count`, `visibility_timeout_ms` | `core/sqs/consumer.py` |
| 리포트 그래프 총·노드별 시간 | `report.graph.finished` | `total_ms`, `node_ms.<노드>`, `failed_nodes`, `llm_failure` | `report_worker/worker.py` |
| LLM 호출별 시간·토큰 | `ai_client.chat.finished` | `duration_ms`, `input_tokens`, `output_tokens`, `cache_*_input_tokens`, `stop_reason`, `effort`, `graph_node` | `core/ai_client.py` |
| 임베딩 호출 시간 | `ai_client.embed.finished` | `duration_ms`, `texts`, `input_type`, `graph_node` | `core/ai_client.py` |
| OCR 문서 1건 총 시간 | `ocr.job.finished` | `duration_ms`, `status`, `pages`, `lines`, `doc_type`, `needs_vlm`, `vlm_adopted`, `ocr_quality` | `ocr_worker/pipeline.py` |
| OCR 단계별 시간 | `ocr.stage.finished` | `stage`, `duration_ms`, `status` (+ 단계별 `pages`·`lines`·`bytes`) | `ocr_worker/ocr.py`, `pipeline.py` |

OCR 단계(`stage`) — 실행 순서:

| stage | 내용 | 비고 |
|---|---|---|
| `s3_fetch` | 원본 다운로드 | `bytes` |
| `render` | PDF/이미지 → 페이지 이미지 | `pages` |
| `ocr_engine` | PaddleOCR 인식 | `pages`, `lines` |
| `analyze` | 분류·추출·텍스트 마스킹(정규식+NER)·`masked_lines` | NER이 여기서 2회 돈다 |
| `vlm_pages` | VLM 쪽 전사(표 문서·저신뢰도만) | `adopted` |
| `quality_check` | 재확인 판정용 이름 검출(NER) | 이벤트 루프에서 동기 실행 |
| `image_pipeline` | 아래 5단계의 합 | |
| ├ `vlm_reread` | 저신뢰도 라인 VLM 재판독 | `ran` |
| ├ `redact` | 검은블록 렌더(검출 1회 더 — NER) | |
| ├ `encode_png` | PNG 인코딩 | |
| ├ `upload` | 마스킹 사본 S3 업로드 | `bytes` |
| └ `verify` | 삭제 게이트 5.1 커버리지 + 5.2 **재OCR** | `delete_original` |
| `db_save` | `ocr_results` 저장 + 청구 종결 카운트(1 트랜잭션) | |
| `publish` | report-job 발행 또는 청구 fan-in 판정 | |

## 3. 기준선 수집

### 3.1 리포트 — 지금 바로 가능 (코드 변경 없음)

PR #92부터 `ai.report_drafts.draft->'metrics'->'pipeline'`에 노드별 ms가 쌓여 있다.

```sql
-- 총 소요 시간 분포
SELECT count(*) AS n,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY (draft->'metrics'->'pipeline'->>'total_ms')::int) AS p50_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY (draft->'metrics'->'pipeline'->>'total_ms')::int) AS p95_ms,
       max((draft->'metrics'->'pipeline'->>'total_ms')::int) AS max_ms
FROM ai.report_drafts
WHERE draft->'metrics'->'pipeline' ? 'total_ms';

-- 노드별 분포 — 어디서 시간이 나가는지
SELECT node->>'node' AS node, count(*) AS n,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY (node->>'ms')::int) AS p50_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY (node->>'ms')::int) AS p95_ms
FROM ai.report_drafts,
     jsonb_array_elements(draft->'metrics'->'pipeline'->'nodes') AS node
GROUP BY 1 ORDER BY p95_ms DESC;
```

한계: **저장된 리포트만** 있다. 실패·거절·재전달된 리포트의 시간이 빠져 분포가 낙관적으로 나온다.
그래서 배포 후 §3.2의 로그 기준선으로 보정한다.

### 3.2 배포 후 — 로그 이벤트

워커 로그(JSON)를 받아 집계한다. 수집 경로와 무관하게 쓸 수 있다.

dev 워커 로그는 JSON 한 줄씩 Loki로 수집된다(`ENV=dev`, alloy). 클러스터 접근은 SSM 포트포워딩
(control-plane `:6443`) + `~/.kube/k3s-config`, AWS CLI는 `--profile brbs`.

```bash
# dev: default 네임스페이스의 Deployment ocr-worker · report
kubectl logs deploy/ocr-worker --since=72h > ocr.log
kubectl logs deploy/report     --since=72h > report.log
uv run python scripts/perf/summarize_events.py report.log ocr.log --json baseline.json
```

prod OCR(별도 EC2, docker compose)처럼 로그가 CloudWatch Logs에 있으면 Logs Insights로 본다:

```
fields @timestamp
| filter event_type = "ocr.stage.finished"
| stats count() as n, pct(duration_ms, 50) as p50, pct(duration_ms, 95) as p95 by stage
```

```
fields @timestamp
| filter event_type = "ai_client.chat.finished"
| stats count() as n, pct(duration_ms, 50) as p50, pct(duration_ms, 95) as p95,
        avg(output_tokens) as out_tok by graph_node
```

```
fields @timestamp
| filter event_type = "sqs.message.finished"
| stats count() as n, pct(handle_ms, 95) as p95, pct(handle_ms, 99) as p99,
        max(handle_ms) as max by queue, outcome
```

기준선 수집 범위: dev에서 **최소 리포트 20건·OCR 문서 30건**(문서 유형·쪽 수가 섞이게). 단, dev는
AI 트래픽이 거의 없고(2026-11-12 기준 `ocr_results` 4건), 샘플을 dev OCR 큐에 넣으면 리포트가 자동
생성돼 Claude 과금이 생긴다 — 그래서 OCR은 §3.3 벤치로 잰다.

### 3.3 OCR 기준선 — dev t3.large 벤치 (B0, 2026-11-12)

조건: `scripts/perf/ocr_bench.py --reps 3 --no-vlm`, 고정 샘플 8종(모두 1쪽) × 3회 = 24건, 운영 OCR
이미지(`brbs/ocr:a7dc4d1`) + 이 브랜치 코드, CPU 2 vCPU·`OCR_CPU_THREADS=2`·`USE_NER=true`, 메모리
상한 4608Mi(운영과 같음), 워밍업 1건 제외. **VLM 제외** — 표·저신뢰 문서는 운영에서 여기에 VLM
시간(쪽 전사 ≤ 210초·재판독 ≤ 90초 예산)이 더해진다. 반복 간 편차 평균 3.9%(최대 8.4%) — 10% 이상
차이는 실험 효과로 본다.

| 지표 | p50 | p95 | max |
|---|---|---|---|
| **문서 1건(=1쪽) 총 시간** `ocr.job` | **75.1초** | **150.8초** | 153.0초 |
| `ocr_engine` (PaddleOCR) | 35.0초 | 55.4초 | 56.7초 |
| `verify` (5.1 커버리지 + **5.2 재OCR**) | 33.7초 | 53.4초 | 58.0초 |
| `analyze` (분류·추출·마스킹·NER) | 3.0초 | 15.3초 | 16.5초 |
| `quality_check` (NER) | 1.5초 | 7.4초 | 8.9초 |
| `redact` (검출 NER + 렌더) | 1.5초 | 7.7초 | 7.9초 |
| `vlm_pages` (VLM 꺼짐 → 쪽별 재마스킹 폴백, NER) | 2.3초 | 8.0초 | 8.7초 |
| 그 밖(render·encode·fetch 등) | < 1초 | 3.6초 | |
| **컨테이너 메모리 최고치** | | | **4,530MiB (상한 4,608MiB의 98%)** |

문서별(p50) — 시간이 라인 수에 비례하고, 재OCR 검증이 OCR 엔진과 거의 같은 시간을 쓴다:

| 문서 | 라인 | 총 | 엔진 | 재OCR 검증 | NER 합 |
|---|---|---|---|---|---|
| 무료진료비계산서 영수증 | 453 | 151.0 | 55.5 | 53.4 | 38.3 |
| 외래진료비계산서 영수증 | 183 | 109.1 | 44.4 | 45.1 | 16.6 |
| 예스폼 외래진료비계산서 | 136 | 96.4 | 42.9 | 42.2 | 10.7 |
| 예스폼 병원진단서소견서 | 39 | 76.0 | 35.9 | 35.5 | 3.1 |
| 실손 지급결과통보서 | 87 | 72.4 | 31.6 | 31.6 | 8.3 |
| 보험금 지급결과 안내문 | 69 | 65.5 | 30.5 | 29.8 | 5.2 |
| 예스폼 입원확인서 | 58 | 61.6 | 29.6 | 27.1 | 4.9 |
| 예스폼 병원진단서(가릴 PII 없음 → 검증 생략) | 6 | 18.8 | 10.7 | 0.0 | 2.3 |
| **합(비중)** | | 650.7 | **281.2 (43%)** | **264.7 (41%)** | **89.4 (14%)** |

읽은 것:
1. **재OCR 검증(5.2)이 문서 시간의 41%** — 원본 삭제 여부만 정하는 단계가 OCR을 한 번 더 돈다.
2. **NER은 14%지만 라인 수에 비례**해 빽빽한 문서에선 25%(453라인 38초). 같은 줄을 4번 돈다(O1).
3. 1쪽 문서가 비VLM만으로 최대 151초다. 기동 예산이 잡은 **비VLM 240초는 빽빽한 2쪽 문서에서 넘는다**
   — VLM 300초를 더하면 가시성 600초를 넘어 처리 중 재전달(중복 처리)이 난다.
4. **메모리 최고치가 상한의 98%** — 운영(prod `mem_limit: 4608m`·dev limit 4608Mi)도 같은 상한이라
   OOMKilled 직전이다. 문서마다 `malloc_trim`을 빼면 22분 만에 실제로 OOMKilled됐다(벤치 1차 시도).

### 3.4 리포트 기준선 — 합성 시나리오 벤치 (R0, 2026-11-12)

조건: `scripts/perf/report_bench.py`, 시나리오 A~D × 5회 = 20건, Opus 5.5 effort high(dev 설정과 같음), dev RDS
읽기 전용(SSM 터널), Voyage 임베딩, **사용자 개인 Claude 키**(dev→Claude 차단 상태). 저장 노드는 대역.
제외 1건: rep2 C — Claude 스트림이 200 OK 뒤 30분 넘게 끝나지 않아 중단(§5 R5). 제 쿼리(테이블 순차 스캔)와
시간이 겹친 rep1 A·B는 표시만 하고 포함(값이 다른 실행과 같은 범위).

| 지표 | 값 |
|---|---|
| **리포트 1건 벽시계** | **p50 146초 / p95 228초 / max 238초** |
| 비용 | 1건 평균 **$0.29** (출력 토큰이 대부분) — 20건 $5.79 |
| 시간 구성 | LLM 71% · **노드 밖 약관 존재 조회(`policy_in_db`) 17%** · RAG 등 비-LLM 노드 12% |

| 노드 | p50 | p95 | 메모 |
|---|---|---|---|
| `report_compose` | 53.5초 | 71.2초 | LLM 2회 순차(본문 → 쟁점). 짧은 쪽 p50 16.2초 |
| `coverage_analysis` | 36.7초 | 59.9초 | LLM 28초 + RAG |
| `disability_rag` | 25.8초 | 42.0초 | |
| `disability_verify` | 17.0초 | 21.5초 | |
| `output_guardrail` | 9.9초 | 16.1초 | Judge |
| `diagnosis` | 4.6초 | 7.0초 | |
| `case_search` | 0.5초 | 0.8초 | LLM 없음 — R3(병렬화)는 이득이 거의 없다 |
| **노드 밖(약관 존재 조회)** | **33.4초** | **44.7초** | 약관 없는 C만 0.2초. 같은 상품을 다시 조회해도 콜드(14GB 테이블 > 2GB 메모리) |

시나리오별 p50: A 209초 · B 190초 · C 99초 · D 130초. 가시성 600초 안에는 넉넉히 든다(p95 228초).

## 4. SLO

### OCR — 확정 (2026-11-12)

벤치(§3.3, t3.large·VLM 제외·고정 샘플) 기준. 목표의 근거는 가시성 600초 = VLM 예산 300초 + 비VLM
240초 + 여유 60초(기동 예산 검사)다. 빽빽한 **3쪽** 문서도 비VLM 240초 안에 들려면 쪽당 p95 ≤ 80초가
필요하다.

| SLI | 기준선(B0) | 목표 SLO | 근거 |
|---|---|---|---|
| 쪽당 비VLM 처리 시간 p95 (`ocr.job` / pages) | 150.8초 | **≤ 80초** | 3쪽 × 80 = 240초 예산 |
| 쪽당 비VLM 처리 시간 p50 | 75.1초 | **≤ 40초** | |
| 컨테이너 메모리 최고치 | 4,530MiB (98%) | **≤ 3,900MiB (85%)** | OOMKilled 여유 |
| 마스킹 결과 | — | **고정 샘플 지문 동일** | 품질 가드레일 |

### 리포트 — 기준선(R0) 확보, SLO는 G3 재측정 후 제안

| SLI | 기준선(R0) | 목표 | 메모 |
|---|---|---|---|
| 리포트 1건 벽시계 p50 / p95 | 146초 / 228초 | TBD | G3(약관 조회 −33초)·R1(−16초) 효과를 본 뒤 정한다 |

**품질 가드레일(비회귀 조건)** — 개선안이 아래를 기준선보다 나쁘게 만들면 기각한다.

| 대상 | 지표 | 출처 |
|---|---|---|
| 리포트 | 신뢰도 등급 분포(HIGH·MEDIUM·LOW) | `draft.metrics.confidence.level` |
| 리포트 | Judge 미확인 주장 수·검증 범위 | `draft.metrics.verification` |
| 리포트 | 그래프 실패율(`failed_nodes` 비율) | `report.graph.finished` |
| OCR | 마스킹 결과 **동일**(텍스트·`masked_lines`·검은블록) | 고정 샘플 diff |
| OCR | 잔류 PII 경고 0건, `needs_reupload` 비율 | 로그·`ocr_results.ocr_quality` |

## 5. 실험 백로그

**OCR 순서(B0 기준, 비중 큰 순)**: O3(재OCR 검증 41%) → O1(NER 14%, 빽빽한 문서 25%) → M1(메모리).
리포트(R*)는 보류.

**O3 방식 결정(2026-11-12)**: 텍스트 기반 잔류 검사 — 원본 OCR 라인 중 가려지지 않은 라인에 같은 잔류
패턴(`find_residual_pii`)을 돌린다(OCR 0회). 5.1 커버리지 검사는 그대로 둔다. **섀도 먼저**: 기존 재OCR이
계속 판정하고 텍스트 검사는 나란히 돌려 삭제 판정 일치율을 남긴 뒤, 일치가 확인되면 전환한다.
(검증을 ack 뒤로 미루는 안은 CPU 작업량이 같아 청구 완료 시간이 거의 줄지 않아 기각.)

| ID | 가설 | 변경 | 측정 | Claude 비용 |
|---|---|---|---|---|
| R1 | 리포트 본문·쟁점 추출은 서로 의존하지 않아 병렬화하면 `report_compose`가 짧은 쪽 호출만큼 준다 | `agents.py` 본문(`chat`)∥쟁점(`chat_json`) `gather` | `node_ms.report_compose` | 실험 실행 시 발생(호출 수는 동일) |
| R2 | JSON 추출성 호출(쟁점·판례 필터·진단)은 effort를 낮춰도 품질이 유지된다 | 노드별 effort 차등 | `llm.chat` 노드별 `duration_ms`·`output_tokens` + 품질 가드 | 실험 실행 시 발생 |
| R3 | `case_search`는 진단에만 의존해 coverage 경로와 병렬화할 수 있다 | 그래프 fan-out | `total_ms` | **R0에서 `case_search` p50 0.5초 — 이득이 거의 없어 보류** |
| R4 | (보호) 리포트도 처리 시간 상한이 가시성 안에 들어야 한다 | 기동 예산 검사 또는 가시성 연장(heartbeat) | p99 vs 600초 | 없음 |
| **R5** | (보호) Claude 스트림이 200 OK 뒤 끝나지 않고 멈출 수 있다(R0 중 1건, 30분+). SDK 타임아웃은 청크 간격이라 못 끊는다 → 워커가 무한정 묶이고 가시성 초과 시 중복 처리 | LLM 호출 단위 전체 상한(`asyncio.timeout`) → `AiClientError`로 재전달 | 멈춘 호출 수·`ai_client.chat.finished` 누락 | 없음(재현은 어렵다) |
| **G3** | 약관 존재 조회가 리포트의 17%(p50 33초)다 — `idx_policy_meta`가 상품명을 못 써 보험사 청크 수십만 행을 디스크에서 읽는다 | `EXISTS` + `(insurer, product_name)` 인덱스(#96) | `outside_nodes_ms`·`coverage_analysis` | 재측정 4건 |
| **O3** | 5.2 재OCR 검증이 OCR 엔진 비용을 한 번 더 쓴다(**B0: 41%**) | 방식 선택 필요 — (a) 검증을 처리 경로 밖(ack 뒤)으로 (b) 텍스트 기반 잔류 검사로 대체 (c) 재OCR을 가려지지 않은 라인 인식만으로 축소 | `verify`·`ocr.job` + 삭제 판정 일치 | 없음 |
| **O1** | NER이 같은 줄을 문서당 4번 돌고 배치도 안 된다(**B0: 14%, 빽빽한 문서 25%**) | 줄 단위 결과 재사용 + `batch_size` | `analyze`·`quality_check`·`redact`·`vlm_pages` + 지문 동일 | 없음 |
| O2 | `quality_check` NER이 이벤트 루프를 막는다 | O1에 포함(결과 재사용) | `quality_check` | 없음 |
| **M1** | 메모리 최고치가 상한의 98%다(B0) — 재OCR·NER 모델이 OCR 최고점 위에 얹힌다 | O3·O1 이후 재측정, 필요하면 검출 상한·모델 적재 조정 | `memory.peak` | 없음 |
| G1 | 오타보정이 trigram 인덱스를 못 타고 토큰마다 DB를 순차 왕복한다 | `%` 연산자 + `unnest` 일괄 조회 | RAG 노드 ms | 없음 |
| G2 | prod HNSW(4.4GB)가 RDS 메모리(2GB)보다 커 벡터 검색이 디스크를 읽는다(파라미터 그룹은 11-12 `default.postgres16`으로 해소) | 인스턴스 클래스·인덱스 크기(halfvec·부분 인덱스) 검토(인프라) | RAG 노드 ms·`EXPLAIN (ANALYZE, BUFFERS)` | 없음 |

## 6. 실험 기록

템플릿:

```
### <ID> <제목> — <날짜>
- 가설:
- 변경: (커밋/PR)
- 조건: 환경(로컬/dev) · 입력(샘플·시나리오) · 반복 수
- 결과: 기준선 p50/p95 → 개선안 p50/p95 (Δ%)
- 품질 가드레일: 통과/위반(내용)
- 결정: 채택/기각 · 사유
```

### B0 OCR 기준선 — 2026-11-12
- 조건: dev t3.large 벤치 파드, 고정 샘플 8종 × 3회, VLM 제외, 운영 메모리 상한(§3.3)
- 결과: 쪽당 p50 75.1초 / p95 150.8초, 메모리 최고치 4,530MiB(98%)
- 비중: OCR 엔진 43% · 재OCR 검증 41% · NER 14%
- 원자료: `out/perf-dev/out/baseline-t3large/`(로컬 보관, 커밋 안 함 — 지문·요약·메타·이벤트)
- 참고(로컬 맥 M5 Pro·2스레드·1회): 쪽당 p50 43.3초 / p95 91.0초 — 비중은 같고 절대값만 빠르다

### O3 텍스트 잔류 검사 — 2026-11-12 (브랜치 `perf/o3-text-verify`)
- 가설: 검은블럭이 라인 단위로 통째 칠해지므로 재OCR(5.2)은 가려지지 않은 원본 라인의 텍스트를 다시 읽는
  것과 같다 → 텍스트 검사로 대신하면 `verify`(41%)가 사라지고 삭제 판정은 그대로다
- 변경: `text_residual_pii` + `ocr_verify_mode`(reocr·text·shadow, 기본 shadow) — 벤치는 `OCR_VERIFY_MODE=text`
- 조건: B0와 같음(dev t3.large 벤치 파드, 8종 × 3회, VLM 제외, 메모리 상한 4608Mi)

| 지표 | B0 | O3 | 변화 |
|---|---|---|---|
| 쪽당 p50 | 75.1초 | **41.9초** | **−44%** |
| 쪽당 p95 | 150.8초 | **97.6초** | **−35%** |
| `verify` p50 / p95 | 33.7초 / 53.4초 | 0.01초 / 0.02초 | |
| 그 밖의 단계(엔진·NER) | | | ±5%(반복 편차 안) |
| 메모리 최고치 | 4,530MiB | 4,558MiB | 변화 없음 — 최고점은 OCR 엔진 검출 |

- 품질 가드레일: **고정 샘플 8종 결과 지문 전부 동일**(마스킹 텍스트·라인·엔티티·검은블록 PNG·원본 삭제 판정)
- 결정: **채택(섀도부터)** — 운영 기본값은 `shadow`(재OCR이 판정, 텍스트 판정은 `ocr.verify.shadow`로 일치
  여부만 남김). dev·prod 실문서에서 불일치 0건이 쌓이면 `OCR_VERIFY_MODE=text`로 전환한다
- SLO 대비: p50 41.9초(목표 ≤ 40), p95 97.6초(목표 ≤ 80) — 남은 차이는 NER(빽빽한 문서 38초)이라 O1로 간다

## 7. 로컬 벤치 환경 메모

- **리포트**: `run_experiments.py`의 4시나리오를 쓴다. 다만 `tempVectorDB/init/02_app_tables.sql`이
  7월 기준이라 dev 스키마와 다르다(`user_claims.details` 등 누락) — 스키마를 맞추고 코퍼스를 Voyage로
  적재해야 돈다. Claude·Voyage 키가 필요하다(**Claude 실험 보류 중**).
- **OCR(dev t3.large)**: 운영 OCR 이미지로 `ocr-bench` 파드(nodeName ip-10-0-137-77, limit 4608Mi,
  `sleep`)를 띄우고 `src`·`scripts/perf`·`samples`를 `COPYFILE_DISABLE=1 tar`로 묶어 `kubectl cp` →
  파드 안에서 `nohup python -u scripts/perf/ocr_bench.py ... &`로 실행한다(SSM 터널이 끊겨도 측정은
  계속된다). macOS `tar`는 기본으로 `._*` AppleDouble 파일을 넣으니 반드시 `COPYFILE_DISABLE=1`.
  끝나면 결과를 `kubectl cp`로 가져오고 파드를 지운다.
- **OCR(로컬)**: `uv sync --extra ocr`로 PaddleOCR을 설치해야 한다. 이 맥(M5 Pro)은 prod(t3.large 2 vCPU)와
  달라 **절대값은 쓰지 않고 A/B 상대 비교만** 한다(`OCR_CPU_THREADS=2`로 맞춘다). 클러스터 vLLM에 닿지
  않으면 VLM 단계가 폴백 경로로 돌아 측정 대상이 달라진다는 점에 주의한다.
