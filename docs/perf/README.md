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

```bash
# 디플로이먼트 이름·네임스페이스는 K8s-manifests 기준으로 바꾼다(아래는 예시)
kubectl logs deploy/report-worker --since=72h > report.log
kubectl logs deploy/ocr-worker    --since=72h > ocr.log
uv run python scripts/perf/summarize_events.py report.log ocr.log --json baseline.json
```

로그가 CloudWatch Logs에 있으면 Logs Insights로도 같은 값을 본다:

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

기준선 수집 범위: dev에서 **최소 리포트 20건·OCR 문서 30건**(문서 유형·쪽 수가 섞이게). 부족하면
고정 샘플(`samples/` 9종)을 dev 큐에 투입해 채운다.

## 4. SLO (기준선 확정 후 채운다)

| 대상 | SLI | 기준선 p50 / p95 / p99 | 목표 SLO | 근거 |
|---|---|---|---|---|
| 리포트 | `sqs.message.finished.handle_ms` (report-job, done) | TBD | TBD | §1 안전선 p99 ≤ 480초는 최소 |
| OCR | `ocr.job.finished.duration_ms` (status=ok) | TBD | TBD | 〃 |
| OCR (쪽 정규화) | `duration_ms / pages` | TBD | TBD | 쪽 수 영향 제거 |

**품질 가드레일(비회귀 조건)** — 개선안이 아래를 기준선보다 나쁘게 만들면 기각한다.

| 대상 | 지표 | 출처 |
|---|---|---|
| 리포트 | 신뢰도 등급 분포(HIGH·MEDIUM·LOW) | `draft.metrics.confidence.level` |
| 리포트 | Judge 미확인 주장 수·검증 범위 | `draft.metrics.verification` |
| 리포트 | 그래프 실패율(`failed_nodes` 비율) | `report.graph.finished` |
| OCR | 마스킹 결과 **동일**(텍스트·`masked_lines`·검은블록) | 고정 샘플 diff |
| OCR | 잔류 PII 경고 0건, `needs_reupload` 비율 | 로그·`ocr_results.ocr_quality` |

## 5. 실험 백로그 (기준선을 본 뒤 순서 재조정)

| ID | 가설 | 변경 | 측정 | Claude 비용 |
|---|---|---|---|---|
| R1 | 리포트 본문·쟁점 추출은 서로 의존하지 않아 병렬화하면 `report_compose`가 짧은 쪽 호출만큼 준다 | `agents.py` 본문(`chat`)∥쟁점(`chat_json`) `gather` | `node_ms.report_compose` | 실험 실행 시 발생(호출 수는 동일) |
| R2 | JSON 추출성 호출(쟁점·판례 필터·진단)은 effort를 낮춰도 품질이 유지된다 | 노드별 effort 차등 | `llm.chat` 노드별 `duration_ms`·`output_tokens` + 품질 가드 | 실험 실행 시 발생 |
| R3 | `case_search`는 진단에만 의존해 coverage 경로와 병렬화할 수 있다 | 그래프 fan-out | `total_ms` | 실험 실행 시 발생 |
| R4 | (보호) 리포트도 처리 시간 상한이 가시성 안에 들어야 한다 | 기동 예산 검사 또는 가시성 연장(heartbeat) | p99 vs 600초 | 없음 |
| O1 | NER이 같은 줄을 문서당 4번 돌고 배치도 안 돼 CPU 2코어에서 큰 몫을 차지한다 | 줄 단위 결과 재사용 + `batch_size` | `analyze`·`quality_check`·`redact` | 없음 |
| O2 | `quality_check` NER이 이벤트 루프를 막는다 | `analyze` 결과 재사용 또는 `to_thread` | `quality_check` | 없음 |
| O3 | 5.2 재OCR 검증이 OCR 엔진 비용을 한 번 더 쓴다 | 측정 후 결정(대상 페이지 축소 등) | `verify` vs `ocr_engine` | 없음 |
| G1 | 오타보정이 trigram 인덱스를 못 타고 토큰마다 DB를 순차 왕복한다 | `%` 연산자 + `unnest` 일괄 조회 | RAG 노드 ms | 없음 |
| G2 | 공용 RDS 파라미터 그룹(`shared_buffers` 32MB)이 HNSW 검색을 디스크로 보낸다 | 파라미터 그룹 교체(인프라) | RAG 노드 ms·`EXPLAIN (ANALYZE, BUFFERS)` | 없음 |

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

(아직 없음)

## 7. 로컬 벤치 환경 메모

- **리포트**: `run_experiments.py`의 4시나리오를 쓴다. 다만 `tempVectorDB/init/02_app_tables.sql`이
  7월 기준이라 dev 스키마와 다르다(`user_claims.details` 등 누락) — 스키마를 맞추고 코퍼스를 Voyage로
  적재해야 돈다. Claude·Voyage 키가 필요하다(**Claude 실험 보류 중**).
- **OCR**: `uv sync --extra ocr`로 PaddleOCR을 설치해야 한다. 이 맥(M5 Pro)은 prod(t3.large 2 vCPU)와
  달라 **절대값은 쓰지 않고 A/B 상대 비교만** 한다(`OCR_CPU_THREADS=2`로 맞춘다). 클러스터 vLLM에 닿지
  않으면 VLM 단계가 폴백 경로로 돌아 측정 대상이 달라진다는 점에 주의한다.
