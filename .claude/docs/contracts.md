# 인터페이스 계약서 (Contracts)

워커·모듈 간 **통합 경계**를 정의한다. 팀원이 각 패키지를 병렬 개발하려면 **이 계약을 먼저 합의·고정**해야 한다. 계약이 어긋나면 발행자·소비자 사이 메시지가 깨진다(통합 버그 1순위 원인).

이 문서는 `src/core/contracts.py`(코드 단일 출처)가 구현해야 할 스펙이다. 변경 시 **발행자·소비자 양쪽 담당자와 합의**하고 이 문서 + `contracts.py` + 변경 이력을 함께 갱신한다.

## 통신 맵

| 구간 | 방식 | 계약 |
|---|---|---|
| Spring → `ocr_worker` | Kafka `ocr-job-queue` | [`OcrJob`](#1-ocrjob) |
| `ocr_worker` → DB | `ocr_results` 테이블 | [`ocr_results`](#3-ocr_results-db-교차-계약) |
| `ocr_worker` → `report_worker` | Kafka `report-job` | [`ReportJob`](#2-reportjob) |
| `report_worker` → DB | `report_drafts` 테이블 | [`report_drafts`](#4-report_drafts-db-교차-계약) |
| Frontend ↔ `chatbot` | WebSocket / REST | [chatbot-events.md](chatbot-events.md) |
| `report_worker`·`chatbot` → `rag` | Python 함수 호출 | [RAG 인터페이스](#5-rag-모듈-함수-인터페이스) |
| `report_worker`·`chatbot` → `guardrail` | Python 함수 호출 | [가드레일 인터페이스](#6-가드레일-모듈-함수-인터페이스) |
| 전 워커 → `ai_client` | OpenAI 호환 HTTP | [ai_client 인터페이스](#7-ai_client-인터페이스) |

공통 규약:
- 모든 메시지는 **UTF-8 JSON**, 시각은 **ISO-8601(UTC)** 문자열.
- 식별자는 **UUIDv4 문자열**.
- Kafka 메시지는 pydantic으로 **역직렬화 즉시 검증**. 실패 시 DLQ.
- **PII는 계약에 절대 평문으로 싣지 않는다**(마스킹 후 값만).

---

## 1. `OcrJob`
**Spring → `ocr-job-queue` → `ocr_worker`.** 파일 업로드 시 발행되는 OCR 작업.

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| `job_id` | string(UUID) | ✓ | OCR 작업 식별자. 멱등 키 |
| `s3_key` | string | ✓ | 업로드 파일 S3 키 (파일명은 UUID로 치환됨) |
| `content_type` | string | ✓ | `application/pdf` `image/jpeg` `image/png` `image/tiff` |
| `user_ref` | string | ✓ | 사용자 참조(내부 식별자, PII 아님) |
| `doc_type_hint` | string \| null | – | 업로드 시 사용자가 고른 문서 유형 힌트 |
| `claim_id` | string(UUID) \| null | – | `USER_CLAIMS.id` 참조(옵셔널). `ocr_worker`는 가공 없이 `ReportJob`으로 패스스루만 |
| `uploaded_at` | string(ISO-8601) | ✓ | 업로드 시각(UTC) |

```json
{
  "job_id": "8f1c2d3e-...-a1",
  "s3_key": "uploads/8f1c2d3e.pdf",
  "content_type": "application/pdf",
  "user_ref": "u_4821",
  "doc_type_hint": null,
  "claim_id": null,
  "uploaded_at": "2026-06-17T05:30:00Z"
}
```
- **토픽**: `ocr-job-queue` · **파티션 키**: `job_id` · **처리**: at-least-once, `job_id` 기준 멱등 처리.

---

## 2. `ReportJob`
**`ocr_worker` → `report-job` → `report_worker`.** OCR·마스킹 완료 후 리포트 생성 트리거.

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| `report_id` | string(UUID) | ✓ | 리포트 식별자(신규 생성). 멱등 키 |
| `ocr_result_id` | string(UUID) | ✓ | `ocr_results.id` 참조 |
| `job_id` | string(UUID) | ✓ | 원 OCR 작업 추적용 |
| `doc_type` | string | ✓ | 분류된 문서 유형(아래 enum) |
| `user_ref` | string | ✓ | 사용자 참조 |
| `claim_id` | string(UUID) \| null | – | `USER_CLAIMS.id` 패스스루(옵셔널). `report_worker`가 DB에서 직접 조회 |
| `created_at` | string(ISO-8601) | ✓ | 발행 시각(UTC) |
| `ocr_quality` | string | ✓ | `ocr_worker`의 자동 품질 판정(아래 enum). 기본값 `"ok"` |

`doc_type` enum: `diagnosis`(진단서) · `policy`(보험증권) · `payout_notice`(지급결과안내문) · `claim`(청구서) · `hospitalization_cert`(입퇴원확인서) · `medical_receipt`(진료비계산서·영수증) · `other`(기타)

`ocr_quality` enum: `ok`(정상) · `needs_reupload`(재확인 필요 — surya 신뢰도가 낮은데 이름·도메인 정보가 하나도 검출되지 않은 저품질 문서). **이 신호를 실제로 소비해 리포트 생성을 건너뛰고 사용자에게 재업로드를 알리는 것은 `report_worker` + 게이트웨이 몫** — `ocr_worker`는 판정만 하고 값만 실어 발행한다(이번 범위는 여기까지).

> 토픽명은 `core.contracts`에 상수로 공유된다: `OCR_JOB_TOPIC = "ocr-job-queue"`, `REPORT_JOB_TOPIC = "report-job"`.

```json
{
  "report_id": "b2a9...-77",
  "ocr_result_id": "c3d8...-12",
  "job_id": "8f1c2d3e-...-a1",
  "doc_type": "diagnosis",
  "user_ref": "u_4821",
  "claim_id": null,
  "created_at": "2026-06-17T05:31:10Z",
  "ocr_quality": "ok"
}
```
- **토픽**: `report-job` · **파티션 키**: `report_id` · 멱등 처리.

---

## 3. `ocr_results` (DB 교차 계약)
`ocr_worker`가 **쓰고**, `report_worker`가 `ocr_result_id`로 **읽는다.** 마스킹된 텍스트·라인좌표·비식별 이미지 참조만 보관(원문·평문 PII 금지).

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `id` | uuid PK | `ReportJob.ocr_result_id`로 참조됨 |
| `job_id` | uuid | OCR 작업 |
| `doc_type` | text | 분류 결과(`doc_type` enum) |
| `doc_type_confidence` | real | 분류 신뢰도(0~1) |
| `ocr_confidence` | real | OCR 라인 평균 신뢰도(0~1) — 저신뢰 QA 플래깅 |
| `masked_text` | text | **PII 마스킹된** OCR 텍스트 (downstream 입력). 표 문서 4종(`payout_notice`·`claim`·`hospitalization_cert`·`medical_receipt`)은 하이브리드 VLM이 성공하면 마크다운 표 문법(`\|`·`---`)을 포함할 수 있음(실패 시 surya 평문 폴백) — LLM은 마크다운을 문제없이 소화하므로 `report_worker` 측 별도 분기는 불필요 |
| `masked_lines` | jsonb | 라인 단위 `[{masked_text, bbox, polygon, confidence}]` — bbox/polygon/conf 보존, **텍스트는 마스킹본**(이미지 마스킹 좌표 재사용). **항상 surya 기반**(VLM은 좌표를 반환하지 않음). 좌표 기반 리딩오더 정렬(시각적 위→아래·좌→우) 후 페이지 단위로 PII를 검출해, 라벨과 값이 서로 다른 라인으로 쪼개진 경우도 페이지 컨텍스트로 잡는다 — 이미지 검은블록 마스킹(`ImageMasker.redact_pages`)과 **동일 판정 로직을 공유**해 두 트랙 간 불일치가 없다. 잔여 한계: 라벨-값 사이에 매우 긴 문단이 끼어 앵커 정규식의 lookahead 범위를 넘는 극단적 경우는 여전히 놓칠 수 있음(후속 과제) |
| `entities` | jsonb | 추출 엔티티(아래). `table_markdown`(string\|null, optional)은 표 문서 4종에서만 등장 — VLM 전사 후 마스킹 완료 상태 |
| `masked_image_s3_keys` | jsonb | 검은블럭 비식별 이미지 사본 S3 키(페이지별 리스트) |
| `ocr_quality` | text | 자동 품질 판정(`ok` \| `needs_reupload`). `ReportJob.ocr_quality`로 패스스루됨 |
| `created_at` | timestamptz | |

`entities` 예시(문서 유형별 일부):
```json
{
  "diagnosis_name": "S82.1",        // KCD 코드
  "insurer": "○○생명",
  "product": "무배당 ...",
  "payout_amount": null              // 단정 금지 — 추출값은 참고용
}
```

`masked_lines` 예시(텍스트는 마스킹본, 좌표·confidence는 원형 — 좌표/신뢰도는 PII 아님):
```json
[
  {"masked_text": "보험계약자 ***", "bbox": [72,140,520,168],
   "polygon": [[72,140],[520,140],[520,168],[72,168]], "confidence": 0.99}
]
```

`masked_image_s3_keys` 예시(비식별 사본 — 원본 키와 분리):
```json
["masked/<job_id>/page-0.png", "masked/<job_id>/page-1.png"]
```

- **이미지 마스킹 트랙(손해사정사 비식별 열람용)**: 디텍터 검출 1회 → 텍스트 마스킹 + 이미지 마스킹 2갈래. 줄 단위 검은블럭으로 렌더한 **비식별 이미지 사본은 S3**, `ocr_results`엔 **키만** 적재. **원본 이미지는 삭제 금지** — KMS·IAM·Lifecycle 자동삭제로 잠금보관(법적 보존·분쟁·재처리).
- **PII 안전**: 좌표·confidence는 PII가 아니라 보존하나, `masked_lines`의 텍스트는 **마스킹본만**. 원문/평문 PII는 `ocr_results`·로그·타 토픽에 절대 금지.
- **보존**: 리포트 확정 전까지만. **손해사정사 서명 완료 이벤트** 시 즉시 삭제(개인정보 최소보존). 비식별 이미지 사본도 동일 트리거로 삭제.

> **2026-06-30 (additive):** `doc_type_confidence`·`ocr_confidence`·`masked_lines`·`masked_image_s3_keys` 추가(이미지 마스킹 트랙 도입, #13 범위 확장). `masked_text`·`entities`는 불변 → `report_worker` 측 **비파괴**. 신규 컬럼은 `ocr_worker`만 기록, 비식별 이미지 소비(손해사정사 UI)는 `ocr_result_id`로 조회.
>
> **2026-08-02 (additive):** `doc_type` enum에 `hospitalization_cert`(입퇴원확인서)·`medical_receipt`(진료비계산서·영수증) 추가(CHECK 제약은 `migrations/003_ocr_results_doctype_expand.sql`로 확장). 이 2종 + 기존 `payout_notice`·`claim`은 다중 항목 표 문서라 하이브리드 VLM 경로(§3 하단 참고)가 `masked_text`/`entities.table_markdown`에 관여할 수 있다.
>
> **2026-08-02 (additive):** `ocr_results.ocr_quality`·`ReportJob.ocr_quality` 추가(CHECK 제약은 `migrations/004_ocr_results_quality.sql`). surya 신뢰도가 낮은데(<0.90) 문서 전체에서 이름·도메인 정보가 하나도 검출되지 않으면 `needs_reupload`로 표시된다. 이와 별개로, 표 문서 4종 한정이던 하이브리드 VLM 트리거를 신뢰도 조건(<0.90)으로 확대해 문서 유형 무관하게 저품질 문서에도 VLM 보완을 시도하며, VLM 결과는 surya 원문과의 토큰 중복률 기반 groundedness 체크를 통과해야만 채택된다(환각 방지, 실패 시 surya 폴백). `masked_lines`도 이미지 마스킹 트랙과 판정 로직을 공유하도록 갱신(위 표 참고). `doc_type`·`masked_text`·`entities` 등 기존 필드는 불변 → `report_worker` 측 비파괴. **`ocr_quality` 신호를 소비해 리포트 생성 여부를 결정하고 사용자에게 재업로드를 안내하는 것은 `report_worker` + 게이트웨이 범위** — 이번 변경은 `ocr_worker`의 판정·발행까지만 다룬다.

---

## 4. `report_drafts` (DB 교차 계약)
`report_worker`가 쓰는 AI 리포트 초안(JSONB, 영구 보존 — 손해사정사 검수 근거). 손해사정사 UI·후속 단계가 읽는다.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `report_id` | uuid PK | `ReportJob.report_id` |
| `draft` | jsonb | 리포트 초안 본문(아래) |
| `status` | text | `draft` / `signed` / `rejected` |
| `created_at` | timestamptz | |

`draft` 구조(권장):
```json
{
  "sections": [
    {"title": "사고 개요", "body": "...", "citations": ["[제3조]", "[2021다1234]"]}
  ],
  "estimated_range": {"min": 3000000, "max": 5000000, "unit": "KRW"},
  "disclaimer": "본 분석은 참고용이며 ...",
  "judge_failures": []   // 인용 검증 실패로 삭제된 섹션 기록
}
```
- 금액은 **단정 금지** → 항상 `estimated_range`(범위)로만 표현(가드레일 강제).

---

## 5. RAG 모듈 함수 인터페이스
`src/rag` — `report_worker`·`chatbot`이 **함수 호출**. 순수 함수형(부수효과 없음).

```python
async def search(
    query: str,
    insurance_type: str | None = None,   # 신체보험 유형 힌트(비신체는 범위 외)
    namespaces: list[str] | None = None, # None이면 라우터가 결정. {terms, case} (level·medical 향후)
    top_k: int = 8,
) -> RagResult: ...

class Chunk(BaseModel):
    text: str               # 청크 원문 (POLICY_CHUNKS.content / CASE_CHUNKS.content)
    namespace: str          # terms(POLICY_CHUNKS) | case(CASE_CHUNKS). level·medical 향후
    score: float            # RRF 통합 점수
    source_ref: str         # 원문 위치 참조 (chunk_id)

class Citation(BaseModel):
    clause_no: str | None   # 조항/사례 번호 (POLICY_CHUNKS.article_number / CASE_CHUNKS.case_number)
    exhibit: str | None     # 별표·항목 (POLICY_CHUNKS.section, 있으면)

class RagResult(BaseModel):
    ranked_chunks: list[Chunk]
    citations: list[Citation]
```
- 임베딩 차원 **1024 고정**. 비신체보험 쿼리는 빈 결과 + 범위 외 사유 반환.
- **`namespace`는 PG 컬럼이 아니라 파생값** — 검색한 소스 테이블로 부여(`POLICY_CHUNKS`→`terms`, `CASE_CHUNKS`→`case`). 물리 스키마는 ERD(Notion) 참조.
- 현재 구현 대상은 `terms`·`case` 2종(`POLICY_CHUNKS`·`CASE_CHUNKS`). `level`(장해분류)·`medical`(수가·KCD)은 테이블 미존재 → 향후 확장.
- 인용 출처 URL 컬럼은 보유 테이블에 없어 계약에서 제외(필요 시 `CASE_CHUNKS.source_id`로 조합).

---

## 6. 가드레일 모듈 함수 인터페이스
`src/guardrail` — 단계별 함수로 분리(호출자가 필요한 것만 조립). 리포트=3단계 전부, 챗봇=입력·생성·출력(LLM Judge 제외).

```python
async def guard_input(text: str) -> InputGuardResult: ...
def guard_generation(text: str) -> str: ...                       # 단정표현 치환·인용 강제 검사
async def guard_output(
    text: str, *, run_judge: bool, chunks: list[Chunk] | None = None
) -> OutputGuardResult: ...

class InputGuardResult(BaseModel):
    masked_text: str         # PII 마스킹(주민번호 앞 6자리만 보존)
    blocked: bool            # 도메인 외 질문 차단 여부
    reason: str | None       # 차단 사유

class OutputGuardResult(BaseModel):
    final_text: str          # 고지문 삽입됨
    judge_failures: list[str]  # 인용 검증 실패 섹션(리포트만, run_judge=True)
```
- `run_judge`: 리포트=True(EXAONE/LLM Judge로 인용↔원문 검증), **챗봇=False**.
- PII 마스킹 규칙은 `ocr_worker` 입력단과 **동일 정의** 공유.

---

## 7. `ai_client` 인터페이스
`src/core/ai_client.py` — OpenAI 호환(Ollama/vLLM/TEI). 모델·엔드포인트는 config 주입(모델 미정).

```python
async def chat(messages: list[dict[str, str]], **opts) -> str: ...   # 챗봇은 비스트리밍
async def embed(text: str) -> list[float]: ...                       # len == 1024 보장
```

---

## 변경 정책
- 계약 변경은 **추가(backward-compatible) 우선**. 필드 삭제·타입 변경은 발행자·소비자 동시 배포 필요 → 사전 합의 필수.
- 변경 시: 이 문서 → `core/contracts.py` → 관련 테스트 순으로 갱신하고 PR에 영향 범위를 명시한다.
