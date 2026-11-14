# 인터페이스 계약서 (Contracts)

워커·모듈 간 **통합 경계**를 정의한다. 팀원이 각 패키지를 병렬 개발하려면 **이 계약을 먼저 합의·고정**해야 한다. 계약이 어긋나면 발행자·소비자 사이 메시지가 깨진다(통합 버그 1순위 원인).

이 문서는 `src/core/contracts.py`(코드 단일 출처)가 구현해야 할 스펙이다. 변경 시 **발행자·소비자 양쪽 담당자와 합의**하고 이 문서 + `contracts.py` + 변경 이력을 함께 갱신한다.

## 통신 맵

| 구간 | 방식 | 계약 |
|---|---|---|
| Spring → `ocr_worker` | SQS `ocr-job-queue`(Standard, `SQS_OCR_JOB_QUEUE_URL`) | [`OcrJob`](#1-ocrjob) |
| `ocr_worker` → DB | `ocr_results` 테이블 | [`ocr_results`](#3-ocr_results-db-교차-계약) |
| `ocr_worker` → `report_worker` | SQS `report-job`(Standard, `SQS_REPORT_JOB_QUEUE_URL`) | [`ReportJob`](#2-reportjob) |
| `report_worker` → DB | `report_drafts` 테이블 | [`report_drafts`](#4-report_drafts-db-교차-계약) |
| `report_worker` → `rag` | Python 함수 호출 | [RAG 인터페이스](#5-rag-모듈-함수-인터페이스) |
| `report_worker` → `guardrail` | Python 함수 호출 | [가드레일 인터페이스](#6-가드레일-모듈-함수-인터페이스) |
| 전 워커 → `ai_client` | OpenAI 호환 HTTP 또는 Claude(anthropic SDK)·Voyage — env provider 스위치 | [ai_client 인터페이스](#7-ai_client-인터페이스) |

공통 규약:
- 모든 메시지는 **UTF-8 JSON**, 시각은 **ISO-8601(UTC)** 문자열.
- 식별자는 **UUIDv4 문자열**.
- SQS 메시지는 pydantic으로 **역직렬화 즉시 검증**한다. 처리 성공 후에만 삭제(ack)하고, 실패하면 삭제하지 않아 visibility timeout 뒤 재전달된다(at-least-once — 소비자는 멱등이어야 한다).
- 결정적 실패(`core.exceptions.NonRetryableError`)는 첫 시도에 즉시 ack한다. 수신 횟수가 `SQS_MAX_RECEIVE_COUNT`(코드 기본 5, report 운영값 3)를 넘으면 컨슈머의 poison 가드가 실패 저널(`ai.ocr_job_failures`·`ai.report_job_failures` §4-1)에 terminal로 기록한 뒤 걷어낸다. 큐 redrive(DLQ)의 `maxReceiveCount`는 이 값보다 커야 저널 기록이 먼저 일어난다.
- **PII는 계약에 절대 평문으로 싣지 않는다**(마스킹 후 값만).

---

## 1. `OcrJob`
**Spring → `ocr-job-queue` → `ocr_worker`.** 문서 1건마다 발행되는 OCR 작업.

**소유권(2026-07-10 변경, 발행측 `OcrJob.java` 정본):** Spring이 `reports`·`report_attachments` shell 행을 먼저 생성하고, 워커는 OCR·AI 결과로 그 행을 **UPDATE(생성 아님)** 한다. `report_id`·`attachment_id`가 그 UPDATE 대상 참조 키다. 한 청구의 문서를 **1건씩** 발행하므로 `doc_index`/`doc_total`로 워커가 리포트 생성(**fan-in**) 시점을 판별한다.

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| `job_id` | string(UUID) | ✓ | OCR 작업 식별자. 멱등 키 |
| `s3_key` | string | ✓ | 업로드 파일 S3 키 (파일명은 UUID로 치환됨) |
| `content_type` | string | ✓ | `application/pdf` `image/jpeg` `image/png` `image/tiff` |
| `user_ref` | string | ✓ | 사용자 참조(내부 식별자, PII 아님) |
| `doc_type_hint` | string \| null | – | 업로드 시 사용자가 고른 문서 유형 힌트 |
| `claim_id` | string(UUID) \| null | – | `USER_CLAIMS.id` 참조(옵셔널). `ocr_worker`는 가공 없이 `ReportJob`으로 패스스루만 |
| `report_id` | string(UUID) | ✓ | `REPORTS.id` 참조 — 결과 UPDATE 대상(Spring이 shell 생성) |
| `attachment_id` | string(UUID) | ✓ | `REPORT_ATTACHMENTS.id` 참조 — 결과 UPDATE 대상 |
| `doc_index` | int \| null | – | 청구 내 문서 순번(1-based) |
| `doc_total` | int \| null | – | 청구 총 문서 수 — 워커의 fan-in(리포트 생성) 판별용 |
| `uploaded_at` | string(ISO-8601) | ✓ | 업로드 시각(UTC) |

```json
{
  "job_id": "8f1c2d3e-...-a1",
  "s3_key": "uploads/8f1c2d3e.pdf",
  "content_type": "application/pdf",
  "user_ref": "3f0e...-uid",
  "doc_type_hint": "diagnosis",
  "claim_id": "c1a1...-cl",
  "report_id": "r2b2...-rp",
  "attachment_id": "a3c3...-at",
  "doc_index": 1,
  "doc_total": 3,
  "uploaded_at": "2026-07-10T09:21:16.123Z"
}
```
- **큐**: `ocr-job-queue`(SQS Standard — 순서 보장·파티션 키 없음) · **처리**: at-least-once, `job_id` 기준 멱등 처리.
- **본 변경 범위(#33)**: 계약(필드) 추가·검증에 한정한다. 워커가 `report_id`를 자체 파생하는 현행 로직(`pipeline._derive_report_id`)을 `job.report_id` 패스스루로 바꾸고 fan-in으로 `ReportJob` 발행을 게이팅하는 작업, §2 `ReportJob` 의미 정정은 **후속(fan-in 설계 결정 필요)** 으로 분리한다.

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

`ocr_quality` enum: `ok`(정상) · `needs_reupload`(재확인 필요 — OCR 엔진 평균 신뢰도가 `ocr_low_confidence_threshold`(기본 0.90, surya 시절 실측값 — PaddleOCR PoC에서 재보정) 미만인데 이름·도메인 정보가 하나도 검출되지 않은 저품질 문서). **이 신호를 실제로 소비해 리포트 생성을 건너뛰고 사용자에게 재업로드를 알리는 것은 `report_worker` + 게이트웨이 몫** — `ocr_worker`는 판정만 하고 값만 실어 발행한다(이번 범위는 여기까지).

> 논리 큐 이름은 `core.contracts` 상수로 공유된다: `OCR_JOB_TOPIC = "ocr-job-queue"`, `REPORT_JOB_TOPIC = "report-job"`(Kafka 시절 상수명이 남은 것 — 값은 큐 이름). 실제 큐 주소는 env `SQS_OCR_JOB_QUEUE_URL`·`SQS_REPORT_JOB_QUEUE_URL`이 정한다.

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
- **큐**: `report-job`(SQS Standard, 파티션 키 없음) · `report_id` 기준 멱등 처리. 재전달 한도를 다 쓰면 `ai.report_job_failures`에 terminal로 남는다(§4-1).

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
| `masked_lines` | jsonb | 라인 단위 `[{masked_text, bbox, polygon, confidence}]` — bbox/polygon/conf 보존, **텍스트는 마스킹본**(이미지 마스킹 좌표 재사용). **항상 OCR 엔진 라인 기반**(2026-11-06부터 PaddleOCR PP-OCRv5, 그 전엔 surya. VLM은 좌표를 반환하지 않음). 좌표 기반 리딩오더 정렬(시각적 위→아래·좌→우) 후 페이지 단위로 PII를 검출해, 라벨과 값이 서로 다른 라인으로 쪼개진 경우도 페이지 컨텍스트로 잡는다 — 이미지 검은블록 마스킹(`ImageMasker.redact_pages`)과 **동일 판정 로직을 공유**해 두 트랙 간 불일치가 없다. **치환도 페이지 스팬 재사용(2026-08-04)**: 이전엔 "이 라인이 PII인가"는 페이지 단위로 판정하고 실제 치환은 라인 텍스트만 떼어 `mask()`를 다시 돌렸는데, 값만 있는 라인은 라벨 컨텍스트가 없어 재검출이 실패해 원문이 그대로 남는 경로가 있었다(실측 확인) — 이제 페이지에서 검출한 스팬을 라인별 로컬 오프셋으로 잘라(`line_local_spans`) 재검출 없이 그대로 치환(`apply_mask`)한다. 잔여 한계: 라벨-값 사이에 매우 긴 문단이 끼어 앵커 정규식의 lookahead 범위를 넘는 극단적 경우는 여전히 놓칠 수 있음(후속 과제). **저신뢰 라인 재판독으로 보강(2026-11-05, 이전 VLM grounding 대체)**: surya가 PII를 완전히 다른 문자로 오독하면(예: 실제 이름을 숫자열로 오독) 라인 기반 판정이 그 라인을 놓친다. `ocr_confidence < 0.90`일 때만 `ImageMasker.reread_pages`가 OCR 엔진 라인 bbox crop을 PaddleOCR-VL(`"OCR:"`)로 다시 읽는다. 그다음 재판독 텍스트로 바꾼 페이지에 같은 페이지 단위 검출을 돌리고, 나온 PII 라인을 엔진 텍스트 기반 판정에 **라인 인덱스로 합친다**(대체 아님 — 5.1 커버리지 검증도 그 라인까지 잰다). 이미지 트랙(`masked_image_s3_keys`)에만 적용되고 `masked_lines` 텍스트는 그대로다. 재판독에 실패한 라인은 건너뛴다(fail-open, 엔진 단독보다 후퇴 없음). 잔여 한계: OCR 엔진이 라인 자체를 검출하지 못한 영역은 다시 읽지 않는다. 2026-08-04~11-04의 Qwen grounding(`qwen3-vl:8b-instruct`, 정규화 0~1000 좌표)은 그런 영역도 가릴 수 있었다. 저신뢰 문서마다 라인 수만큼 VLM 호출이 늘어난다(`vlm_concurrency` 상한, `vlm_reread_budget_seconds` 예산 — 넘기면 남은 라인은 건너뛰고, 신뢰도 낮은 라인부터 읽는다) |
| `entities` | jsonb | 추출 엔티티(아래). `table_markdown`(string\|null, optional)은 표 문서 4종에서만 등장 — VLM 전사 후 마스킹 완료 상태. **소비 구분(2026-08-04)**: `icd`(`DIAGNOSIS`)·`admission_days`/`surgery`(`HOSPITALIZATION_CERT`)는 `report_worker`(#11)의 지급액 산정 로직이 실제로 참조하는 필드명이다. `insurer`/`product`(`POLICY`)·`payout_amount`(`PAYOUT_NOTICE`·`CLAIM`)·`diagnosis_name`(`DIAGNOSIS`, 코드 없을 때 한글 병명 폴백)은 `report_worker`가 소비하지 않고 `ocr_worker` 내부 `ocr_quality`(재확인 필요) 판정에만 쓰인다 — report_worker는 이 값들 대신 `user_insurances`/`user_claims` 테이블을 직접 조회한다. **알려진 한계(실측 확인, 2026-08-04)**: `payout_amount`는 다중 항목 표에서 문맥 키워드에 가장 먼저 부합하는 금액 하나만 뽑는 구조라, "합계"·최종 지급 예정액이 아닌 개별 항목 금액을 뽑거나 — 최악의 경우 **부지급(거절)된 항목의 청구금액**을 뽑을 수 있다(어느 쪽이든 report_worker가 안 쓰므로 리포트 정확도엔 영향 없음). 참고용으로만 쓰고 절대 단정하지 말 것 |
| `masked_image_s3_keys` | jsonb | 검은블럭 비식별 이미지 사본 S3 키(페이지별 리스트) |
| `ocr_quality` | text | 자동 품질 판정(`ok` \| `needs_reupload`). `ReportJob.ocr_quality`로 패스스루됨 |
| `created_at` | timestamptz | |

`entities` 예시(문서 유형별 일부):
```json
{
  "diagnosis_name": "S82.1",        // KCD 코드 있으면 코드, 없으면 라벨 뒤 한글 병명 폴백(실측: 코드 없는 문서가 더 흔함) — quality 판정용
  "icd": "S82.1",                    // 코드 없으면 폴백 없이 null — report_worker 소비 필드
  "insurer": "○○생명",               // quality 판정용, report_worker 비소비
  "product": "무배당 ...",           // quality 판정용, report_worker 비소비
  "payout_amount": null,             // 단정 금지 — 참고용, report_worker 비소비
  "admission_days": 5,               // HOSPITALIZATION_CERT — report_worker 소비 필드(참고값)
  "surgery": true                    // HOSPITALIZATION_CERT — '수술명' 라벨 없으면 null(알 수 없음)
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
- **PII 안전**: 좌표·confidence는 PII가 아니라 보존하나, `masked_lines`의 텍스트는 **마스킹본만**. 원문/평문 PII는 `ocr_results`·로그·다른 큐 메시지에 절대 금지.
- **보존**: 리포트 확정 전까지만. **손해사정사 서명 완료 이벤트** 시 즉시 삭제(개인정보 최소보존). 비식별 이미지 사본도 동일 트리거로 삭제.

> **2026-06-30 (additive):** `doc_type_confidence`·`ocr_confidence`·`masked_lines`·`masked_image_s3_keys` 추가(이미지 마스킹 트랙 도입, #13 범위 확장). `masked_text`·`entities`는 불변 → `report_worker` 측 **비파괴**. 신규 컬럼은 `ocr_worker`만 기록, 비식별 이미지 소비(손해사정사 UI)는 `ocr_result_id`로 조회.
>
> **2026-08-02 (additive):** `doc_type` enum에 `hospitalization_cert`(입퇴원확인서)·`medical_receipt`(진료비계산서·영수증) 추가(CHECK 제약은 `migrations/003_ocr_results_doctype_expand.sql`로 확장). 이 2종 + 기존 `payout_notice`·`claim`은 다중 항목 표 문서라 하이브리드 VLM 경로(§3 하단 참고)가 `masked_text`/`entities.table_markdown`에 관여할 수 있다.
>
> **2026-08-02 (additive):** `ocr_results.ocr_quality`·`ReportJob.ocr_quality` 추가(CHECK 제약은 `migrations/004_ocr_results_quality.sql`). surya 신뢰도가 낮은데(<0.90) 문서 전체에서 이름·도메인 정보가 하나도 검출되지 않으면 `needs_reupload`로 표시된다. 이와 별개로, 표 문서 4종 한정이던 하이브리드 VLM 트리거를 신뢰도 조건(<0.90)으로 확대해 문서 유형 무관하게 저품질 문서에도 VLM 보완을 시도하며, VLM 결과는 surya 원문과의 토큰 중복률 기반 groundedness 체크를 통과해야만 채택된다(환각 방지, 실패 시 surya 폴백). `masked_lines`도 이미지 마스킹 트랙과 판정 로직을 공유하도록 갱신(위 표 참고). `doc_type`·`masked_text`·`entities` 등 기존 필드는 불변 → `report_worker` 측 비파괴. **`ocr_quality` 신호를 소비해 리포트 생성 여부를 결정하고 사용자에게 재업로드를 안내하는 것은 `report_worker` + 게이트웨이 범위** — 이번 변경은 `ocr_worker`의 판정·발행까지만 다룬다.
>
> **2026-08-03 (additive):** 하이브리드 VLM 경로가 다중 페이지 문서에서 페이지별로 독립 채택되도록 수정. 이전엔 1페이지 VLM 성공만으로 `masked_text` 전체가 그 페이지 결과로 교체돼 2페이지 이후 내용이 유실되는 결함이 있었다 — 이제 페이지마다 VLM 성공/실패가 갈리고, 실패한 페이지는 그 페이지의 surya 결과로 개별 폴백한다. `entities.table_markdown`도 채택된 페이지만 구분자(`\n\n---\n\n`)로 이어붙인다(타입은 여전히 `string`, 계약 형태 불변).
>
> **2026-08-04 (additive, 실측 기반):** 실제 문서(진단서·보험증권·지급결과통보서·입퇴원확인서·진료비영수증 × 3화질)로 E2E 재검증 중 발견한 것들을 반영. (1) NER이 흔치 않은 합성 이름(예: "이샘플")을 화질과 무관하게 놓치는 사례를 확인해, "환자 성명"·"피보험자"·"계약자"·"예금주" 등 라벨 뒤 이름을 잡는 정규식 안전망(`PERSON_LABEL_NAME_RE`)을 NER과 별개로 추가 — 마크다운 표 형태(`\| 라벨 \| 값 \|`)의 VLM 원문에서도 동작한다. (2) `entities` 병합 우선순위를 반전 — surya가 이미 값을 찾았어도 VLM이 채택되면 VLM 쪽 값을 우선한다(VLM은 이미 groundedness 검증을 통과했고 애초에 surya 신뢰도가 낮아 호출된 것이므로 더 신뢰할 근거가 있음 — surya가 금액을 다른 문서 필드로 오독해 엉뚱한 값을 채운 사례가 실측으로 확인됨). (3) VLM 프롬프트에 "표뿐 아니라 상단 라벨-값 블록도 빠짐없이 포함" 지시를 추가(완전성 편차 완화 시도). `payout_amount` 다중 항목 한계와 이미지 마스킹의 OCR 파괴형 유출 한계는 위 표에 각각 명시 — 이번 라운드에서 코드로 고치지 않고 known limitation으로만 기록.
>
> **2026-08-04 (additive):** `report_worker`(#11, 미머지) 코드 리뷰 결과 `entities.icd`/`entities.admission_days`/`entities.surgery`를 참조하고 있으나 `ocr_worker`가 그 키를 만든 적이 없어(항상 미스매치) 실질적으로 죽은 경로였음을 확인. `DocType.DIAGNOSIS`에 `icd`(KCD 코드, 없으면 null — `diagnosis_name`과 달리 한글 병명 폴백 없음), `DocType.HOSPITALIZATION_CERT`에 `admission_days`(입원~퇴원 일수, 직접 표기·날짜쌍 계산 순으로 시도)·`surgery`(수술명 라벨 존재·값 기반 불리언, 라벨 자체가 없으면 null) 추가로 계약 정렬. 기존 `diagnosis_name`/`insurer`/`product`/`payout_amount`는 report_worker가 소비하지 않는 것으로 확인돼(user_insurances/user_claims 테이블에서 별도 조회) 스키마 변경 없이 `ocr_quality` 판정 전용으로 남긴다 — doc_type별로 "report_worker 소비 필드"와 "quality 판정 전용 필드"가 분리된 상태(위 표·예시에 구분 명시).
>
> **2026-08-04 (fix):** `masked_lines` 생성 로직(`build_masked_lines`)이 "이 라인이 PII인가" 판정(페이지 단위)과 "실제 치환"(라인 단위 재검출)에 서로 다른 탐지를 쓰던 불일치를 수정. 라벨과 값이 다른 줄로 쪼개진 경우, 값만 있는 라인은 라벨 컨텍스트 없이 재검출하면 정규식 앵커가 안 걸려 원문이 그대로 남을 수 있었다 — 코드리뷰로 지적됨, 실제 테스트가 이 케이스를 놓치고 있었음도 같이 확인(재검출 없이도 우연히 통과하는 페이크로 짜여 있었음). 이제 페이지에서 검출한 스팬을 `line_local_spans`로 라인별 로컬 오프셋으로 잘라 재검출 없이 `apply_mask`로 직접 치환한다 — `build_masked_lines`의 `mask` 매개변수 제거(더 이상 필요 없음), `detect`만 받음(브레이킹, 내부 함수라 `ocr_worker.pipeline` 호출부만 갱신하면 됨).
>
> **2026-08-04 (additive, 실측 기반):** 이미지 마스킹의 "VLM은 좌표를 안 준다"는 가정을 재검증 — `qwen3-vl:8b-instruct`에 PII 위치를 정규화(0~1000) 좌표로 직접 물으면(픽셀 좌표로 물으면 실패, 반드시 정규화 좌표) 실제로 정확한 bbox를 준다는 걸 실사진 10건 + surya 오독 재현 케이스(이름→"828")로 확인했다. `vlm_client.ground_pii()` 신설(정규화 응답을 픽셀로 변환, 실패 시 예외 없이 빈 리스트), `ImageMasker.ground_pages()`(신뢰도 게이팅, `_LOW_CONFIDENCE_THRESHOLD` 재사용)·`redact_pages(grounded_boxes=...)`(라인 기반 검은블록에 추가, 대체 아님) 신설. `masked_image_s3_keys` 이미지 트랙에만 적용 — `masked_text`/`masked_lines`(DB 텍스트)는 이 변경과 무관(불변).
>
> **2026-08-04 (fix):** 바로 위 `table_markdown` 제외 수정(2026-08-04 additive, `entities` 행)이 `MEDICAL_RECEIPT`에 회귀를 만들었음을 확인·수정. `MEDICAL_RECEIPT`는 `extract()`가 애초에 doc_type 고유 필드를 정의하지 않는 유형(항목별 데이터는 전부 `table_markdown`에만 담김)이라, `table_markdown`을 일괄 제외하면 확인할 필드 자체가 없어 신뢰도가 낮을 때 이름 유무·VLM 성공 여부와 무관하게 **항상** `needs_reupload`가 됐다. `_missing_domain_info(doc_type, entities)`로 시그니처를 바꿔, `MEDICAL_RECEIPT`(`_TABLE_MARKDOWN_ONLY_DOC_TYPES`)는 `table_markdown` 유무를 그대로 신호로 쓰고 나머지 doc_type은 기존대로 엄격히(테이블마크다운 제외) 판정한다.
>
> **2026-11-05 (internal, 계약 형태 불변):** OCR VLM을 Ollama Qwen(`qwen3.6:35b-a3b` 표 전사·`qwen3-vl:8b-instruct` grounding)에서 클러스터 vLLM의 PaddleOCR-VL-1.6(`PaddleOCR-VL-1.6-0.9B`, K8s-manifests `charts/etl-vllm`, brbs-etl T4)으로 바꿨다. 호출은 OpenAI 호환 `/v1/chat/completions`, 태스크 토큰 `"OCR:"`다. surya는 그대로 엔진이고 신뢰도 기준이다(PaddleOCR-VL은 좌표·신뢰도를 주지 않는다). 쪽 전사(하이브리드 VLM 경로)는 쪽 전체 `"OCR:"`로 받는다. 응답의 HTML·OTSL 표는 pipe markdown으로 바꾸므로 `masked_text`·`entities.table_markdown`의 형태(마크다운)는 그대로다. 잘림(`finish_reason=length`)·반복 루프 응답은 버리고 그 쪽만 surya로 폴백한다. grounding은 위 `masked_lines` 행의 "저신뢰 라인 재판독"으로 대체했다. `VLM_GROUNDING_MODEL`·`VLM_NUM_CTX`는 없어졌다. 알려진 한계: 공식 문서는 쪽 전체 직접 호출을 비권장한다(약 1MP로 축소 — 작은 글씨 손실 가능, 후속 후보: 레이아웃+영역 crop). VLM 엔티티 우선 규칙의 근거는 Qwen 시절 실측이라 재평가가 필요하다.

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
- `draft.metrics`(2026-11-11 추가, 선택 키): 리포트 생성 과정의 기술 수치(입력 품질·검색 근거·검증·처리 경로·신뢰도 등급과 사유). 구조는 `src/report_worker/README.md`의 "리포트 수치" 절이 단일 출처다. 계산에 실패하면 키가 없을 수 있다 — 읽는 쪽은 없음을 허용해야 한다. PII 없음: 건수·분류값·조항 번호·노드명과 소요 ms만 담는다. 예외 하나는 `verification.disability.reclassified[].injury`(LLM이 붙인 손상명 — 같은 값이 이미 `draft.disability`에 있다, 건강정보이며 식별자 아님).
- `core.reports.confidence_level`(backend 소유 컬럼, varchar(10))에 `HIGH`·`MEDIUM`·`LOW` 중 하나를 쓴다(`draft.metrics.confidence.level`과 같은 값). backend가 고객 상세·검수 화면에 내려주고 프론트가 추정 보상 범위의 신뢰도 게이지로 보여준다. 쓰려면 `ai_owner`(prod는 `ai_owner_prod`)에 이 컬럼 UPDATE 권한이 있어야 한다(`deploy/schema_split*.sql`). 권한이 없으면 이 값만 건너뛰고 리포트는 저장된다.

---

## 4-1. `report_job_failures` (DB 교차 계약 — 리포트 실패 저널)
**report_worker → `ai.report_job_failures`(012) → Spring 읽기 전용(`app_owner` SELECT).** 재전달 한도(`SQS_MAX_RECEIVE_COUNT`, report 운영값 3)를 다 써 poison으로 걷힌 리포트를 FAILED로 표시하는 근거다. 실패한 리포트는 초안을 저장하지 않으므로(2026-11-01) 이 행이 없으면 Spring은 영원히 PROCESSING으로 본다.

| 컬럼 | 의미 |
|---|---|
| `report_id` (uuid, UNIQUE) | 조인 키 = `core.reports.id`. 역직렬화 실패면 NULL(조인 불가 — 해당 리포트는 PROCESSING에 남는 알려진 한계) |
| `failure_class` | `context_unavailable` · `llm_unavailable` · `persist_failed` · `schema_invalid` · `unknown` |
| `error_type` | 예외 클래스명만(메시지 문자열 저장 금지 §9). **사용자 화면 노출 금지** |
| `attempts`, `first_failed_at`, `last_failed_at` | 운영 조회용 |
| `terminal` | `true` = 확정 실패(사용자 노출 기준). false→true 단방향 |

**Spring 판정 순서(순서가 계약)**: ① `reports.status='BLOCKED'` → BLOCKED(입력 차단·**LLM 거절** 포함) ② `NEEDS_REUPLOAD` ③ `applicable_guarantees IS NOT NULL` → COMPLETED ④ `ai.ocr_job_failures` terminal → FAILED(기존) ⑤ **`ai.report_job_failures`에 `report_id = reports.id AND terminal` → FAILED(신규)** ⑥ 그 외 PROCESSING.
- 종결 상태(①~③)를 저널보다 먼저 본다 — 경합으로 성공한 리포트에 terminal 행이 남을 수 있다.
- terminal 행은 사라질 수 있다(같은 report_id의 다른 메시지가 성공하면 워커가 지움 = 회복). FAILED를 상태로 저장·캐시하지 말고 조회 시점에 파생한다.
- `terminal=false`(재전달 중) 행과 `report_id IS NULL` 행은 무시한다. 거절은 저널에 남지 않는다(BLOCKED로 보인다).
- 상세·조회 SQL 예: `_workspace/16_agent_report_failure_journal.md` §4.

---

## 5. RAG 모듈 함수 인터페이스
`src/rag` — `report_worker`가 **함수 호출**. 순수 함수형(부수효과 없음).

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
`src/guardrail` — 단계별 함수로 분리(호출자가 필요한 것만 조립). 리포트=3단계 전부.

```python
async def guard_input(
    text: str,                          # 마스킹 대상(리포트: OCR 문서 본문)
    *,
    question: str | None,               # 질문(reports.question) — 키워드 검사
    narratives: Sequence[str | None],   # 사실 서술(사고 경위·추가 정보) — 서술용 검사
) -> InputGuardResult: ...              # question·narratives는 기본값 없는 필수 키워드 인자
def guard_generation(text: str) -> str: ...                       # 단정표현 치환·인용 강제 검사
async def guard_output(
    text: str,
    *,
    run_judge: bool = True,
    chunks: list[Chunk] | None = None,      # Judge 근거(약관·장해분류표·판례). 호출자가 고른다
    max_report_chars: int = JUDGE_MAX_REPORT_CHARS,  # Judge에 넣을 리포트 앞부분 글자 수
) -> OutputGuardResult: ...

class InputGuardResult(BaseModel):
    masked_text: str         # PII 마스킹(주민번호 앞 6자리만 보존)
    blocked: bool            # 도메인 외 질문 차단 여부
    reason: str | None       # 차단 사유

class OutputGuardResult(BaseModel):
    final_text: str          # 고지문 삽입됨
    judge_failures: list[str]  # 인용 검증 실패 섹션(리포트만, run_judge=True)
    judge_checked_chars: int = 0    # Judge가 실제로 본 리포트 글자 수(미수행·호출 실패면 0)
    judge_total_chars: int = 0      # final_text 전체 글자 수 → 검증 범위 = checked / total
    judge_evidence_chunks: int = 0  # Judge가 본 근거 청크 수(미수행·호출 실패면 0)
```
- `guard_input`은 마스킹을 `text`에, 보험·법률 외 주제 검사를 사용자가 쓴 글에만 건다. 둘 다 회사 표기(`주식회사`)는 빼고 찾는다.
  - `question`(질문): 키워드(`_OFF_DOMAIN`)가 있으면 막는다. 사유 `보험·법률 외 질문({키워드})`(형식 그대로).
  - `narratives`(사고 경위 `user_claims.description`·추가 정보 `additional_information`): 사실 서술이라 "요리하다가 데었다", "주식 매매 업무를 담당합니다"처럼 키워드가 정상적으로 들어간다. 그래서 키워드만으로는 막지 않는다. 세 조건이 다 맞을 때만 막는다: 도메인 밖 용건 표현("코인 시세", "주식 투자", "요리 레시피" 등)이 있다, 묻거나 부탁하는 말("알려주세요", "궁금", "?" 등)이 있다, 부상·질병·재물 손해·보험·법률·직업 같은 사안 표시가 하나도 없다. 사유 `보험·법률 외 내용({키워드} {용건})`. 항목마다 따로 검사한다.
- 2026-11-14 변경(#103): 전에는 `text`(청구 문서를 이은 OCR 본문)에 키워드 검사를 걸었다. 그래서 증권에 찍힌 "○○보험 주식회사"의 "주식" 때문에 리포트가 BLOCKED로 끝났고, 정작 사용자가 쓴 글은 검사하지 않았다. `question`·`narratives`는 기본값 없는 키워드 인자다. 옛 한 인자 호출(`guard_input(text)`)은 검사를 조용히 잃지 않고 `TypeError`로 드러난다.
- `judge_*` 세 필드는 2026-11-11 추가(backward-compatible, 기본값 0). 빈 `judge_failures`가 "전부 검증해 통과"인지 "일부만 봤다"인지 구분하려고 둔다 — 리포트 `draft.metrics.verification.judge`의 근거.
- Judge 범위: 리포트(고지문 포함) 앞 `max_report_chars`자(기본 `JUDGE_MAX_REPORT_CHARS` 12,000)와 `chunks` 앞 `JUDGE_MAX_EVIDENCE_CHUNKS`(20)건 × `JUDGE_EVIDENCE_CHARS`(800)자. 근거 순서는 호출자가 정한다(리포트는 약관 → 장해분류표 → 판례). `max_report_chars < 1`이면 `ValueError`(호출자 버그).
- `judge_failures`에 `JUDGE_UNAVAILABLE` 표식 하나만 들어 있으면 Judge가 판정하지 못한 것이다 — 호출 실패(`AiClientError`, 거절 포함) **또는 판정 불가 응답**(2026-11-11부터: 파싱 실패 `{}`, `failures`가 없거나 문자열 리스트가 아님). 이때 `judge_checked_chars`·`judge_evidence_chunks`는 0이다. 예외를 올리지 않는다(고지문 붙은 본문이 저장돼야 하므로).
- `run_judge`: 리포트=True(LLM Judge로 인용↔원문 검증). 코드 기본값도 True다.
- PII 마스킹 규칙은 `ocr_worker` 입력단과 **동일 정의** 공유.

---

## 7. `ai_client` 인터페이스
`src/core/ai_client.py` — provider를 env로 고른다. 챗: `LLM_PROVIDER=anthropic`(기본, Claude 공식 SDK) | `openai_compat`(Ollama/vLLM). 임베딩: `EMBEDDING_PROVIDER=voyage`(기본, voyage-4-large — 코퍼스도 같은 모델로 적재) | `openai_compat`(Ollama 등). 외부 API에는 마스킹된 텍스트만 보낸다(CODE_CONVENTIONS §13). 모델·엔드포인트·키는 config 주입. **anthropic 경로는 `chat()`의 `**opts`로 `model`만 받는다** — 샘플링 파라미터(temperature 등)는 `TypeError`(Opus 5.5에서 400).

```python
async def chat(messages: list[dict[str, str]], **opts) -> str: ...   # 완성 응답 1회(anthropic은 내부 스트리밍)
async def chat_json(messages: list[dict[str, str]], **opts) -> Any: ...  # JSON 파싱(실패 시 {})
async def close_client() -> None: ...                                # 앱 종료 시 1회
# 예외: AiClientError ⊃ EmbeddingDimensionError, LlmRefusalError(Claude 거절 — 폴백 모델까지 거절)

type EmbedInputType = Literal["query", "document"]  # voyage만 요청에 실음(openai_compat은 무시)
async def embed(text: str, *, input_type: EmbedInputType = "query") -> list[float]: ...
    # len == 1024 보장. 검색 쿼리=query(기본), 코퍼스 적재=document
async def embed_batch(
    texts: list[str], *, input_type: EmbedInputType = "document"
) -> list[list[float]]: ...
    # 입력 순서대로 반환, 각 len == 1024. 요청당 32개씩 순차 호출, 빈 입력이면 호출 없이 []
```

---

## 변경 정책
- 계약 변경은 **추가(backward-compatible) 우선**. 필드 삭제·타입 변경은 발행자·소비자 동시 배포 필요 → 사전 합의 필수.
- 변경 시: 이 문서 → `core/contracts.py` → 관련 테스트 순으로 갱신하고 PR에 영향 범위를 명시한다.
