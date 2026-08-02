# OCR Worker: 전체 파이프라인 재설계 — 저신뢰도 보완·VLM 환각 완화·재확인 필요 판정·이미지 마스킹 보강

## Context

이번 세션 동안 실제 GPU EC2 테스트(클린 PDF 8건 + 실제 폰카 사진 3건)로 다음을 확인하고 노션에 결정사항으로 정리했다:
- 실제 사용자 사진은 surya 신뢰도가 클린 PDF(0.951~0.988)보다 훨씬 낮다(0.782~0.861) — 표 문서가 아니어도 발생.
- surya가 완전히 무너지면(외국어 환각 등) VLM이 확실히 구제하지만, VLM도 완전히 믿을 순 없다 — 원본 자체가 안 보이는 부분은 VLM도 실패하고, 진단서 하나에서 완전 무관한 내용을 환각 생성한 사례도 있었다(qwen3-vl, 히알루론산 가격 지어냄).
- "재확인 필요(재업로드)" 판정은 문서 전체 신뢰도나 doc_type별 필수 필드 목록 대신, 기존 `extract.py`/마스커 코드를 재사용하는 절충안으로 결정했다.
- 이미지 검은블록 마스킹(B안)도 표 스크램블보다 "정보 블록이 라인별로 쪼개지는" 문제가 실측으로 확인됐다.

이번 라운드는 이 결정사항들을 **`ocr_worker` 범위 안에서** 구현한다. `report_worker`(재확인 필요 신호를 실제로 소비하는 쪽)는 이번 범위 밖 — `core/contracts.py`의 `ReportJob.ocr_quality` 필드까지만 추가하고 멈춘다.

**설계 중 새로 발견한 정정 사항**: `ImageMasker.redact_pages`(`image_masker.py`)는 `build_masked_lines`(DB 저장용, `repository.py`)와 완전히 별개 경로였다. 전자는 **페이지 전체 텍스트**(`OcrPage.text`, 전체 라인 `\n` 조인)에 대고 한 번에 탐지해서 스팬이 걸친 라인들을 역산해 가리고, 후자는 라인별로 독립적으로 마스킹한다. 즉 실제 이미지 검은블록은 이미 페이지 단위 컨텍스트를 쓰고 있어 걱정했던 것보다 낫다 — 진짜 약점은 앵커 정규식의 **고정 글자수 lookahead**가 여러 라인에 걸친 개행·글자수를 못 따라가는 것과, surya 라인 순서가 시각적 순서와 다를 수 있다는 것이다.

---

## 1. Kafka 아키텍처 (기존 동작 — 변경 없음, 문서화만)

`core/kafka/consumer.py` · `core/kafka/producer.py` · `src/ocr_worker/__main__.py` 기준.

- **토픽**: `ocr-job-queue`(소비) → `report-job`(발행). 상수는 `core/contracts.py`의 `OCR_JOB_TOPIC`/`REPORT_JOB_TOPIC`.
- **컨슈머**(`KafkaConsumer[OcrJob]`, aiokafka 래퍼): `group_id=settings.kafka_consumer_group`("ocr-worker"), `enable_auto_commit=False` — **핸들러 성공 후에만 수동 커밋**해 at-least-once를 보장한다(크래시 시 재전달 가능 → 그래서 `OcrPipeline.handle`이 `job_id` 멱등 조회로 재처리를 흡수한다).
- **재시도**: 핸들러 예외 시 인프로세스 재시도(`kafka_max_retries=3`, 지수 백오프 `2^attempt`, 최대 10초). 소진되면 DLQ(`{topic}.dlq`)로 raw bytes 그대로 보존(원본 유실 방지).
- **검증 실패**: pydantic 역직렬화 자체가 실패하면 재시도 없이 즉시 DLQ.
- **프로듀서**(`KafkaProducer`): `acks="all"` + `enable_idempotence=True`. `publish()`는 파티션 키로 `report_id`(=`ocr_result_id`에서 결정적 파생, `_derive_report_id`)를 써서 재발행해도 같은 키로 간다 — report_worker 쪽 멱등 처리와 맞물린다.
- **엔트리포인트**(`__main__.py`): `OcrPipeline.handle`을 컨슈머 핸들러로 배선.

---

## 2. 저신뢰도 VLM 보완 트리거 확대

`src/ocr_worker/pipeline.py`의 `_process()` — 현재 `analysis.doc_type in _TABLE_DOC_TYPES`만 보는 조건에 신뢰도 조건을 OR로 추가.

```python
# 표 문서와 별개로, surya 신뢰도가 낮으면(실측: 클린 PDF 0.951+ vs 실제 사진 0.861-)
# 문서 유형 무관하게 VLM 보완을 시도한다. 표본 3건 기준 시작값 — 운영 중 재조정.
_LOW_CONFIDENCE_THRESHOLD: float = 0.90

...
needs_vlm = (
    analysis.doc_type in _TABLE_DOC_TYPES or result.mean_confidence < _LOW_CONFIDENCE_THRESHOLD
)
if needs_vlm and images:
    ...
```

`result.mean_confidence`는 이미 `OcrResult`에 있는 프로퍼티라 추가 계산 불필요.

---

## 3. VLM 환각 완화

### 3.1 `src/ocr_worker/vlm_client.py`

**temperature 낮추기** — `transcribe_table`의 payload에 옵션 추가:
```python
payload: dict[str, Any] = {
    "model": settings.vlm_model,
    "prompt": VLM_TABLE_PROMPT,
    "images": [b64],
    "stream": False,
    "options": {"temperature": 0.0},
}
```

**프롬프트 보강** — 읽을 수 없으면 지어내지 말고 명시적으로 표시하도록:
```python
VLM_TABLE_PROMPT = (
    "이 이미지는 한국 보험/의료 문서입니다. 이미지 안의 모든 텍스트를 표 구조를 "
    "유지해 가능한 한 정확히 그대로 옮겨 적어주세요. 표는 마크다운 표로, "
    "항목-금액 쌍은 누락 없이 옮겨주세요. 추측하지 말고 보이는 대로만 적어주세요. "
    "특정 부분이 흐리거나 읽을 수 없으면 내용을 지어내지 말고 '[읽을 수 없음]'이라고만 적어주세요."
)
```

### 3.2 `src/ocr_worker/pipeline.py` — groundedness 체크(핵심)

VLM 결과와 surya 원문(`result.full_text`) 사이 토큰 중복률이 너무 낮으면 "지어냈다"고 보고 폐기한다. 새 모듈 수준 함수:

```python
import re
...
_TOKEN_RE = re.compile(r"[가-힣A-Za-z0-9]{2,}")
# VLM 토큰 중 surya 원문에도 나타나는 비율. 실측 보정 필요한 시작값 — surya가 오타를
# 내도 대부분의 토큰(날짜·숫자·구조어)은 겹치므로 낮게 잡아 정상 교정까지 걸리지 않게 한다.
_MIN_GROUNDED_OVERLAP = 0.3

def _looks_grounded(vlm_text: str, source_text: str) -> bool:
    """VLM 결과가 surya 원문과 무관한 내용을 지어낸 게 아닌지 대략적으로 검사한다."""
    vlm_tokens = set(_TOKEN_RE.findall(vlm_text))
    if not vlm_tokens:
        return True  # 빈 결과는 다른 경로(마스킹 검증 등)에서 처리
    source_tokens = set(_TOKEN_RE.findall(source_text))
    overlap = len(vlm_tokens & source_tokens) / len(vlm_tokens)
    return overlap >= _MIN_GROUNDED_OVERLAP
```

`_extract_table`을 반환값을 `(원문, 마스킹본) | None`으로 바꿔 groundedness 체크를 끼워 넣는다(원문은 §4의 이름-스팬 판정에도 재사용):

```python
async def _extract_table(self, image: PageImage, source_text: str) -> tuple[str, str] | None:
    """VLM으로 표를 전사하고 검증·마스킹한다. 실패하면 None(surya 결과로 폴백)."""
    try:
        raw = await self._vlm_transcribe(image)
    except VlmClientError:
        logger.warning("vlm_table_extraction_failed")
        return None
    if not _looks_grounded(raw, source_text):
        logger.warning("vlm_table_ungrounded")
        return None
    masked = await asyncio.to_thread(self._mask_table, raw)
    if masked is None:
        return None
    return raw, masked

def _mask_table(self, raw: str) -> str | None:
    """VLM 원문을 마스킹한다. 잔류 고민감 PII가 있으면 None(surya 결과로 폴백)."""
    masked = self._masker.mask(raw)
    try:
        assert_no_residual(masked)
    except MaskingError:
        logger.warning("vlm_table_masking_residual_pii")
        return None
    return masked
```

`assert_no_residual`은 PII 유출만 잡을 뿐 환각은 못 잡는다는 점을 groundedness 체크가 보완한다 — 별개 안전장치로 둘 다 필요.

---

## 4. "재확인 필요"(`needs_reupload`) 판정 + `ReportJob.ocr_quality`

### 4.1 판정 로직 — `src/ocr_worker/pipeline.py`

```python
from ocr_worker.masking.spans import PiiLabel
...

def _missing_domain_info(entities: dict[str, object]) -> bool:
    """extract() + (있다면) VLM table_markdown까지 합쳐 뽑힌 값이 하나도 없는지 확인."""
    if not entities:
        return True
    return all(v is None for v in entities.values())
```

`_process()`에서 VLM 결과 채택 이후, 최종적으로 쓰인 원문(surya 또는 VLM 원문)에 이름 스팬이 있는지 확인하고 품질을 판정한다:

```python
async def _process(self, job: OcrJob) -> None:
    result, images = await self._processor.process_with_images(job.s3_key, job.content_type)
    analysis = await asyncio.to_thread(self._analyze, result, job.doc_type_hint)

    masked_text = analysis.masked_text
    entities = analysis.entities
    quality_source_text = result.full_text  # 최종 품질 판정 기준 원문(기본 surya)

    needs_vlm = (
        analysis.doc_type in _TABLE_DOC_TYPES or result.mean_confidence < _LOW_CONFIDENCE_THRESHOLD
    )
    if needs_vlm and images:
        vlm_outcome = await self._extract_table(images[0], result.full_text)
        if vlm_outcome is not None:
            raw, masked = vlm_outcome
            masked_text = masked
            entities = {**entities, "table_markdown": masked}
            quality_source_text = raw  # VLM이 채택됐으면 품질 판정도 VLM 원문 기준

    has_name = any(
        span.label is PiiLabel.NAME for span in self._masker.detect(quality_source_text)
    )
    ocr_quality = (
        "needs_reupload"
        if result.mean_confidence < _LOW_CONFIDENCE_THRESHOLD
        and (not has_name or _missing_domain_info(entities))
        else "ok"
    )

    image_keys = await self._image_pipeline(job, result, images)

    record = OcrResultRecord(
        ...,
        ocr_quality=ocr_quality,
    )
    ocr_result_id = await save_ocr_result(self._pool, record)
    await self._publish_report(job, ocr_result_id, analysis.doc_type, ocr_quality)
```

**주의**: `quality_source_text`는 반드시 마스킹 *이전* 원문이어야 한다(마스킹 후 텍스트는 이름이 `[이름]` 토큰으로 치환돼 있어 이름 스팬 탐지가 항상 실패한다).

**알려진 한계**(노션에 이미 기록): `DIAGNOSIS`(`diagnosis_name` 1개)·`CLAIM`(`payout_amount` 1개)은 entities 필드가 하나뿐이라 `_missing_domain_info`에 취약하다 — 그 필드가 원래 없는 양식이면 오탐 가능. 치명적이진 않지만(재확인 요청은 반복될 수 있음, §4.3 폴백 참고) 알고 진행한다.

### 4.2 `ReportJob.ocr_quality` — `src/core/contracts.py`

```python
class ReportJob(BaseModel):
    ...
    ocr_quality: Literal["ok", "needs_reupload"] = "ok"  # 신규
```

`_publish_report`가 이 값을 받아 실어 발행하도록 시그니처 확장. 멱등 재발행 경로(`handle()`에서 `find_ocr_result`로 기존 행을 찾는 경우)도 저장된 `ocr_quality`를 다시 실어야 하므로 §4.3에서 DB 컬럼화한다.

### 4.3 DB 반영 — `ocr_results.ocr_quality` 컬럼 (새 마이그레이션)

`migrations/004_ocr_results_quality.sql`:
```sql
-- 004_ocr_results_quality.sql — ocr_results에 품질 판정 결과 컬럼 추가.
ALTER TABLE ocr_results ADD COLUMN IF NOT EXISTS ocr_quality text NOT NULL DEFAULT 'ok';

ALTER TABLE ocr_results DROP CONSTRAINT IF EXISTS ocr_results_ocr_quality_check;
ALTER TABLE ocr_results ADD CONSTRAINT ocr_results_ocr_quality_check
    CHECK (ocr_quality IN ('ok', 'needs_reupload'));
```

`src/ocr_worker/repository.py`:
- `OcrResultRecord`에 `ocr_quality: str = "ok"` 필드 추가.
- `_UPSERT_SQL`에 `ocr_quality` 컬럼 추가(INSERT 컬럼 목록·VALUES·`ON CONFLICT DO UPDATE SET`).
- `_SELECT_BY_JOB_SQL`을 `SELECT id, doc_type, ocr_quality FROM ocr_results WHERE job_id = $1`로 확장.
- `find_ocr_result` 반환 타입을 `tuple[str, DocType, str] | None`로 확장(멱등 재발행 시 저장된 품질을 그대로 재사용).

`pipeline.py` 조정:
```python
async def handle(self, job: OcrJob) -> None:
    ...
    existing = await find_ocr_result(self._pool, job.job_id)
    if existing is not None:
        ocr_result_id, doc_type, ocr_quality = existing
        await self._publish_report(job, ocr_result_id, doc_type, ocr_quality)
        return
    ...

async def _publish_report(
    self, job: OcrJob, ocr_result_id: str, doc_type: DocType, ocr_quality: str
) -> None:
    ...
    report = ReportJob(..., ocr_quality=ocr_quality, ...)
```

### 4.4 `.claude/docs/contracts.md` 갱신
- `report-job` 섹션에 `ocr_quality` 필드 설명 + enum(`ok`/`needs_reupload`) 추가.
- `ocr_results` 테이블 컬럼 표에 `ocr_quality` 행 추가.
- 기존 additive 체인지로그 패턴 따라 변경 이력 한 줄 추가.
- **"이 신호를 실제로 소비해 사용자에게 알리는 것은 report_worker + 게이트웨이 몫"**이라는 범위 경계도 명시(이번 라운드에서 안 건드림을 분명히).

---

## 5. 이미지 마스킹 보강(정정된 이해 반영)

### 5.1 문제의 정확한 위치

`ImageMasker.redact_pages`(`src/ocr_worker/masking/image_masker.py`)는 이미 `page.text`(전체 라인 `\n` 조인) 단위로 `detect()`를 호출하므로 페이지 컨텍스트를 쓰고 있다. 약점은 두 가지:
1. `page.lines`가 **surya 검출 순서**(시각적 위→아래 순서가 아닐 수 있음)로 조인된다.
2. 앵커 정규식의 lookahead가 **고정 글자수**라, 사이에 낀 라인들의 글자수+개행이 그 값을 넘으면 못 잡는다.

`build_masked_lines`(`src/ocr_worker/repository.py`)는 반대로 라인을 완전히 독립적으로 `mask(line.text)` 처리해 **컨텍스트가 아예 없다** — `ocr_results.masked_lines`(DB 저장, 향후 다른 소비자가 읽을 수 있는 JSON)도 같은 이유로 취약하다.

### 5.2 해결: 좌표 기반 리딩오더 정렬 + 두 경로 로직 통합

**`image_masker.py`에 리딩오더 정렬 헬퍼 추가**:
```python
def _reading_order(page: OcrPage) -> list[int]:
    """bbox(y0, x0) 기준으로 정렬한 라인 인덱스 순서를 반환한다.

    surya 검출 순서가 아니라 시각적 순서로 라인을 재배열해, 앵커 정규식의 lookahead가
    "물리적으로 가까운" 라벨-값 쌍을 더 잘 붙잡게 한다.
    """
    return sorted(range(len(page.lines)), key=lambda i: (page.lines[i].bbox[1], page.lines[i].bbox[0]))
```

`_line_char_ranges`와 `pii_line_indices`를 이 순서를 쓰도록 수정 — 정렬된 순서로 텍스트를 조인해 오프셋을 계산하고, 스팬을 다시 **원래 라인 인덱스**로 매핑한다(오프셋 계산 순서와 결과 인덱스가 어긋나지 않게 주의).

```python
def _reading_order_text(page: OcrPage, order: list[int]) -> str:
    return _LINE_SEPARATOR.join(page.lines[i].text for i in order)

def _line_char_ranges(page: OcrPage, order: list[int]) -> dict[int, tuple[int, int]]:
    """정렬된 순서로 조인한 텍스트에서, 원래 라인 인덱스별 [start, end) 오프셋."""
    ranges: dict[int, tuple[int, int]] = {}
    cursor = 0
    for i in order:
        start = cursor
        end = start + len(page.lines[i].text)
        ranges[i] = (start, end)
        cursor = end + len(_LINE_SEPARATOR)
    return ranges
```

`redact_pages`에서:
```python
order = _reading_order(page)
spans = self._detect(_reading_order_text(page, order))
indices = pii_line_indices(page, spans, order, self._labels)
```

`pii_line_indices`도 `ranges = _line_char_ranges(page, order)`를 쓰도록 수정(딕셔너리 순회로 변경).

**`repository.py`의 `build_masked_lines`도 같은 탐지·매핑을 재사용**하도록 변경 — 라인을 독립적으로 `mask()`하는 대신, `image_masker`의 `_reading_order`/`_line_char_ranges`/`pii_line_indices`를 import해서 **어느 라인이 PII에 걸리는지 판정을 공유**하고, 걸리는 라인은 전체 라인 텍스트를 `mask(line.text)`(이미 PII가 그 라인 자체에 있으면 정상 치환) 또는 그래도 전혀 안 걸리면 원문 그대로 둔다. 이렇게 하면 이미지 검은블록과 DB `masked_lines`가 **같은 판정 기준**을 공유해 둘 사이 불일치가 없어진다.

```python
def build_masked_lines(
    result: OcrResult, mask: Callable[[str], str], detect: DetectFn
) -> list[dict[str, object]]:
    lines: list[dict[str, object]] = []
    for page in result.pages:
        order = _reading_order(page)
        spans = detect(_reading_order_text(page, order))
        pii_indices = pii_line_indices(page, spans, order)
        for index, line in enumerate(page.lines):
            text = mask(line.text) if index in pii_indices else line.text
            lines.append({"masked_text": text, "bbox": list(line.bbox),
                          "polygon": [list(p) for p in line.polygon], "confidence": line.confidence})
    return lines
```

`build_masked_lines` 호출부(`pipeline.py`의 `_analyze`)에 `detect=self._masker.detect` 인자 추가.

### 5.3 `.claude/docs/contracts.md`의 "알려진 한계" 문구 정정

이전에 적은 "masked_lines는 surya 라인 bbox 기반이라 헤더-값 분리 시 놓칠 수 있다"는 문구를, 이번 개선(리딩오더 정렬 + 페이지 단위 탐지 공유) 반영해서 갱신한다. 여전히 남는 한계(아주 긴 문단이 사이에 낄 때 lookahead를 넘는 경우)는 명시하되, 문제 자체가 완화됐음을 반영.

---

## 6. 테스트

- `tests/test_pipeline.py`: 저신뢰도 트리거(표 문서 아니어도 confidence 낮으면 VLM 호출), groundedness 체크(무관한 VLM 결과 폐기 후 surya 폴백), `ocr_quality` 판정(이름 없음/도메인 정보 없음 각각 needs_reupload, 둘 다 있으면 ok, confidence 높으면 needs_reupload 안 됨) 케이스 추가.
- `tests/test_vlm_client.py`: temperature 옵션이 payload에 포함되는지 확인 케이스 추가.
- `tests/test_image_masker.py`(기존): `_reading_order`/정렬된 `pii_line_indices`가 뒤섞인 라인 순서에서도 올바른 원래 인덱스를 반환하는지 케이스 추가.
- `tests/test_masking.py` 또는 신규: `build_masked_lines`가 `detect` 인자를 받아 페이지 단위로 판정하는지, 독립 라인엔 없던 앵커 PII를 인접 라인 덕에 잡아내는지 케이스 추가.
- `tests/test_contracts.py`: `ReportJob.ocr_quality` 기본값·enum 검증 케이스 추가.
- 신규 `migrations/004_ocr_results_quality.sql` 적용 후 CHECK 제약 확인(수동, 로컬 PG).

## 검증 방법

1. `docker run --rm -v "$(pwd)":/app -w /app ghcr.io/astral-sh/uv:python3.12-bookworm-slim uv lock` — 새 의존성 없음, lock 무변경 확인.
2. `pytest tests/test_pipeline.py tests/test_vlm_client.py tests/test_image_masker.py tests/test_masking.py tests/test_contracts.py` — GPU·ollama 없이 CI에서 통과해야 함.
3. 마이그레이션 004 적용 후 `ocr_results_ocr_quality_check` 제약 확인.
4. (선택) GPU EC2에서 실제 저신뢰도 사진으로 `ocr_quality="needs_reupload"`가 정확히 찍히는지, groundedness 체크가 이전 환각 사례(히알루론산)를 실제로 걸러내는지 재현 확인.
