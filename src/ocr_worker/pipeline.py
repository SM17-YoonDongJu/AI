"""OCR 워커 파이프라인 (이슈 #15) — consume→OCR→분류→추출→마스킹→저장→produce.

``SqsConsumer``가 역직렬화·검증한 ``OcrJob`` 하나를 받아 전 과정을 오케스트레이션하고
``ReportJob``을 발행하는 **핸들러 본체**다. 순수 로직(분류·추출·마스킹)은 각 모듈에
있고, 여기서는 I/O 경계(S3·OCR·DB·SQS)를 잇는 오케스트레이션만 담당한다.

정식 at-least-once 규약(계획 · [[ocr-worker-build]]):
- **멱등(job_id)**: 진입부에서 이미 저장된 작업이면 무거운 OCR을 건너뛰고 기존
  ``ocr_result_id``로 ``ReportJob``만 재발행한다. 저장은 ``job_id`` 업서트라 중복
  소비가 새 행을 만들지 않는다. 다운스트림은 ``ocr_result_id``로 멱등 처리한다.
- **삭제=ack(발행 후)**: 컨슈머는 핸들러 성공 후에만 메시지를 삭제한다(SQS DeleteMessage).
  그래서 저장~발행 사이 crash 시 메시지가 visibility timeout 뒤 재전달되고, 재발행이
  안전해야 한다 → 위 멱등 단락 + 결정적 ``report_id``(``ocr_result_id`` 파생)로 완전 멱등을 만든다.
- **실패 격리(DLQ 미도입)**: 핸들러가 예외를 던지면 컨슈머가 삭제하지 않아 재전달되고,
  끝내 못 살리면 수신 횟수 상한(poison 가드)에서 스킵한다. 마스킹 잔류 검증 실패
  (``MaskingError``)도 예외로 전파해 **PII를 저장하지 않고** 격리한다(fail-closed).

실패 저널(``ai.ocr_job_failures``, 마이그레이션 008): 예전엔 위 격리가 **아무 기록도
남기지 않았다** — 사용자는 업로드가 조용히 증발하는 무음 실패를 겪었고, 운영은 로그를
뒤지는 것 말고 조회 수단이 없었다. 이제 ``handle``이 모든 실패를 저널에 남긴다:
- **결정적 실패**(``NonRetryableError`` — 마스킹 잔류·파일 디코드 실패): ``terminal=True``로
  기록한 뒤 원래 예외를 그대로 올린다. 컨슈머가 이 타입을 보고 **첫 시도에서 즉시 ack**
  하므로(``core.sqs.consumer``) 재전달 낭비 없이 사용자 신호가 바로 뜬다.
- **일시 실패**(그 외): ``terminal=False``로 기록하고 예외를 올려 재전달에 맡긴다.
  기록 자체가 실패해도 삼키고 경고만 남긴다 — 저널이 원래 예외를 가려선 안 된다.
- **회복**: 성공 경로(멱등 단락 포함) 끝에서 저널 행을 지운다. 남겨두면 이미 처리된
  작업이 계속 실패로 조회된다. 이 DELETE 실패는 작업의 성공을 뒤집지 않는다(경고만).
- 기록이 실패한 결정적 실패는 **재시도 가능한 예외로 바꿔** 던진다(아래 ``handle`` 참고).

청구 fan-in(``ai.claim_readiness``, 마이그레이션 009·010): 예전엔 문서 1건이 성공할
때마다 즉시 ``ReportJob``을 발행했다 — 문서 N건짜리 청구가 리포트 N건을 만들었다.
한 청구(claim)는 증권·진단서 등 여러 문서로 이루어지고 ``OcrJob``에 이미 ``claim_id``·
``report_id``(Spring이 청구 단위로 부여)·``doc_index``·``doc_total``이 실려 온다. 이제
문서가 **종결**될 때마다 청구 종결 카운터를 원자적으로 올리고(``advance_claim_progress``),
마지막 문서를 끝낸 워커만 필수 유형(증권+진단서) 충족을 판정해 청구당 1건을 발행한다.
- **종결 = 성공 + 확정 실패 + poison 소진**이다. 실패를 안 세면 문서 하나가 못 읽힌
  순간 그 청구의 리포트가 영영 나오지 않는다. 대신 "무엇이 실제로 인식됐는가"는
  카운터가 아니라 ``ai.ocr_results`` 재조회로 판정한다(성공한 문서만 행이 있다) —
  3건 중 2건 성공 + 1건 실패라도 그 2건이 필수 유형을 채우면 정상 발행된다.
- **필수 유형 미충족 → ``blocked``**(발행 없음). 증권·진단서는 업로드가 강제되므로,
  이 상태는 "안 올림"이 아니라 **"올렸는데 인식 못 함"**이다(``_REQUIRED_DOC_TYPES`` 참고).
- **단독 문서**(``claim_id``·``doc_total`` 없음)는 기존 문서별 즉시 발행 경로 그대로다.

fan-in의 정합성은 두 규칙에 걸려 있다(QA 실측으로 확정된 실패 모드의 반대편):
- **"이 문서가 끝났다"는 기록과 그 근거는 한 트랜잭션**이다. 저장(``ocr_results``)과
  종결 카운트, 확정 실패 저널(``ocr_job_failures``)과 종결 카운트를 각각 함께 커밋한다.
  갈라 두면 "저장은 됐는데 카운트가 안 된" 상태가 남는데, 재전달은 멱등 단락으로
  빠져 카운트를 재시도하지 않으므로 그 청구는 **영구 정지**한다.
- **종결 카운트는 문서별 멱등**이다(``job_id`` 집합, 마이그레이션 010). 같은 문서가
  두 번 종결로 보고돼도(가시성 타임아웃으로 인한 동시 중복 전달, ack 실패 후 재전달,
  poison 중복) 수가 늘지 않는다 — 늘면 아직 처리도 안 된 문서를 빼고 불완전한 리포트를
  조기 발행하게 된다.
판정·발행(SQS 왕복)은 트랜잭션 **밖**이라 그 사이에 죽을 수 있다. 그래서 "모든 문서
종결 + 여전히 ``pending``"을 멱등 단락이 판정 재개 신호로 읽는다(``_replay_processed``).

원본 삭제 게이트(#18 노션 5장): 비식별 사본이 S3에 안착하고 페이지별 검증(5.1 bbox
커버리지 + 5.2 재OCR 잔류)을 **모두** 통과해야 ``job.s3_key`` 원본을 지운다. 검증 실패는
예외가 아니라 "원본 보존 + 경고 로그"다 — 복구 가능한 방향으로만 실패시킨다.
- **삭제 시점 = ``ocr_results`` 저장 성공 이후**: 이미지 트랙은 삭제 가능 여부를 판정만
  하고(``ImageTrackResult``) 실행은 저장 뒤로 미룬다. 저장 전에 지우면 일시적 DB 오류로
  인한 재전달 재처리(SQS visibility timeout 후 재소비)가 원본을 다시 못 받아
  ``OcrError``로 승격되고, 재전달로 흡수 가능했던 오류가 복구 불가능한 원본 유실이 된다.
- **삭제 실행은 non-blocking**: 트리거 시점은 저장 이후로 고정하되 완료는 기다리지 않고
  백그라운드 task로 던진다(``_schedule_original_delete``). ``ReportJob`` 발행이 S3 왕복
  뒤로 밀릴 이유가 없다 — 삭제 성공 여부는 다운스트림 결과를 바꾸지 않고, 실패해도
  "원본이 남을 뿐"이다.
- **삭제 outbox(재시도)**: fire-and-forget 1회 시도만으로는 실패·crash 시 원본이 조용히
  남는다(재소비는 멱등 단락으로 재발행만 하므로 삭제를 다시 시도하지 않는다). 그래서
  삭제 대상 키와 시도 상태를 ``ocr_results``의 ``original_delete_*`` 컬럼에
  ``save_ocr_result``와 **같은 문(=같은 트랜잭션)**으로 남긴다 — 검증 통과분은
  ``pending``으로 커밋되므로, 저장 직후 crash가 나도 durable한 재시도 근거가 남고
  ``sweep_pending_deletes``가 주기적으로 이어받는다(최대 ``ocr_delete_max_attempts``회,
  ``ocr_delete_retry_interval_seconds`` 간격). 검증 실패분은 ``not_eligible``이라 스윕이
  절대 집지 않는다. 상한을 소진하면 ``exhausted``로 종결하고 S3 라이프사이클 정책이
  최종 백스톱이다(운영 개입 신호 = ``logger.error``, 판정은 UPDATE가 돌려준 갱신 후
  상태 기준 — 즉시 경로·스윕 경로가 같은 기준을 공유한다).
  - 저장 시 ``next_attempt_at``을 한 주기 뒤로 잡아 **첫 시도 창은 즉시 task가 독점**한다.
    NULL(즉시 due)로 두면 즉시 task가 S3 왕복 중인 사이 스윕이 같은 키를 중복 집행해
    시도 예산을 두 배로 태운다. 그래도 남는 좁은 경합(재시도 창 겹침)은 S3
    ``DeleteObject``가 멱등이라 무해하므로 막지 않는다.

블로킹 격리(CODE_CONVENTIONS §7): OCR·이미지 렌더는 ``ocr.py``가, PII 마스킹(정규식·
NER)과 이미지 검은블럭(PIL)은 이 모듈이 ``asyncio.to_thread``로 이벤트 루프에서 뗀다.

PII 규약(§13): OCR 원문(평문)은 이 핸들러 메모리에만 존재하고, ``ocr_results``·로그·
``report-job``에는 마스킹본·비-PII 값·식별자만 나간다.
"""

import asyncio
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import asyncpg

from core.config import Settings, get_settings
from core.contracts import DocType, OcrJob, ReportJob
from core.exceptions import NonRetryableError, OcrError, UnreadableFileError
from core.logging import bind_context, clear_context, get_logger
from core.sqs.producer import SqsProducer
from ocr_worker.classify import classify
from ocr_worker.extract import extract
from ocr_worker.masking.image_masker import ImageMasker, RedactedPage, image_to_png_bytes
from ocr_worker.masking.image_verify import CoverageFn, VerificationResult, verify_redacted_page
from ocr_worker.masking.masker import Masker, get_masker
from ocr_worker.masking.spans import PiiLabel
from ocr_worker.masking.verify import MaskingError, assert_no_residual
from ocr_worker.ocr import OcrProcessor, OcrResult, PageImage, get_processor
from ocr_worker.repository import (
    ClaimDocument,
    DeleteRetryState,
    OcrResultRecord,
    PendingDeletion,
    build_masked_lines,
    clear_job_failure,
    fetch_claim_documents,
    fetch_claim_readiness,
    fetch_due_deletions,
    find_ocr_result,
    mark_claim_blocked,
    mark_claim_published,
    record_delete_failure,
    record_delete_success,
    record_document_terminal,
    record_job_failure,
    save_ocr_result,
)
from ocr_worker.storage import delete_object, put_object
from ocr_worker.vlm_client import VlmClientError, ground_pii, transcribe_table

logger = get_logger(__name__)

# 결정적 report_id 파생용 네임스페이스(같은 ocr_result_id → 같은 report_id → 완전 멱등).
# 청구에 묶이지 않은 **단독 문서**(claim_id 없음) 경로에서만 쓴다 — 청구 경로는 Spring이
# 청구 전체에 이미 부여한 ``OcrJob.report_id``를 그대로 쓴다(_publish_claim_report).
_REPORT_ID_NAMESPACE = uuid.NAMESPACE_URL

# 청구 리포트를 만들기 위해 **반드시 인식돼 있어야** 하는 문서 유형.
# 보험증권·진단서는 업로드 자체가 필수라(게이트웨이가 강제) 청구의 모든 문서가 종결된
# 시점에도 이 유형이 안 잡혔다면, 그건 "사용자가 안 올렸다"가 아니라 **"올렸는데 인식을
# 못 했다"**는 뜻이다 — 분류가 다른 유형으로 새거나(OTHER 폴백) 마스킹 잔류·파일 디코드
# 실패로 저장 자체가 안 된 경우다. 그래서 사용자에게 필요한 후속 조치도 "다시 업로드"가
# 아니라 **"해당 문서를 다시 촬영"**이고, blocked 사유(missing_doc_types)는 그 안내 근거다.
#
# 알려진 트레이드오프(의도적, 2026-08 결정): 이 판정은 doc_type만 보고
# doc_type_confidence는 안 본다. classify()의 hint-only 승격(본문 근거 0점이어도
# doc_type_hint를 채택)이 만든 저신뢰 POLICY/DIAGNOSIS 분류도 여기선 "인식됨"으로
# 친다. 이건 hint 승격을 만든 이유(저화질 증권·진단서 사진이 OTHER로 폴백해 fan-in을
# 놓치는 것 방지)와 정확히 같은 방향이라 의도한 결과다 — 다만 반대급부로 "화질은
# 좋은데 완전히 엉뚱한 문서가 hint 하나로 POLICY/DIAGNOSIS 행세를 하는" 오탐은 여기서
# 못 막는다(ocr_quality도 텍스트가 선명하면 안 잡는다). "화질 나쁜 진짜 문서"가
# "화질 좋은 엉뚱한 문서"보다 훨씬 흔할 거라 보고 지금은 감수한다. 신뢰도를 반영해
# hint-only 승격을 여기서 걸러내려면 fetch_claim_documents가 doc_type_confidence도
# 읽어와야 한다(현재 미저장) — 필요해지면 별도 작업으로.
_REQUIRED_DOC_TYPES: frozenset[DocType] = frozenset({DocType.POLICY, DocType.DIAGNOSIS})
# 비식별 이미지 사본 키 공간 — 원본 키와 분리(원본은 별도 보존정책).
_MASKED_IMAGE_CONTENT_TYPE = "image/png"

# 하이브리드 VLM 보조 대상 — surya 라인 순서가 표 행/열 구조를 보존 못하는 다중
# 항목 표 문서 유형만. 단순 라벨-값 문서(진단서·증권)는 surya만으로 충분해 제외한다.
_TABLE_DOC_TYPES: frozenset[DocType] = frozenset(
    {DocType.PAYOUT_NOTICE, DocType.CLAIM, DocType.HOSPITALIZATION_CERT, DocType.MEDICAL_RECEIPT}
)

# 표 문서와 별개로, surya 신뢰도가 낮으면(실측: 클린 PDF 0.951+ vs 실제 폰카 사진
# 0.861-) 문서 유형 무관하게 VLM 보완을 시도한다. 표본 3건 기준 시작값 — 운영 중 재조정.
_LOW_CONFIDENCE_THRESHOLD: float = 0.90

# VLM 결과 groundedness(환각 여부 대략 검사)용 토큰 추출·임계값. surya가 오타를 내도
# 대부분의 토큰(날짜·숫자·구조어)은 겹치므로 낮게 잡아 정상 교정까지 걸리지 않게 한다.
_TOKEN_RE = re.compile(r"[가-힣A-Za-z0-9]{2,}")
_MIN_GROUNDED_OVERLAP = 0.3

# 다중 페이지 문서에서 일부 페이지만 VLM이 채택됐을 때 entities.table_markdown을
# 페이지별로 구분해 이어붙이는 구분자(사람이 읽기 쉽게). masked_text/quality_source_text
# 는 OcrResult.full_text와 같은 "\n" 접합을 그대로 써서 문서 형태를 일관되게 유지한다.
_TABLE_PAGE_SEPARATOR = "\n\n---\n\n"


def _looks_grounded(vlm_text: str, source_text: str) -> bool:
    """VLM 결과가 surya 원문과 무관한 내용을 지어낸 게 아닌지 대략적으로 검사한다.

    ``assert_no_residual``은 PII 유출만 잡고 환각(이미지에 없는 내용 생성)은 못 잡으므로
    별도 안전장치로 둔다. 완벽한 검증은 아니다 — 토큰 중복률 기반 근사치.
    """
    vlm_tokens = set(_TOKEN_RE.findall(vlm_text))
    if not vlm_tokens:
        # 빈 VLM 결과를 grounded로 처리하면 _extract_pages가 이 페이지를 "채택"으로
        # 표시하고, 검증된 surya 텍스트를 빈 문자열로 덮어써버린다(실제 유실).
        # 빈 결과는 실패로 취급해 surya 폴백을 타게 한다.
        return False
    source_tokens = set(_TOKEN_RE.findall(source_text))
    overlap = len(vlm_tokens & source_tokens) / len(vlm_tokens)
    return overlap >= _MIN_GROUNDED_OVERLAP


# extract()가 doc_type 고유 필드를 아예 정의하지 않는 유형 — 항목별 데이터가 전부
# table_markdown에만 담긴다(extract.py 참고). 이 유형은 table_markdown 유무 자체를
# 도메인 정보 신호로 쓴다 — 그 외엔 애초에 확인할 필드가 없다.
_TABLE_MARKDOWN_ONLY_DOC_TYPES: frozenset[DocType] = frozenset({DocType.MEDICAL_RECEIPT})


def _missing_domain_info(doc_type: DocType, entities: dict[str, object]) -> bool:
    """extract()로 뽑힌 doc_type 고유 엔티티가 하나도 없는지 확인한다.

    "재확인 필요" 판정 신호 중 하나. doc_type별 필수 필드 정책을 새로 만들지 않고
    이미 있는 추출 결과를 재사용한다 — 알려진 한계: entities 필드가 1개뿐인 CLAIM
    (payout_amount 하나)은 그 필드가 원래 없는 양식이어도 오탐할 수 있다. DIAGNOSIS는
    diagnosis_name/icd 두 필드 중 하나라도 있으면 통과하므로 상대적으로 덜 취약하다.

    ``_TABLE_MARKDOWN_ONLY_DOC_TYPES``(예: MEDICAL_RECEIPT)는 ``table_markdown``
    유무를 그대로 쓴다 — 실측 확인: 이 예외 없이 일괄 제외하면, extract()가 애초에
    고유 필드를 정의하지 않는 이 유형만 신뢰도가 낮을 때 이름 유무·VLM 성공 여부와
    무관하게 항상 재확인 필요로 판정되는 회귀가 있었다(확인할 필드 자체가 없어서
    남는 게 늘 빈 dict가 됨). 나머지 doc_type은 ``table_markdown``을 제외한다 — VLM이
    채택되면 페이지 전사 원문이 거의 항상 채워지므로, 이를 도메인 정보로 세면 실제
    doc_type 필드가 하나도 안 뽑힌 저신뢰도 문서도 표 문서라는 이유만으로 늘 통과해버린다.
    """
    if doc_type in _TABLE_MARKDOWN_ONLY_DOC_TYPES:
        return not entities
    domain_entities = {key: value for key, value in entities.items() if key != "table_markdown"}
    if not domain_entities:
        return True
    return all(v is None for v in domain_entities.values())


def _masked_image_key(job_id: str, page_index: int) -> str:
    """비식별 이미지 사본의 S3 키(페이지별). 원본과 분리된 키 공간을 쓴다."""
    return f"masked/{job_id}/page-{page_index}.png"


def _encode_pngs(redacted_pages: list[RedactedPage]) -> list[bytes]:
    """비식별 사본을 PNG 바이트로 인코딩한다(동기 — PIL, ``to_thread``용)."""
    return [image_to_png_bytes(page.image) for page in redacted_pages]


def _derive_report_id(ocr_result_id: str) -> str:
    """``ocr_result_id``에서 결정적 ``report_id``를 파생한다(재발행 시 동일 값)."""
    return str(uuid.uuid5(_REPORT_ID_NAMESPACE, f"report:{ocr_result_id}"))


@dataclass(frozen=True, slots=True)
class _ClaimContext:
    """fan-in에 필요한 값이 모두 갖춰진 청구 컨텍스트(``_claim_context`` 반환).

    ``report_id``가 ``str``이 아니라 ``uuid.UUID``인 게 요점이다 — 형식 검증을 진입
    시점에 한 번 끝내, 저장 이후 단계에서 변환 예외가 터져 fan-in이 영구 정지하는 경로를
    없앤다(``repository.record_document_terminal`` 참고).

    Attributes:
        claim_id: 청구 식별자.
        doc_total: 청구 총 문서 수(fan-in 완료 기준).
        report_id: Spring이 청구 전체에 부여한 ``REPORTS.id``(검증된 UUID).
    """

    claim_id: str
    doc_total: int
    report_id: uuid.UUID


def _claim_context(job: OcrJob) -> _ClaimContext | None:
    """청구 fan-in 대상이면 컨텍스트를, 단독 문서/계약 이상이면 ``None``을 돌려준다.

    fan-in이 성립하려면 셋이 다 필요하다: ``claim_id``(형제 문서를 묶는 키),
    ``doc_total``("몇 건이 모여야 끝인가"), 그리고 **UUID로 파싱되는** ``report_id``
    (청구 대표 리포트의 멱등 키이자 ``ai.claim_readiness.report_id`` NOT NULL 컬럼 값).

    이 판정을 모든 호출부(``_process``·멱등 단락·``advance_claim_progress``)가 **공유**
    하는 게 중요하다. 조건이 갈리면 첫 처리는 문서별로 발행해 놓고 재전달 때는 청구
    경로로 빠져 아무것도 재발행하지 않는, 경로마다 다른 동작이 생긴다.

    ``claim_id``는 있는데 나머지가 빠진 조합은 **발행측 계약 위반**이다(``OcrJob``에서
    ``report_id``는 필수, 청구 문서에는 ``doc_total``이 실려야 한다). 조용히 넘기지 않고
    경고를 남긴다 — 이 조합은 문서별 즉시 발행으로 폴백하는데, 그 경로의 ``report_id``는
    ``ocr_result_id``에서 파생한 값이라 report_worker가 ``REPORTS`` 행을 찾지 못해
    **아무 일도 일어나지 않을 수 있다**(크로스팀 확인 필요 — ``_publish_report`` 참고).
    """
    if job.claim_id is None:
        return None  # 청구에 묶이지 않은 단독 문서 — 정상 경로
    report_id = None if job.doc_total is None else _as_report_uuid(job.report_id)
    if job.doc_total is None or report_id is None:
        logger.warning(
            "claim_context_incomplete",
            claim_id=job.claim_id,
            has_doc_total=job.doc_total is not None,
            report_id_parsable=report_id is not None,
        )
        return None
    return _ClaimContext(claim_id=job.claim_id, doc_total=job.doc_total, report_id=report_id)


def _as_report_uuid(report_id: str) -> uuid.UUID | None:
    """``report_id``를 UUID로 파싱한다(형식 오류면 ``None`` — 값은 로그에 남기지 않는다)."""
    try:
        return uuid.UUID(report_id)
    except ValueError:
        return None


def _failure_class(exc: Exception) -> str:
    """결정적 실패 예외를 저널의 ``failure_class``로 매핑한다(마이그레이션 008의 CHECK 값).

    분류를 못 하면 ``'unknown'``으로 폴백한다 — 새 ``NonRetryableError`` 하위 타입이
    생겼을 때 CHECK 제약 위반으로 **기록 자체가 실패하는** 것보다, 덜 구체적인 분류로라도
    남기는 편이 낫다(error_type 컬럼에 클래스명이 남아 사후 분류는 가능하다).
    """
    if isinstance(exc, MaskingError):
        return "masking_residual"
    if isinstance(exc, UnreadableFileError):
        return "unreadable_file"
    return "unknown"


@dataclass(frozen=True, slots=True)
class ImageTrackResult:
    """이미지 마스킹 트랙 산출 — 비식별 사본 키 + 원본 삭제 가능 여부.

    트랙은 삭제 여부를 **판정만** 하고 실행하지 않는다. 실제 삭제는 ``ocr_results``
    저장이 성공한 뒤 ``_process``가 백그라운드 task로 띄운다 — 저장 전에 지우면 일시적
    DB 오류로 인한 재시도가 "원본이 없어 OCR 실패"로 승격돼(at-least-once 규약 위반)
    복구 가능한 오류가 복구 불가능한 원본 유실이 된다.

    Attributes:
        keys: 업로드한 비식별 사본의 S3 키(페이지 순서).
        delete_original: 페이지별 마스킹 검증을 전부 통과해 원본을 지워도 되는가.
    """

    keys: list[str]
    delete_original: bool


# 이미지 마스킹 트랙 주입점 — (job, OCR결과, 페이지 이미지) → 사본 키 + 삭제 가능 여부.
type ImagePipeline = Callable[[OcrJob, OcrResult, list[PageImage]], Awaitable[ImageTrackResult]]
# S3 업로드 주입점(테스트에서 페이크).
type UploadFn = Callable[[str, bytes, str], Awaitable[None]]
# S3 원본 삭제 주입점(테스트에서 페이크) — 되돌릴 수 없는 연산이라 게이트 뒤에서만 호출한다.
type DeleteFn = Callable[[str], Awaitable[None]]
# 하이브리드 VLM 표 전사 주입점(테스트에서 페이크로 대체).
type VlmTranscribe = Callable[[PageImage], Awaitable[str]]


@dataclass(frozen=True, slots=True)
class _Analysis:
    """CPU 바운드 분석(분류·추출·텍스트 마스킹) 산출 — 한 번의 스레드 호출로 묶는다."""

    doc_type: DocType
    doc_type_confidence: float
    entities: dict[str, object]
    masked_text: str
    masked_lines: list[dict[str, object]]


class OcrPipeline:
    """``OcrJob`` 한 건을 처리해 ``ReportJob``을 발행하는 파이프라인 핸들러.

    I/O 경계(OCR 프로세서·마스커·이미지 마스킹·업로드·원본 삭제·프로듀서·DB 풀)를 주입
    가능하게 두어, SQS·DB·GPU·PIL 없이 오케스트레이션을 단위 테스트한다(#20).
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        producer: SqsProducer,
        settings: Settings | None = None,
        processor: OcrProcessor | None = None,
        masker: Masker | None = None,
        image_masker: ImageMasker | None = None,
        image_pipeline: ImagePipeline | None = None,
        vlm_transcribe: VlmTranscribe | None = None,
        coverage: CoverageFn | None = None,
        upload: UploadFn | None = None,
        delete: DeleteFn | None = None,
    ) -> None:
        """파이프라인을 구성한다.

        Args:
            pool: asyncpg 연결 풀(``ocr_results`` 저장·조회).
            producer: ``ReportJob`` 발행용 SQS 프로듀서.
            settings: 환경설정. ``None``이면 ``get_settings()``.
            processor: OCR 오케스트레이터. ``None``이면 프로세스 공용 ``get_processor()``.
            masker: 텍스트 마스커. ``None``이면 프로세스 공용 ``get_masker()``.
            image_masker: 이미지 마스커. ``None``이면 텍스트 마스커의 검출을 공유하는
                기본 구성(PIL 렌더 + 저신뢰도 VLM grounding).
            image_pipeline: 이미지 마스킹 트랙. ``None``이면 기본(PIL 렌더 → S3 업로드 →
                마스킹 검증 → 통과 시 원본 삭제).
            vlm_transcribe: 하이브리드 VLM 표 전사 함수. ``None``이면
                ``vlm_client.transcribe_table``.
            coverage: 검은블럭 커버리지 측정 함수(5.1). ``None``이면 numpy 픽셀 측정
                (``image_verify.measure_coverage``) — 주입점을 둬 numpy 없이 게이트를 테스트한다.
            upload: 비식별 사본 S3 업로드 함수. ``None``이면 ``storage.put_object``.
            delete: 원본 S3 삭제 함수. ``None``이면 ``storage.delete_object``.
        """
        self._pool = pool
        self._producer = producer
        self._settings = settings or get_settings()
        self._processor = processor or get_processor()
        self._masker = masker or get_masker()
        self._image_masker = image_masker or ImageMasker(
            detect=self._masker.detect,
            ground=ground_pii,
            ground_confidence_threshold=_LOW_CONFIDENCE_THRESHOLD,
        )
        self._vlm_transcribe: VlmTranscribe = vlm_transcribe or transcribe_table
        self._coverage: CoverageFn | None = coverage
        self._upload: UploadFn = upload or put_object
        self._delete: DeleteFn = delete or delete_object
        self._image_pipeline: ImagePipeline = image_pipeline or self._default_image_pipeline
        # 진행 중인 원본 삭제 task의 강한 참조(fire-and-forget · GC 방지).
        # 자세한 이유는 ``_schedule_original_delete`` 참고.
        self._pending_deletes: set[asyncio.Task[None]] = set()

    async def handle(self, job: OcrJob) -> None:
        """OCR 작업 하나를 처리한다(컨슈머 핸들러 진입점).

        멱등: 이미 저장된 ``job_id``면 OCR을 건너뛰고 ``ReportJob``만 재발행한다.
        예외는 그대로 전파해 컨슈머의 재시도/종결 규약에 맡긴다(핸들러가 삼키지 않는다) —
        다만 전파 **전에** 실패 저널(``ai.ocr_job_failures``)에 흔적을 남긴다. 모듈
        docstring의 "실패 저널" 절이 세 경로(결정적·일시·회복)의 근거다.

        ``except`` 순서가 의미를 만든다: ``NonRetryableError``가 먼저다.
        ``UnreadableFileError``는 ``OcrError``이기도 해서 순서가 뒤집히면 확정 실패가
        ``terminal=False``로 기록되고, 컨슈머는 즉시 ack 대신 재전달을 반복한다.

        청구에 묶인 문서(``claim_id``+``doc_total``)면 발행이 여기서 바로 일어나지
        않는다 — 성공·확정 실패 **양쪽 모두** 이 문서의 종결을 청구 카운터에 반영하고
        (``advance_claim_progress``), 마지막 문서를 끝낸 호출만 청구 대표 ``ReportJob``
        을 낸다(모듈 docstring의 "청구 fan-in" 절).

        Args:
            job: 검증된 ``OcrJob``.

        Raises:
            RuntimeError: 결정적 실패를 저널에 기록하지 **못한** 경우. 원래 예외 대신
                재시도 가능한 이 예외를 던져 컨슈머가 메시지를 남기게 한다(아래
                ``_journal_terminal`` 참고).
            Exception: 그 외에는 처리 중 발생한 원래 예외를 그대로 전파한다.
        """
        bind_context(job_id=job.job_id, user_ref=job.user_ref)
        try:
            existing = await find_ocr_result(self._pool, job.job_id)
            if existing is not None:
                await self._replay_processed(job, existing)
            else:
                await self._process(job)
            # 회복 경로: 이전 시도가 남긴 일시 실패 행을 지운다. 멱등 단락도 "이 작업은
            # 끝났다"는 같은 결론이라 함께 정리한다 — 저장은 됐는데 발행 직전에 죽어
            # 실패로 기록된 작업이 재전달로 여기 도달하는 게 정확히 그 경우다.
            await self._clear_failure(job)
        except NonRetryableError as exc:
            # 저널 기록과 청구 종결 카운트는 **한 트랜잭션**이라 함께 커밋되거나 함께
            # 롤백된다(``_journal_terminal``). 실패한 문서도 세야 나머지가 멀쩡한 청구가
            # 영영 pending에 갇히지 않는다 — 무엇이 실제로 인식됐는지는 ocr_results
            # 재조회가 따로 판정하므로, 실패 문서를 세는 것이 판정을 왜곡하지 않는다.
            progress = await self._journal_terminal(job, exc)
            if progress is not None:
                # 판정·발행은 트랜잭션 **밖**에서 한다(SQS 왕복을 트랜잭션에 담지 않는다).
                # 여기서 죽어도 멱등 단락이 "다 모였는데 pending"을 보고 재개한다.
                claim, terminal_count = progress
                await self._judge_claim(job, claim, terminal_count)
            raise
        except Exception as exc:
            await self._journal_transient(job, exc)
            raise
        finally:
            clear_context()

    async def _replay_processed(self, job: OcrJob, existing: tuple[str, DocType, str]) -> None:
        """이미 저장된 문서가 재전달됐을 때의 처리(멱등 단락 본문).

        **여기서 청구 종결 카운터를 절대 다시 올리지 않는다.** 카운터는 이 문서가
        *처음* 종결됐을 때(``_process`` 끝 또는 결정적 실패 저널 직후) 딱 한 번
        오른다. 재전달로 여기 들어온 것은 "이미 세어진 문서를 다시 보는 것"일 뿐이라,
        한 번 더 세면 아직 문서가 남았는데도 fan-in이 완성돼 미완성 청구로 리포트가
        나가거나, 반대로 카운트가 doc_total을 넘겨 영영 발행 조건을 못 맞춘다.

        단독 문서(``claim_id`` 없음)는 기존대로 ``ReportJob``을 재발행한다. 청구 문서는
        진행 상태에 따라 넷으로 갈린다:
        - ``published``: 발행 후 ack 전에 죽었을 수 있다 → 같은 ``report_id``로 안전망
          재발행(다운스트림은 ``report_id`` 멱등이라 중복이 무해하다).
        - ``blocked``: ``published``와 같은 이유로 안전망 재발행이 필요하다 — 이제
          ``blocked``도 ``_publish_needs_reupload``로 ``ReportJob``을 낸다(마킹 후
          발행 순서라 마킹만 되고 발행 전에 죽는 경우가 있을 수 있다).
        - **모든 문서가 종결됐는데 아직 ``pending``**: 카운트까지는 커밋됐는데 그 뒤
          판정·발행 단계에서 죽었다는 뜻이다(예: ``producer.send`` 실패). 상태만 보고
          "아직 안 됐다"로 넘기면 아무도 다시 판정해 주지 않아 **영구 정지**한다 —
          카운터는 그대로 두고 판정만 재실행한다.
        - 그 외(문서가 남은 ``pending``, 행 없음): 낼 리포트 자체가 아직(또는 영영)
          없다 → 조용히 끝낸다.
        """
        ocr_result_id, doc_type, ocr_quality = existing
        logger.info("job already processed → republish", ocr_result_id=ocr_result_id)
        claim = _claim_context(job)
        if claim is None:
            await self._publish_report(job, ocr_result_id, doc_type, ocr_quality)
            return
        readiness = await fetch_claim_readiness(self._pool, claim.claim_id)
        if readiness is None:
            # 이 문서가 첫 종결이었는데 행이 없다 = 저장과 카운트가 갈라진 상태. 저장과
            # 카운트를 한 트랜잭션으로 묶은 뒤로는 발생할 수 없다(둘 다 없거나 둘 다 있다).
            logger.warning("claim_readiness_missing", claim_id=claim.claim_id)
            return
        if readiness.status == "published":
            docs = await fetch_claim_documents(self._pool, claim.claim_id)
            await self._publish_claim_report(job, docs)
            return
        if readiness.status == "blocked":
            docs = await fetch_claim_documents(self._pool, claim.claim_id)
            await self._publish_needs_reupload(job, claim, docs)
            return
        if readiness.status == "pending" and readiness.all_documents_terminal:
            logger.info(
                "claim_judgement_resumed",
                claim_id=claim.claim_id,
                docs_terminal=readiness.docs_terminal,
                doc_total=readiness.doc_total,
            )
            await self._judge_claim(job, claim, readiness.docs_terminal)
            return
        logger.info(
            "claim_report_not_due",
            claim_id=claim.claim_id,
            claim_status=readiness.status,
            docs_terminal=readiness.docs_terminal,
            doc_total=readiness.doc_total,
        )

    async def _journal_terminal(
        self, job: OcrJob, exc: Exception
    ) -> tuple[_ClaimContext, int] | None:
        """결정적 실패를 ``terminal=True``로 기록한다. **기록 실패 시 예외를 바꿔 던진다.**

        여기서만 저널 실패를 삼키지 않는 이유: 컨슈머는 ``NonRetryableError``를 보면
        메시지를 즉시 삭제한다. 기록이 실패한 채로 원래 예외를 올리면 메시지는 사라지고
        저널에도 아무것도 없다 — 사용자 관점에선 업로드가 조용히 증발하는 무음 실패이고,
        이건 이 기능이 없애려던 바로 그 상태다.

        그래서 재시도 가능한 ``RuntimeError``로 바꿔 던진다. 컨슈머는 이 타입을 삭제
        사유로 보지 않아 메시지가 재전달되고, 다음 시도에서 (결정적 실패는 어차피 같은
        지점에서 다시 나므로) 기록을 다시 노려볼 수 있다. 끝내 실패해도 수신 횟수 상한의
        poison 훅(``mark_failure_terminal``)이 백스톱이다.

        원래 예외는 유실되지 않는다 — 저널 예외가 직접 원인(``__cause__``)이고, 그 저널
        예외가 다시 원래 실패를 ``__context__``로 물고 있어 예외 체인에 둘 다 남는다.

        청구 문서면 종결 카운트를 **같은 트랜잭션에서** 함께 남긴다. 둘이 갈리면
        "저널엔 확정 실패로 찍혔는데 청구는 그 문서를 못 센" 상태가 되고, 그 문서는
        컨슈머가 즉시 ack해 사라지므로 카운트를 재시도할 기회가 영영 없다 — 형제 문서가
        모두 정상이어도 청구가 ``pending``에 갇힌다. 반대로 카운트만 커밋되고 저널이
        실패하면 무음 실패로 되돌아간다. 둘 다 아니거나 둘 다여야 한다.

        Returns:
            청구 문서면 ``(청구 컨텍스트, 갱신 후 종결 문서 수)``, 단독 문서면 ``None``.
            호출측이 이 값으로 트랜잭션 **밖에서** 판정·발행을 이어간다.
        """
        claim = _claim_context(job)
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                await record_job_failure(
                    conn,
                    job,
                    failure_class=_failure_class(exc),
                    error_type=type(exc).__name__,
                    terminal=True,
                )
                if claim is None:
                    return None
                terminal_count = await record_document_terminal(
                    conn,
                    claim_id=claim.claim_id,
                    job_id=job.job_id,
                    report_id=claim.report_id,
                    doc_total=claim.doc_total,
                )
        except Exception as journal_exc:
            # 예외는 타입만 남긴다(§9) — 원래 예외 타입도 함께 남겨야 무엇을 기록하려다
            # 실패했는지 로그만으로 추적된다.
            logger.warning(
                "ocr_job_failure_journal_failed",
                error_type=type(journal_exc).__name__,
                original_error_type=type(exc).__name__,
            )
            raise RuntimeError("실패 저널 기록 실패 — 재전달로 재시도") from journal_exc
        return claim, terminal_count

    async def _journal_transient(self, job: OcrJob, exc: Exception) -> None:
        """일시 실패를 ``terminal=False``로 기록한다(기록 실패는 삼킨다).

        여기서는 저널 실패를 삼킨다 — 원래 예외가 이미 재전달을 유발하므로(컨슈머가
        삭제하지 않는다) 기록은 다음 시도에서 다시 시도된다. 저널 예외를 올리면 원인
        예외를 가려 진단만 어려워진다.
        """
        try:
            await record_job_failure(
                self._pool,
                job,
                failure_class="ocr_error" if isinstance(exc, OcrError) else "unknown",
                error_type=type(exc).__name__,
                terminal=False,
            )
        except Exception as journal_exc:
            logger.warning(
                "ocr_job_failure_journal_failed",
                error_type=type(journal_exc).__name__,
                original_error_type=type(exc).__name__,
            )

    async def _clear_failure(self, job: OcrJob) -> None:
        """성공한 작업의 실패 저널 행을 지운다(실패해도 잡의 성공을 뒤집지 않는다).

        DELETE가 실패해도 이미 저장·발행은 끝났다. 예외를 올리면 성공한 작업이 실패로
        뒤집혀 재전달되고, 멱등 단락을 타며 같은 DELETE를 또 시도한다 — 남은 행 하나
        때문에 잡을 실패시키는 건 비용이 이득보다 크다. 남은 일시 실패 행은 다음 성공
        시도나 운영 정리가 걷어낸다.
        """
        try:
            await clear_job_failure(self._pool, job.job_id)
        except Exception as exc:
            logger.warning("ocr_job_failure_clear_failed", error_type=type(exc).__name__)

    async def _process(self, job: OcrJob) -> None:
        """신규 작업의 전체 처리 흐름(OCR→분석→하이브리드 VLM→품질 판정→저장→발행)."""
        result, images = await self._processor.process_with_images(job.s3_key, job.content_type)

        # 분류·추출·텍스트 마스킹은 CPU 바운드(정규식·NER) → 한 스레드 호출로 격리한다.
        # surya 기반 masked_text는 자체 게이트(assert_no_residual)를 통과한 안전한
        # 값이라, 아래 VLM 경로가 실패해도 항상 되돌아갈 수 있는 폴백으로 유지한다.
        analysis = await asyncio.to_thread(self._analyze, result, job.doc_type_hint)

        # 표 문서 유형(다중 항목) 또는 surya 신뢰도가 낮으면(문서 유형 무관) VLM으로
        # 보완한다. 페이지마다 독립적으로 채택 여부가 갈린다 — 한 페이지가 실패해도
        # 그 페이지만 surya 결과로 폴백하고 나머지 페이지는 영향받지 않는다(과거엔
        # 1페이지 VLM 성공만으로 문서 전체 masked_text가 교체돼 2페이지 이후가 통째로
        # 유실됐었다). masked_lines(DB 텍스트)는 항상 surya 기반 그대로 둔다 — 좌표가
        # surya 라인에 묶여 있어 VLM 채택 여부와 무관하다. 이미지 검은블록(_image_pipeline)
        # 은 별도로 VLM grounding을 호출해 surya 라인 기반 검출에 없는 영역까지 보강한다
        # (§ImageMasker.ground_pages, 저신뢰도일 때만 — needs_vlm과 동일 임계값 재사용).
        masked_text = analysis.masked_text
        entities = analysis.entities
        quality_source_text = result.full_text  # 최종 품질 판정 기준 원문(기본 surya)

        needs_vlm = (
            analysis.doc_type in _TABLE_DOC_TYPES
            or result.mean_confidence < _LOW_CONFIDENCE_THRESHOLD
        )
        if needs_vlm and images:
            vlm_outcome = await self._extract_pages(images, result)
            if vlm_outcome is not None:
                masked_text, quality_source_text, table_markdown = vlm_outcome
                # extract()는 surya 원문에만 돌았던 상태라, surya가 못 뽑았거나(None)
                # 잘못 뽑은 필드도 VLM이 더 깨끗하게 읽었으면 실제 값과 다를 수 있다.
                # VLM 채택 원문에 extract()를 한 번 더 돌려, 있으면 VLM 쪽 값을
                # 우선한다 — VLM은 이미 groundedness 검증을 통과했고 애초에 surya
                # 신뢰도가 낮거나 표 문서라서 호출된 것이므로, 둘 다 값이 있으면
                # VLM 쪽을 더 신뢰할 근거가 있다(실측: surya가 금액을 숫자로 오독해
                # 엉뚱한 값을 잘못 채운 사례 확인). surya만 값이 있으면 그대로 둔다.
                vlm_entities = extract(analysis.doc_type, quality_source_text)
                entities = {
                    key: vlm_entities.get(key) if vlm_entities.get(key) is not None else value
                    for key, value in entities.items()
                }
                entities["table_markdown"] = table_markdown

        # "재확인 필요" 판정: surya 신뢰도가 낮은데(문서 자체가 저품질) 사람 이름조차
        # 못 찾았거나 도메인 정보가 하나도 없으면, 자동 처리로는 신뢰할 수 없다고 본다.
        has_name = any(
            span.label is PiiLabel.NAME for span in self._masker.detect(quality_source_text)
        )
        ocr_quality = (
            "needs_reupload"
            if result.mean_confidence < _LOW_CONFIDENCE_THRESHOLD
            and (not has_name or _missing_domain_info(analysis.doc_type, entities))
            else "ok"
        )

        # 이미지 마스킹은 렌더한 페이지 이미지를 재사용한다(재다운로드/재렌더 없음).
        # 트랙은 "원본을 지워도 되는가"를 **판정만** 하고 삭제는 하지 않는다(아래 참고).
        track = await self._image_pipeline(job, result, images)

        record = OcrResultRecord(
            job_id=job.job_id,
            doc_type=analysis.doc_type,
            doc_type_confidence=analysis.doc_type_confidence,
            ocr_confidence=result.mean_confidence,
            masked_text=masked_text,
            masked_lines=analysis.masked_lines,
            entities=entities,
            masked_image_s3_keys=track.keys,
            ocr_quality=ocr_quality,
            # 삭제 outbox: 지울 키와 "지워도 되는가"를 저장과 같은 문으로 커밋한다.
            # 아래 즉시 삭제가 실패하거나 그 전에 crash가 나도 스윕이 이어받는다.
            original_s3_key=job.s3_key,
            original_delete_eligible=track.delete_original,
            # 청구 컨텍스트(마이그레이션 009) — fan-in이 claim_id로 형제 문서를 되짚는다.
            claim_id=job.claim_id,
            report_id=job.report_id,
            attachment_id=job.attachment_id,
            doc_index=job.doc_index,
            doc_total=job.doc_total,
        )
        # 저장과 청구 종결 카운트는 **한 트랜잭션**으로 묶는다. 갈라 두면 "저장은 커밋
        # 됐는데 카운트만 실패"가 생기는데, 그 상태는 스스로 낫지 않는다: 재전달돼도
        # 진입부 멱등 단락이 저장된 행을 보고 무거운 처리를 건너뛰므로 카운트를 다시
        # 시도할 지점이 없고, 형제 문서가 전부 정상 종결돼도 doc_total에 영영 못 닿아
        # 리포트가 나가지 않는다(QA 실측). 한 트랜잭션이면 저장이 실패할 때 아무것도
        # 남지 않아(재전달 시 _process 전체 재실행 — OCR 재수행 비용은 있지만 정확),
        # 저장이 성공하면 카운트도 반드시 함께 성공한다.
        claim = _claim_context(job)
        async with self._pool.acquire() as conn, conn.transaction():
            ocr_result_id = await save_ocr_result(
                conn,
                record,
                # 첫 스윕 인수 시점을 한 주기 미뤄, 아래 즉시 삭제 task가 S3 왕복 중일 때
                # 스윕이 같은 키를 중복 집행하지 않게 한다(attempts 예산 이중 소모 방지).
                delete_retry_interval_seconds=self._settings.ocr_delete_retry_interval_seconds,
            )
            terminal_count = (
                None
                if claim is None
                else await record_document_terminal(
                    conn,
                    claim_id=claim.claim_id,
                    job_id=job.job_id,
                    report_id=claim.report_id,
                    doc_total=claim.doc_total,
                )
            )

        # 아래는 전부 **커밋 이후**다. 원본 삭제(S3)·발행(SQS) 같은 외부 왕복을 트랜잭션
        # 안에 담으면 잠금을 네트워크 지연만큼 붙들게 되고, 롤백돼도 되돌릴 수 없다.
        # 원본 삭제는 저장 성공 이후에만 트리거한다 — 저장 전에 지우면 일시적 DB 오류로
        # 인한 재전달 재처리가 "원본이 없어 OCR 실패"로 승격돼 복구 불가가 된다.
        # 다만 완료는 기다리지 않는다 — 발행이 S3 왕복 뒤로 밀릴 이유가 없다.
        if track.delete_original:
            self._schedule_original_delete(job, ocr_result_id)
        # 청구에 묶인 문서면 곧바로 발행하지 않는다 — 청구의 **모든** 문서가 종결되고
        # 필수 유형이 다 인식됐을 때 청구당 1건만 나가야 한다(fan-in). 단독 문서는
        # 기존대로 문서별 즉시 발행(하위 호환).
        if claim is not None and terminal_count is not None:
            await self._judge_claim(job, claim, terminal_count)
        else:
            await self._publish_report(job, ocr_result_id, analysis.doc_type, ocr_quality)

    async def _extract_pages(
        self, images: list[PageImage], result: OcrResult
    ) -> tuple[str, str, str] | None:
        """페이지마다 독립적으로 VLM 보완을 시도해 문서 전체를 합성한다.

        한 페이지의 VLM 실패(호출 오류·환각·잔류PII)가 다른 페이지까지 막지 않는다.
        실패한 페이지는 그 페이지의 surya 결과로 폴백해 문서 전체 내용이 유실되지
        않게 한다. masked_text/원문은 ``OcrResult.full_text``와 같은 "\\n" 접합을
        써서 VLM 관여 여부와 무관하게 문서 형태가 일관되게 유지된다.

        Returns:
            어느 페이지든 VLM이 채택됐으면 ``(합성 masked_text, 합성 원문,
            table_markdown)``. 전 페이지가 실패했으면 ``None``(호출측이 순수
            surya 결과를 그대로 쓰게 한다).
        """
        masked_parts: list[str] = []
        raw_parts: list[str] = []
        table_parts: list[str] = []
        adopted = False

        for image, page in zip(images, result.pages, strict=True):
            outcome = await self._extract_table(image, page.text)
            if outcome is not None:
                raw, masked = outcome
                masked_parts.append(masked)
                raw_parts.append(raw)
                table_parts.append(masked)
                adopted = True
            else:
                masked_parts.append(await asyncio.to_thread(self._masker.mask, page.text))
                raw_parts.append(page.text)

        if not adopted:
            return None
        return (
            "\n".join(masked_parts),
            "\n".join(raw_parts),
            _TABLE_PAGE_SEPARATOR.join(table_parts),
        )

    async def _extract_table(self, image: PageImage, source_text: str) -> tuple[str, str] | None:
        """VLM으로 표를 전사하고 검증·마스킹한다.

        실패(호출 오류·환각·잔류PII)하면 ``None``을 반환해 surya 결과로 폴백한다.

        Returns:
            성공 시 ``(VLM 원문, 마스킹된 텍스트)``. 원문은 호출측의 품질 판정(이름
            스팬 확인)에도 재사용한다.
        """
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
        """VLM 원문을 마스킹한다. 잔류 고민감 PII가 있으면 ``None``(surya 결과로 폴백)."""
        masked = self._masker.mask(raw)
        try:
            assert_no_residual(masked)
        except MaskingError:
            logger.warning("vlm_table_masking_residual_pii")
            return None
        return masked

    def _analyze(self, result: OcrResult, hint: str | None) -> _Analysis:
        """분류·추출·텍스트 마스킹을 수행한다(순수/CPU — ``to_thread``에서 호출).

        마스킹 후 ``assert_no_residual``로 고민감 PII 잔류를 확인한다. 잔류 시
        ``MaskingError``를 던져 **PII를 저장하지 않고** DLQ로 보낸다(fail-closed).
        """
        classification = classify(result.first_page_text, hint)
        entities = extract(classification.doc_type, result.full_text)
        masked_text = self._masker.mask(result.full_text)
        assert_no_residual(masked_text)  # 고민감 PII 잔류 시 MaskingError(전파 → DLQ)
        masked_lines = build_masked_lines(result, self._masker.detect)
        return _Analysis(
            doc_type=classification.doc_type,
            doc_type_confidence=classification.confidence,
            entities=entities,
            masked_text=masked_text,
            masked_lines=masked_lines,
        )

    async def _default_image_pipeline(
        self, job: OcrJob, result: OcrResult, images: list[PageImage]
    ) -> ImageTrackResult:
        """PII 라인을 가린 비식별 페이지 사본을 S3에 올리고 키·삭제 가능 여부를 돌려준다.

        검은블럭 렌더·PNG 인코딩은 CPU/PIL 바운드 → ``to_thread``로 격리한다. 업로드는
        페이지별 독립 I/O라 ``gather``로 병렬화한다. 페이지가 없으면 빈 결과.

        업로드가 **끝난 뒤에야** 삭제 게이트(5.1+5.2 검증)를 판정한다 — 사본이 S3에
        안착하기 전에는 원본을 지울 근거가 없다. 판정만 하고 삭제는 호출측(저장 이후)이
        수행한다(``ImageTrackResult`` 참고).
        """
        if not images:
            return ImageTrackResult(keys=[], delete_original=False)
        # grounding은 VLM I/O라 async로 먼저 끝내고, PIL 렌더(CPU 바운드)만 to_thread로
        # 격리한다 — redact_pages 자체는 동기 함수로 유지해 블로킹 규약(§7)을 지킨다.
        grounded_boxes = await self._image_masker.ground_pages(images, result)
        redacted_pages = await asyncio.to_thread(
            self._image_masker.redact_pages, images, result, grounded_boxes
        )
        pngs = await asyncio.to_thread(_encode_pngs, redacted_pages)
        keys = [_masked_image_key(job.job_id, index) for index in range(len(pngs))]
        await asyncio.gather(
            *(
                self._upload(key, png, _MASKED_IMAGE_CONTENT_TYPE)
                for key, png in zip(keys, pngs, strict=True)
            )
        )
        logger.info("masked_images_uploaded", pages=len(keys))
        delete_original = await self._may_delete_original(result, redacted_pages)
        return ImageTrackResult(keys=keys, delete_original=delete_original)

    async def _may_delete_original(
        self, result: OcrResult, redacted_pages: list[RedactedPage]
    ) -> bool:
        """이미지 마스킹 검증(5.1+5.2)을 전 페이지가 통과했는지 판정한다(삭제는 안 한다).

        검증 실패는 **예외로 올리지 않는다**. 비식별 사본은 이미 업로드됐고 텍스트
        트랙은 자체 게이트(``assert_no_residual``)를 통과한 상태라, 여기서 DLQ로 보내면
        정상 처리된 작업 전체를 되돌리게 된다 — 검증이 걸러야 할 위험은 "PII 저장"이
        아니라 "복구 불가능한 원본 삭제"다. 그래서 실패의 결과는 "원본을 S3에 남기고
        경고 로그"이며, 이는 수동 검수·재처리로 복구 가능한 방향이다(반대는 불가).

        재OCR(5.2)은 GPU/CPU 바운드라 ``to_thread``로 격리한다(§7).

        Returns:
            전 페이지 통과 시 ``True``. 한 페이지라도 미달이면 ``False``(원본 보존).
        """
        if not result.pages or any(not page.lines for page in result.pages):
            # 라인이 0개인 페이지는 "가릴 것이 없다"와 "못 읽어서 못 가렸다"를 구분할 수
            # 없다(그 페이지의 사본 == 원본 픽셀). 문서 전체가 아니라 **페이지 단위**로
            # 본다 — 3페이지 중 1페이지만 못 읽은 혼합 문서에서 그 페이지가 미마스킹
            # 원본 그대로 남는데도 삭제가 통과되던 구멍을 막는다.
            logger.warning("original_delete_skipped", reason="page_without_ocr_text")
            return False

        verified = await asyncio.to_thread(self._verify_pages, result, redacted_pages)
        failures = [(index, outcome) for index, outcome in verified if outcome.needs_review]
        if failures:
            for index, outcome in failures:
                # 잔류 PII는 **분류와 건수만** 남긴다(원문 값 로깅 금지, §9).
                logger.warning(
                    "masked_image_verification_failed",
                    page=index,
                    low_coverage_lines=len(outcome.low_coverage_lines),
                    residual_pii=len(outcome.residual_pii),
                    residual_labels=sorted({span.label.value for span in outcome.residual_pii}),
                )
            logger.warning(
                "original_delete_skipped",
                reason="verification_failed",
                verified_pages=len(verified),
                failed_pages=len(failures),
            )
            return False
        logger.info(
            "masked_image_verified",
            pages=len(redacted_pages),
            verified_pages=len(verified),
        )
        return True

    def _schedule_original_delete(self, job: OcrJob, ocr_result_id: str) -> None:
        """원본 삭제를 백그라운드 task로 띄운다(fire-and-forget — 완료를 기다리지 않는다).

        트리거 시점은 여전히 ``ocr_results`` 저장 성공 이후지만, ``ReportJob`` 발행을
        S3 왕복 뒤로 미루지 않는다. 삭제 성공 여부는 다운스트림 결과를 바꾸지 않고,
        실패해도 outbox 행(``pending``)이 남아 스윕이 재시도한다.

        생성한 task는 ``self._pending_deletes``에 **강한 참조로** 보관한다. 이벤트 루프는
        실행 중인 task를 약한 참조로만 들고 있어서, ``asyncio.create_task``의 반환값을
        버리면 완료 전에 GC돼 삭제가 조용히 사라질 수 있다(asyncio의 알려진 함정).
        참조 해제·예외 흡수는 완료 콜백이 맡는다.

        로그 상관관계: task도 완료 콜백도 생성 시점의 contextvars **사본**을 들고 돌기
        때문에, ``handle``의 ``clear_context()``가 먼저 실행돼도 삭제 로그에 ``job_id``가
        그대로 붙는다(§9 상관관계 식별자). 그래서 별도 재바인딩을 하지 않는다.
        """
        task = asyncio.create_task(self._delete_original(job, ocr_result_id))
        self._pending_deletes.add(task)
        task.add_done_callback(self._on_original_delete_done)

    def _on_original_delete_done(self, task: asyncio.Task[None]) -> None:
        """삭제 task 완료 처리 — 참조 해제 + 미처리 예외 흡수.

        정상 경로의 성공/실패 로그(``original_deleted``·``original_delete_failed``)는
        ``_delete_original``이 이미 남긴다. 여기서 다루는 건 그 방어선을 빠져나온
        예기치 못한 예외뿐이다 — fire-and-forget이라 아무도 결과를 읽지 않으면 asyncio가
        "Task exception was never retrieved"만 뱉고 실패가 묻힌다. ``task.exception()``으로
        꺼내 경고를 남겨 그 두 가지를 모두 막는다.

        예외 **메시지는 남기지 않고 타입만** 남긴다 — 정형화되지 않은 예외의 문자열에
        무엇이 섞일지 보장할 수 없어 PII 규약(§9)상 안전한 쪽을 택한다.
        """
        self._pending_deletes.discard(task)
        if task.cancelled():
            logger.warning("original_delete_cancelled")
            return
        exc = task.exception()
        if exc is not None:
            logger.warning("original_delete_task_error", error_type=type(exc).__name__)

    async def wait_for_pending_deletes(self) -> None:
        """진행 중인 원본 삭제 task가 모두 끝날 때까지 기다린다(우아한 종료·테스트 훅).

        상한을 두려면 호출측에서 ``asyncio.timeout``으로 감싼다(``__main__`` 참고).
        그래도 안전한 이유: 내부가 ``gather``가 아니라 ``asyncio.wait``이라, 대기가
        취소돼도 **삭제 task 자체는 취소되지 않는다**(S3 호출을 중간에 끊을 이유가 없다).
        ``gather``였다면 취소가 자식 task로 전파돼 삭제를 중단시켰을 것이다. 단 이는 이
        메서드 범위의 얘기고, 프로세스가 정말 내려가면 루프 teardown이 결국 취소한다.

        예외는 완료 콜백이 이미 흡수하므로 여기서 전파되지 않는다.
        """
        while pending := tuple(self._pending_deletes):
            await asyncio.wait(pending)
            # 완료 콜백은 ``call_soon``으로 도는 터라 아직 안 돌았을 수 있다. 여기서 직접
            # 빼 다음 루프가 같은 task를 다시 기다리지 않게 한다(무한 루프 방지).
            self._pending_deletes.difference_update(pending)

    async def _delete_original(self, job: OcrJob, ocr_result_id: str) -> None:
        """검증을 통과한 S3 원본을 삭제하고 결과를 outbox에 기록한다.

        ``ocr_results`` 저장 성공 후에만 호출된다. 삭제 실패는 사본·저장 결과에 영향이
        없다. 여기서 예외를 올리면 이미 저장된 작업이 통째로 재전달·재처리되므로(재OCR
        비용 포함) 경고만 남긴다 — 실패한 원본은 outbox에 ``pending``으로 남아
        ``sweep_pending_deletes``가 재시도한다(백그라운드 task라 SQS 재전달이 이 실패를
        받아주지 못하므로, 재시도 경로는 outbox뿐이다).
        """
        try:
            await self._delete(job.s3_key)
        except OcrError as exc:
            # 기록을 **먼저** 하고 그 결과(갱신 후 실제 상태)로 로그 레벨을 정한다 —
            # 즉시 경로도 상한을 소진시킬 수 있다(예: max_attempts=1). 사전 계산으로는
            # 이 경우 스윕이 그 행을 다시 집지 못해(종결 상태) 운영 신호가 영영 안 뜬다.
            state = await self._mark_delete_failed(ocr_result_id)
            # OcrError 메시지는 s3_key만 담아 PII가 없다(storage.py).
            self._log_delete_failure(
                "original_delete_failed", state, s3_key=job.s3_key, error=str(exc)
            )
            return
        logger.info("original_deleted", s3_key=job.s3_key)
        await self._mark_deleted(ocr_result_id)

    @staticmethod
    def _log_delete_failure(event: str, state: DeleteRetryState | None, **fields: object) -> None:
        """삭제 실패를 남긴다 — ``exhausted``면 ``error``, 아니면 ``warning``.

        ``exhausted``는 자동 재시도가 끝났다는 뜻이고, 이후 마스킹 검증까지 통과한 PII
        원본이 S3에 남은 채 라이프사이클 만료를 기다리게 되므로 운영 개입 신호다.
        상태는 **DB가 돌려준 갱신 후 값**을 쓴다(앱 사전 계산 금지 — 즉시 경로·스윕
        경로가 같은 판정을 공유하고, 조회 시점 stale 값에 흔들리지 않게).
        outbox 갱신 자체가 실패해 상태를 모르면(``None``) 안전하게 warning으로 둔다 —
        그 경우 행은 아직 ``pending``이라 다음 스윕이 다시 시도하고, 정말 소진되면
        그때 error가 뜬다.
        """
        exhausted = state is not None and state.exhausted
        log = logger.error if exhausted else logger.warning
        attempts = state.attempts if state is not None else None
        log(event, exhausted=exhausted, attempts=attempts, **fields)

    async def _mark_deleted(self, ocr_result_id: str) -> None:
        """삭제 성공을 outbox에 반영한다(DB 오류는 흡수 — 아래 참고)."""
        try:
            await record_delete_success(self._pool, ocr_result_id)
        except Exception as exc:  # DB 오류가 삭제 로깅·워커 흐름을 죽이지 않게(§8)
            self._log_outbox_update_failed(ocr_result_id, deleted=True, exc=exc)

    async def _mark_delete_failed(self, ocr_result_id: str) -> DeleteRetryState | None:
        """삭제 실패를 outbox에 반영하고 갱신 후 상태를 돌려준다(DB 오류 시 ``None``).

        outbox 갱신은 "다음에 다시 시도할지"를 정할 뿐이라, 여기서 실패해도 이번 삭제의
        사실관계(로그)나 파이프라인 결과가 달라지지 않는다. 그래서 예외를 올리지 않는다:
        - 성공 기록 실패 → 행이 ``pending``으로 남아 스윕이 한 번 더 지운다(멱등, 무해).
        - 실패 기록 실패 → attempts가 안 늘어 스윕이 다음 주기에 즉시 다시 집는다.
        """
        try:
            return await record_delete_failure(
                self._pool,
                ocr_result_id,
                self._settings.ocr_delete_max_attempts,
                self._settings.ocr_delete_retry_interval_seconds,
            )
        except Exception as exc:  # DB 오류가 삭제 로깅·워커 흐름을 죽이지 않게(§8)
            self._log_outbox_update_failed(ocr_result_id, deleted=False, exc=exc)
            return None

    @staticmethod
    def _log_outbox_update_failed(ocr_result_id: str, *, deleted: bool, exc: Exception) -> None:
        """outbox 갱신 실패 경고. 예외는 **타입만** 남긴다(§9 — 메시지 내용 보장 불가)."""
        logger.warning(
            "original_delete_outbox_update_failed",
            ocr_result_id=ocr_result_id,
            deleted=deleted,
            error_type=type(exc).__name__,
        )

    async def sweep_pending_deletes(self, *, batch_size: int = 50) -> int:
        """outbox에 남은 원본 삭제를 재시도한다(워커 진입점의 주기 task가 호출).

        즉시 삭제 task가 실패했거나 그 전에 crash가 나 ``pending``으로 남은 행을 집어
        다시 지운다. ``not_eligible``(검증 실패)·종결 상태는 조회 자체가 제외한다.

        한 행의 실패가 나머지를 막지 않도록 행 단위로 격리한다 — 배치의 첫 키에서
        권한 오류가 나도 나머지 49건은 정상 처리돼야 한다.

        Args:
            batch_size: 한 사이클에 처리할 최대 건수.

        Returns:
            시도한 행 수(성공·실패 무관). 0이면 대기열이 비었다는 뜻이다.
        """
        rows = await fetch_due_deletions(self._pool, batch_size)
        for row in rows:
            try:
                await self._sweep_one(row)
            except Exception as exc:  # 개별 격리: 한 행의 예기치 못한 오류로 배치가 멈추지 않게
                logger.warning(
                    "sweep_delete_error",
                    ocr_result_id=row.id,
                    error_type=type(exc).__name__,
                )
        if rows:
            logger.info("original_delete_sweep", attempted=len(rows))
        return len(rows)

    async def _sweep_one(self, row: PendingDeletion) -> None:
        """outbox 1건을 재시도하고 결과를 기록한다(``sweep_pending_deletes`` 내부).

        즉시 삭제 경로와 **같은 헬퍼**로 기록·로깅한다 — ``exhausted`` 판정은 UPDATE가
        돌려준 갱신 후 상태를 그대로 쓴다. 조회 시점 ``row.original_delete_attempts``로
        미리 계산하면 조회~UPDATE 사이에 다른 주체(즉시 task·다른 워커)가 증가시켰을 때
        로그와 DB 상태가 어긋난다.
        """
        try:
            await self._delete(row.original_s3_key)
        except OcrError as exc:
            state = await self._mark_delete_failed(row.id)
            # s3_key·OcrError 메시지 모두 내부 식별자만 담는다(PII 없음, storage.py).
            self._log_delete_failure(
                "sweep_delete_failed",
                state,
                ocr_result_id=row.id,
                s3_key=row.original_s3_key,
                error=str(exc),
            )
            return
        logger.info("sweep_delete_succeeded", ocr_result_id=row.id, s3_key=row.original_s3_key)
        await self._mark_deleted(row.id)

    def _verify_pages(
        self, result: OcrResult, redacted_pages: list[RedactedPage]
    ) -> list[tuple[int, VerificationResult]]:
        """페이지별 5.1 커버리지 + 5.2 재OCR 잔류 검사(동기 — ``to_thread``에서 호출).

        재OCR은 ``OcrProcessor``가 이미 로드한 엔진을 재사용한다(모델 재로드 회피).

        가릴 것이 없던 페이지(``redacted``가 ``False``)는 검증 대상에서 뺀다 — 사본이
        곧 원본이라 커버리지를 잴 bbox가 아예 없고(5.1은 공집합 = 자동 통과), 5.2도
        방금 "PII 없음"이라고 판정한 그 이미지를 같은 검출기로 다시 읽는 셈이라 결론이
        바뀌지 않는다. 재OCR은 문서당 OCR 비용을 그대로 한 번 더 쓰는 가장 비싼
        단계라, 새 정보가 없는 페이지에는 돌리지 않는다.

        Returns:
            실제로 검증한 페이지의 ``(페이지 인덱스, 결과)`` 목록.
        """
        engine = self._processor.engine
        verified: list[tuple[int, VerificationResult]] = []
        for page, redacted in zip(result.pages, redacted_pages, strict=True):
            if not redacted.redacted:
                continue
            outcome = verify_redacted_page(
                redacted.image,
                page,
                redacted.line_indices,
                engine=engine,
                measure=self._coverage,
            )
            verified.append((page.index, outcome))
        return verified

    async def _publish_report(
        self, job: OcrJob, ocr_result_id: str, doc_type: DocType, ocr_quality: str
    ) -> None:
        """``ReportJob``을 ``report-job`` 큐에 발행한다(Standard 큐 — 파티션 키 없음).

        **단독 문서 전용 경로**다. 청구에 묶인 문서는 ``_publish_claim_report``가 청구당
        1건만 낸다.

        ``report_id``는 ``ocr_result_id``에서 결정적으로 파생해 재발행 시 동일하다 —
        report_worker가 ``report_id``/``ocr_result_id`` 어느 쪽으로 멱등 처리해도 안전하다.
        ``claim_id``는 가공 없이 패스스루한다(USER_CLAIMS는 report_worker가 직접 읽음).
        ``ocr_quality``가 ``needs_reupload``면 report_worker가 리포트 생성을 건너뛸 신호다
        (report_worker·게이트웨이 쪽 소비는 이번 범위 밖).

        **크로스팀 확인 필요(파생 report_id)**: report_worker가 결과를 쓸 때
        ``UPDATE reports ... WHERE id = report_id``로 Spring이 미리 만든 shell 행을
        갱신한다면, 여기서 파생한 ``report_id``는 그 행과 매칭되지 않아 0건 갱신으로
        조용히 끝난다. ``OcrJob.report_id``는 이제 **필수 필드**이므로 모든 발행이 그
        값을 쓰는 게 맞을 수 있는데, 그건 이 경로(문서별 멱등 재발행)의 의미를 바꾸는
        변경이라 report_worker 담당과 합의가 필요하다 — 이번 범위 밖으로 남긴다.
        """
        report_id = _derive_report_id(ocr_result_id)
        report = ReportJob(
            report_id=report_id,
            ocr_result_id=ocr_result_id,
            job_id=job.job_id,
            doc_type=doc_type,
            user_ref=job.user_ref,
            claim_id=job.claim_id,
            created_at=datetime.now(UTC),
            ocr_quality=ocr_quality,
        )
        await self._producer.send(self._settings.sqs_report_job_queue_url, report)

    async def advance_claim_progress(self, job: OcrJob) -> None:
        """이 문서의 **종결**을 청구 진행에 반영하고, 마지막이면 fan-in을 판정한다.

        공개 메서드인 이유: 파이프라인 내부(성공·결정적 실패)뿐 아니라 워커 진입점의
        poison 훅(``__main__._poison_journal``)도 "이 문서는 끝났다"를 알려야 한다 —
        재시도를 소진해 큐에서 걷힌 문서를 안 세면 그 청구가 영영 ``pending``에 갇힌다.

        **성공·실패를 구분하지 않고 똑같이 호출된다**는 게 이 설계의 핵심이다. 카운터는
        "몇 건이 끝났나"만 세고, "무엇이 인식됐나"는 ``ai.ocr_results``를 다시 조회해
        판정한다(성공한 문서만 행이 있다). 그래서 3건 중 2건 성공 + 1건 마스킹 잔류
        실패로 끝난 청구도, 그 2건이 필수 유형을 채우면 정상적으로 발행된다 — 마지막
        카운트를 채운 게 실패한 문서였다는 사실은 판정에 아무 영향이 없다.

        카운트는 **문서별 멱등**이라(``record_document_terminal``) 같은 문서로 여러 번
        불려도 안전하다 — poison 훅이 이미 세어진 문서를 다시 알려도 수가 늘지 않는다.

        Args:
            job: 방금 종결된 문서의 작업. 청구 컨텍스트가 없으면(단독 문서 또는 발행측
                계약 이상) 아무 일도 하지 않는다 — 그 경로는 문서별 즉시 발행을 그대로
                쓴다(하위 호환, ``_claim_context`` 참고).
        """
        claim = _claim_context(job)
        if claim is None:
            return  # 단독 문서 — fan-in 대상이 아니다
        terminal_count = await record_document_terminal(
            self._pool,
            claim_id=claim.claim_id,
            job_id=job.job_id,
            report_id=claim.report_id,
            doc_total=claim.doc_total,
        )
        await self._judge_claim(job, claim, terminal_count)

    async def _judge_claim(self, job: OcrJob, claim: _ClaimContext, terminal_count: int) -> None:
        """모든 문서가 종결됐으면 필수 유형 충족·문서 손실 여부를 판정해 발행한다.

        카운트를 올린 트랜잭션 **밖**에서 부른다 — 여기에는 SQS 왕복(발행)이 들어 있어
        트랜잭션에 담으면 네트워크 지연만큼 행 잠금을 붙들게 되고, 롤백돼도 이미 나간
        메시지를 되돌릴 수 없다. 그 대신 이 단계에서 죽으면 "모든 문서 종결 + 여전히
        ``pending``" 상태가 남고, 멱등 단락이 그걸 보고 판정을 재개한다
        (``_replay_processed``) — 재개 가능성이 트랜잭션 밖으로 뺀 대가를 상쇄한다.

        막히는 조건은 둘이다(합집합):
        - ``missing``: 필수 유형(보험증권·진단서)이 성공한 문서들 중에 없다. 오분류일
          수도, 그 유형의 문서가 아래 ``incomplete``로 실패한 것일 수도 있다 — 여기선
          구분하지 않는다(``fetch_claim_documents``는 doc_type만 보고, 실패 원인은
          ``ai.ocr_job_failures``가 별도로 갖고 있다).
        - ``incomplete``(``len(docs) < doc_total``): 업로드된 문서 수보다 성공한 문서
          수가 적다 — 필수·비필수 안 가리고, 하나라도 결정적 실패(또는 재시도 소진)로
          결과를 못 냈다는 뜻이다(``_process``/``_journal_terminal``/poison 훅 중
          하나가 이 문서의 종결을 카운트에 반영했는데 ``ai.ocr_results``엔 행이 없다 —
          그 셋 다 실패 저널에 ``attachment_id``를 남기므로 문서 특정은 항상 가능하다).

        둘 중 하나라도 걸리면 정상 리포트를 내는 대신 ``ocr_quality='needs_reupload'``로
        발행한다(``_publish_needs_reupload``) — report_worker의 기존 스킵·통지 로직
        (``reports.status='NEEDS_REUPLOAD'``)을 그대로 태운다. ``claim_readiness``에
        ``blocked``로 남기는 건 운영 조회용 기록이지 더 이상 "발행 안 함"을 뜻하지 않는다.

        멱등하다: 재개로 두 번 불려도 ``blocked``/``published``는 같은 값으로 다시
        찍히고, 발행은 같은 ``report_id``라 다운스트림이 중복을 흡수한다.
        """
        if terminal_count < claim.doc_total:
            return  # 아직 종결되지 않은 형제 문서가 있다 — 마지막 문서가 판정한다

        docs = await fetch_claim_documents(self._pool, claim.claim_id)
        missing = _REQUIRED_DOC_TYPES - {doc.doc_type for doc in docs}
        incomplete = len(docs) < claim.doc_total
        if missing or incomplete:
            missing_types = sorted(doc_type.value for doc_type in missing)
            # 발행 **전에** 도장을 찍는다 — published 경로와 같은 이유(아래 참고).
            await mark_claim_blocked(
                self._pool, claim_id=claim.claim_id, missing_doc_types=missing_types
            )
            # 운영·사용자 안내 신호: 필수 문서를 "안 올린" 게 아니라 **못 읽은** 상태거나
            # (missing), 업로드된 문서 중 일부가 결정적 실패로 사라진 상태다(incomplete).
            # 후속 조치는 둘 다 재촬영/재업로드다.
            logger.warning(
                "claim_blocked_needs_reupload",
                claim_id=claim.claim_id,
                missing=missing_types,
                incomplete=incomplete,
                recognized_docs=len(docs),
                doc_total=claim.doc_total,
            )
            await self._publish_needs_reupload(job, claim, docs)
            return

        # 발행 **전에** 도장을 찍는다 — 발행 후 crash로 ack를 못 하면 재전달되는데,
        # 그때 멱등 단락이 이 상태를 보고 같은 report_id로 안전망 재발행을 한다.
        # 순서가 뒤집히면(발행 후 기록) 발행 직후~기록 전에 죽었을 때 상태가 pending으로
        # 남아 재개 경로가 **한 번 더 발행**하게 되고, 기록이 끝내 실패하면 그 청구는
        # 재전달마다 리포트를 다시 낸다.
        await mark_claim_published(self._pool, claim.claim_id)
        await self._publish_claim_report(job, docs)

    async def _publish_needs_reupload(
        self, job: OcrJob, claim: _ClaimContext, docs: list[ClaimDocument]
    ) -> None:
        """청구를 막은 사실을 ``ocr_quality='needs_reupload'``로 report_worker에 알린다.

        report_worker의 ``mark_needs_reupload``(``reports.status='NEEDS_REUPLOAD'``)가
        이 경로의 실제 Backend 통지 지점이다. ocr_worker는 그 코드를 가져다 쓸 수
        없으므로(별도 컨테이너·의존성) 이미 배포된 그 경로를 ``ReportJob`` 발행으로
        재사용한다 — ``core.reports``를 여기서 직접 쓰지 않는다.

        대표 문서는 성공한 문서 중에서 고른다(``_publish_claim_report``와 같은 규칙).
        **성공한 문서가 하나도 없으면**(전부 실패) 고를 게 없으므로 ``job``(이 판정을
        트리거한, 방금 종결된 문서) 자체를 대표로 쓴다 — ``ocr_quality='needs_reupload'``
        경로는 report_worker가 ``ocr_result_id``/``doc_type`` 값을 아예 읽지 않으므로
        (즉시 스킵), 실재하지 않는 placeholder 값이어도 무해하다.
        """
        representative = next(
            (doc for doc in docs if doc.doc_type is DocType.POLICY),
            docs[0] if docs else None,
        )
        report = ReportJob(
            report_id=job.report_id,
            ocr_result_id=representative.ocr_result_id if representative else job.job_id,
            job_id=job.job_id,
            doc_type=representative.doc_type if representative else DocType.OTHER,
            user_ref=job.user_ref,
            claim_id=job.claim_id,
            created_at=datetime.now(UTC),
            ocr_quality="needs_reupload",
        )
        await self._producer.send(self._settings.sqs_report_job_queue_url, report)
        logger.info(
            "claim_needs_reupload_published",
            claim_id=claim.claim_id,
            report_id=job.report_id,
            docs=len(docs),
            doc_total=claim.doc_total,
        )

    async def _publish_claim_report(self, job: OcrJob, docs: list[ClaimDocument]) -> None:
        """청구 **전체**를 대표하는 ``ReportJob`` 1건을 발행한다.

        ``ReportJob``은 원래 문서 1건짜리 계약이라 ``ocr_result_id``·``doc_type``이 단일
        필수값이다. 청구 여러 문서를 대표하려면 값을 골라야 하고, 이건 계약을 아직 안
        바꾼 데서 오는 **임시방편**이다(정공법은 report_worker가 ``claim_id``로
        ``ai.ocr_results``를 직접 훑어 전체 문서를 보는 것 — 다른 워커 소관이라 별건):
        - 대표 문서: 보험증권(``POLICY``) 우선, 없으면 첫 문서(``doc_index`` 순).
          증권이 계약 조건·보장 범위의 기준 문서라 리포트의 앵커로 가장 적합하다.
        - ``ocr_quality``: **집계** — 문서 하나라도 ``needs_reupload``면 청구 전체를
          그렇게 본다. report_worker의 기존 스킵 로직이 청구 단위에서도 그대로 작동해야
          하고, 저품질 문서 하나가 리포트 전체의 신뢰도를 깎기 때문이다.
        - ``report_id``: ``job.report_id``(Spring이 청구 전체에 이미 부여한 ``REPORTS.id``)
          를 그대로 쓴다 — ``_derive_report_id``(문서별 파생)가 아니다. 재발행해도 같은
          값이라 다운스트림 멱등이 유지된다.
        - ``job_id``: fan-in을 완성시킨(=마지막으로 종결된) 문서의 작업 id. 청구 전체를
          가리키는 단일 값이 계약에 없어 추적용으로 그 문서를 남긴다.
        """
        if not docs:
            # 정상 경로에선 도달할 수 없다(문서가 하나도 없으면 필수 유형이 전부 빠져
            # blocked로 끝난다). 멱등 단락의 안전망 재발행이 데이터 정리 뒤에 들어오는
            # 등 예외적 경우에만 가능한데, 여기서 IndexError를 내면 무해한 재전달이
            # 실패 루프로 승격된다.
            logger.warning("claim_report_skipped", claim_id=job.claim_id, reason="no_documents")
            return
        representative = next(
            (doc for doc in docs if doc.doc_type is DocType.POLICY),
            docs[0],
        )
        ocr_quality = (
            "needs_reupload" if any(doc.ocr_quality == "needs_reupload" for doc in docs) else "ok"
        )
        report = ReportJob(
            report_id=job.report_id,
            ocr_result_id=representative.ocr_result_id,
            job_id=job.job_id,
            doc_type=representative.doc_type,
            user_ref=job.user_ref,
            claim_id=job.claim_id,
            created_at=datetime.now(UTC),
            ocr_quality=ocr_quality,
        )
        await self._producer.send(self._settings.sqs_report_job_queue_url, report)
        logger.info(
            "claim_report_published",
            claim_id=job.claim_id,
            report_id=job.report_id,
            docs=len(docs),
            doc_type=str(representative.doc_type),
            ocr_quality=ocr_quality,
        )
