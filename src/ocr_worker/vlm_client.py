"""클러스터 vLLM(PaddleOCR-VL) 비전 클라이언트 — 쪽 전사·라인 재판독(하이브리드 경로).

core/ai_client.py는 텍스트 chat/embeddings 전용이라 이미지 페이로드를 다루지 않는다. VLM은
OCR 전용 보조 단계라 core가 아닌 여기 별도 모듈로 둔다. 하는 일은 둘이다:

- **쪽 전사**(``transcribe_table``): 다중 항목 표 문서·저신뢰도 문서에서 OCR 엔진 라인 순서가
  표 구조를 못 살리는 문제를 보완한다. 채택 여부(groundedness·마스킹·잔류 검사)는 파이프라인이
  정한다.
- **라인 재판독**(``reread_lines``): 저신뢰도 문서에서 OCR 엔진 라인 bbox를 원본 해상도로 잘라
  한 줄씩 다시 읽는다. OCR 엔진이 PII 글자를 완전히 오독(예: surya 시절 이름 → "828")하면 라인
  기반 검출이 그 라인을 못 잡는데, ``ImageMasker``가 이 텍스트로 다시 검출해 검은블록을 보탠다
  (예전 Qwen grounding의 대체 —
  PaddleOCR-VL은 "PII를 찾아 좌표로 답하라" 같은 지시를 따르지 않는다).

서빙은 OpenAI 호환 ``POST /v1/chat/completions``다. 운영은 클러스터 vLLM(K8s-manifests
``charts/etl-vllm``, ``--served-model-name PaddleOCR-VL-1.6-0.9B``)이고, 같은 요청 형태를 Ollama도
받으므로 env(``VLM_BASE_URL``·``VLM_MODEL``)만 바꿔 범용 VLM으로 되돌릴 수 있다. 요청 형태·퇴행
출력 폐기·표 변환은 같은 vLLM을 운영 중인 Policy-Chunker(``insurance_chunker/extractor.py``의
``call_vlm`` 등, main 059908c)에서 검증된 것을 옮겼다.

PaddleOCR-VL-1.6 공식 문서·모델 카드 기준으로 알아 둘 한계(2026-11-05 확인):

- **쪽 전체 입력은 공식 권장 경로가 아니다.** 문서는 쪽 전체 이미지를 OpenAI 클라이언트로 VLM
  서버에 직접 보내는 것을 "strongly discouraged"라 하고, 레이아웃 검출 → 영역별 인식 파이프라인을
  권장한다. 직접 호출 예시는 element-level 인식과 text spotting만 지원한다고 적는다. 쪽 전사
  (``transcribe_table``)를 쪽 전체 ``"OCR:"``로 두는 건 팀 청커가 운영에서 쓰는 경로라서이고, 비전
  입력이 ``max_pixels``(spotting 외 태스크 기본 1280*28*28 ≈ 1.0MP)로 줄어 작은 글씨가 뭉개질 수
  있다. 그래서 채택 전에 groundedness·마스킹·잔류 검사를 거치고 실패하면 쪽마다 OCR 엔진 결과로
  폴백한다.
  후속 후보: 레이아웃 + 영역 crop 전사.
- **라인 재판독은 문서화된 사용법(element-level 입력)과 맞는다** — 라인 crop이 곧 텍스트 요소다.
  crop은 작아 ``max_pixels``에 걸리지 않고, 아주 작은 crop은 모델 쪽 ``min_pixels``에 맞춰 늘어난다
  (여기서 따로 업스케일하지 않는다 — 효과를 잰 근거가 없다).
- **출력에 신뢰도·logprob이 없다.** 그래서 신뢰도 게이트·검증의 기준은 계속 OCR 엔진
  (PP-OCRv5)이다.
- ``"Spotting:"`` 태스크(좌표 출력)는 1.6에 있지만 출력의 좌표 인코딩이 문서에 없어 쓰지 않는다.
- 한국어는 지원 언어 목록에 명시돼 있지 않다("109 languages"). 청커가 한국어 약관에 운영 중이라
  실사용은 확인됐지만 우리 서류(진단서·영수증 사진 등)의 정확도는 따로 평가해야 한다.

동시도: 모든 호출이 프로세스 단위 세마포어(``vlm_concurrency``)를 지난다. 워커는 메시지를 1건씩
처리하므로 곧 문서 1건이 vLLM에 거는 요청 상한이다(같은 vLLM을 청커와 나눠 쓴다).

블로킹 격리(§7): PNG 인코딩·crop(PIL)은 CPU 바운드라 ``asyncio.to_thread``로 뗀다.

PII(§9·§13): 응답 본문(VLM 전사·재판독 텍스트)은 마스킹 전 평문이라 로그·예외 메시지에 싣지
않는다. 로그엔 개수·사유(고정 어휘)만 남긴다.
"""

import asyncio
import base64
import html
import io
import math
import re
from collections import Counter
from collections.abc import Sequence
from typing import Any

import httpx

from core.config import settings
from core.logging import get_logger
from ocr_worker.ocr import OcrPage, PageImage

logger = get_logger(__name__)

_vlm_client: httpx.AsyncClient | None = None
_vlm_semaphore: asyncio.Semaphore | None = None
_vlm_semaphore_loop: asyncio.AbstractEventLoop | None = None

_CHAT_COMPLETIONS_PATH = "/v1/chat/completions"

# PaddleOCR-VL은 태스크 토큰이 곧 프롬프트다 — 범용 지시문을 주면 태스크 라우팅이 안 된다(청커
# 실측). "OCR:"은 쪽 전체 입력이면 쪽 markdown을(팀 Colab 실험 D — 공식 권장 경로는 아니다, 모듈
# docstring), 텍스트 영역 crop이면 그 글자를 낸다. 모델명으로 고르므로 VLM_MODEL만 바꾸면 프롬프트를
# 따로 맞출 필요가 없다.
_PADDLE_OCR_TASK = "OCR:"
_PADDLE_MODEL_MARKER = "paddleocr"

# 범용 instruct VLM(모델명에 paddleocr 없음 — 예: Ollama의 qwen 계열)용 쪽 전사 지시문.
VLM_TABLE_PROMPT = (
    "이 이미지는 한국 보험/의료 문서입니다. 이미지 안의 모든 텍스트를 표 구조를 "
    "유지해 가능한 한 정확히 그대로 옮겨 적어주세요. 표뿐 아니라 문서 상단의 "
    "발급기관·환자/계약자 정보 같은 라벨-값 블록도 빠짐없이 포함해주세요. "
    "표는 마크다운 표로, 항목-금액 쌍은 누락 없이 옮겨주세요. 추측하지 말고 "
    "보이는 대로만 적어주세요. 특정 부분이 흐리거나 읽을 수 없으면 내용을 "
    "지어내지 말고 '[읽을 수 없음]'이라고만 적어주세요."
)
# 범용 instruct VLM용 라인 재판독 지시문. 쪽 지시문을 그대로 쓰면 한 줄 crop에도 표·라벨-값을
# 찾으라는 지시가 붙어 설명문이 섞인다.
VLM_LINE_PROMPT = (
    "이 이미지는 문서에서 글자 한 줄을 잘라낸 것입니다. 보이는 글자를 그대로 한 줄로 "
    "옮겨 적어주세요. 설명이나 따옴표 없이 글자만 출력하고, 읽을 수 없으면 아무것도 "
    "출력하지 마세요."
)

# 라인 crop 여백(픽셀). OCR 라인 bbox는 글자에 가깝게 잡혀 가장자리 획이 잘릴 수 있다. 청커의
# 영역 crop 여백(8px)보다 작게 둔다 — 줄 간격이 좁은 서식에서 위아래 줄 획이 섞여 들어오지 않게.
_LINE_CROP_PAD_PX = 4

# 같은 줄·셀이 이만큼 연속되면 퇴행(반복 루프) 출력으로 보고 버린다(청커 VLM_LOOP_CELLS 기본값).
# max_tokens에 닿은 루프는 finish_reason으로 잡히지만, 그 전에 멈춘 루프는 이 검사만 잡는다.
_LOOP_REPEAT = 20
# 표 병합 셀 상한. 퇴행 출력의 colspan="99999" 한 줄이 거대한 행을 만들지 않게 자른다.
_MAX_CELL_SPAN = 64
# 셀 안의 "|"는 markdown 행을 깨므로 전각 슬래시(U+FF0F)로 바꾼다(청커와 같은 규칙).
_CELL_PIPE_SUBSTITUTE = chr(0xFF0F)

# 재판독 결과 분류 — 실패가 아닌 것(로그의 failed에 세지 않는다).
_REREAD_OK = "ok"
_REREAD_EMPTY = "empty"
_REREAD_SKIPPED = "skipped"
_REREAD_BUDGET = "budget"
_NOT_FAILURES = frozenset({_REREAD_OK, _REREAD_EMPTY, _REREAD_SKIPPED})


class VlmClientError(RuntimeError):
    """VLM 호출 실패의 기반 예외 — 호출측은 잡아서 OCR 엔진 결과로 폴백한다.

    Attributes:
        reason: 실패 분류(고정 어휘: ``http``·``timeout``·``malformed``·``truncated``·``loop``).
            로그에 그대로 남겨도 되는 값이다 — 메시지·원인 체인에는 응답 원문을 싣지 않는다(§9).
    """

    def __init__(self, message: str, *, reason: str = "unknown") -> None:
        super().__init__(message)
        self.reason = reason


def _page_timeout() -> httpx.Timeout:
    """쪽 전사 타임아웃 — 읽기는 생성 시간(max_tokens ÷ 생성 속도)만큼, 접속은 짧게."""
    return httpx.Timeout(settings.vlm_timeout_seconds, connect=settings.vlm_connect_timeout_seconds)


def _reread_timeout() -> httpx.Timeout:
    """라인 재판독 타임아웃 — 한 줄 출력은 짧아 읽기 상한도 작다."""
    return httpx.Timeout(
        settings.vlm_reread_timeout_seconds, connect=settings.vlm_connect_timeout_seconds
    )


def _get_vlm_client() -> httpx.AsyncClient:
    """VLM 추론용 공유 AsyncClient(지연 생성)."""
    global _vlm_client
    if _vlm_client is None:
        _vlm_client = httpx.AsyncClient(base_url=settings.vlm_base_url, timeout=_page_timeout())
    return _vlm_client


def _get_semaphore() -> asyncio.Semaphore:
    """VLM 동시 요청 상한(``vlm_concurrency``) — 실행 중인 이벤트 루프마다 하나.

    asyncio 동기화 객체는 처음 경합할 때 그 루프에 묶여, 다른 루프에서 쓰면 RuntimeError가
    난다. 루프가 바뀌면(테스트마다 새 루프 등) 새로 만든다.
    """
    global _vlm_semaphore, _vlm_semaphore_loop
    loop = asyncio.get_running_loop()
    if _vlm_semaphore is None or _vlm_semaphore_loop is not loop:
        _vlm_semaphore = asyncio.Semaphore(settings.vlm_concurrency)
        _vlm_semaphore_loop = loop
    return _vlm_semaphore


async def close_client() -> None:
    """공유 AsyncClient를 종료한다(앱 종료 시 1회 — ``ocr_worker.__main__``이 호출)."""
    global _vlm_client, _vlm_semaphore, _vlm_semaphore_loop
    if _vlm_client is not None:
        await _vlm_client.aclose()
    _vlm_client = None
    _vlm_semaphore = None
    _vlm_semaphore_loop = None


def _is_paddle_model() -> bool:
    """서빙 모델이 PaddleOCR-VL인가(태스크 토큰 프롬프트를 쓸지)."""
    return _PADDLE_MODEL_MARKER in settings.vlm_model.lower()


async def transcribe_table(image: PageImage) -> str:
    """페이지 이미지 1장을 VLM으로 표 구조를 살려 전사한다(쪽 전체 입력).

    PaddleOCR-VL이면 ``"OCR:"``(쪽 markdown), 범용 VLM이면 ``VLM_TABLE_PROMPT``를 보낸다.
    응답은 ``normalize_output``으로 정리한다(코드펜스·위첨자·HTML/OTSL 표 → pipe markdown).

    Args:
        image: ``render_to_images``가 만든 PIL 페이지 이미지.

    Returns:
        정리된 전사 텍스트(원문 — PII 포함 가능, 호출측이 마스킹).

    Raises:
        VlmClientError: HTTP 오류·타임아웃·응답 형식 이상, 또는 퇴행 출력(``max_tokens``에서
            잘림·반복 루프 — 잘린 전사는 행이 끊겨 구조가 틀리므로 반쪽을 싣지 않고 버린다).
    """
    image_b64 = await asyncio.to_thread(_png_base64, image)
    prompt = _PADDLE_OCR_TASK if _is_paddle_model() else VLM_TABLE_PROMPT
    return await _chat(
        image_b64, prompt, max_tokens=settings.vlm_max_tokens, http_timeout=_page_timeout()
    )


async def reread_lines(
    images: Sequence[PageImage], pages: Sequence[OcrPage]
) -> list[list[str | None]]:
    """문서의 OCR 엔진 라인을 bbox crop으로 한 줄씩 다시 읽는다(이미지 마스킹 보강).

    crop은 원본 해상도라(쪽 전체 입력은 모델이 ~1MP로 줄인다) 작은 글씨도 선명하고, 좌표는
    OCR 엔진의 정밀 bbox를 그대로 쓴다. 문서 전체를 한 번에 받는 이유는 우선순위·예산 때문이다:

    - **신뢰도 낮은 라인부터** 요청한다. 오독한 라인일수록 OCR 신뢰도가 낮을 가능성이 크고,
      예산이 끝나면 남는 것은 신뢰도 높은 라인이다.
    - **단계 예산**(``vlm_reread_budget_seconds``)을 넘기면 남은 라인은 건너뛴다. 단건 타임아웃만
      으로는 라인 수에 비례해 SQS 가시성(600초)을 넘길 수 있다(config의 최악 경로 주석).

    실패는 라인 단위로 흡수한다(fail-open): 그 라인만 ``None``이고 OCR 엔진 텍스트가 유지된다 —
    재판독은 기존 OCR 엔진 판정에 **보태기만** 하므로 실패해도 OCR 엔진 단독 판정보다 후퇴하지
    않는다(예전 grounding 대비 한계는 ``masking.image_masker`` 모듈 설명).
    대신 결과 분포(성공·빈 응답·실패 사유별 수)를 로그 1건으로 남긴다(텍스트는 남기지 않는다).

    Args:
        images: 원본 페이지 이미지(``pages``와 같은 순서·길이).
        pages: 같은 문서의 OCR 엔진 페이지(라인 bbox·신뢰도).

    Returns:
        페이지별·라인별 재판독 텍스트(공백 정규화된 한 줄). 실패·빈 응답·잘못된 bbox·예산
        초과 라인은 ``None``. 바깥 길이는 ``pages``, 안쪽 길이는 각 ``page.lines``와 같다.

    Raises:
        ValueError: 이미지 수와 페이지 수가 다를 때(호출측 계약 위반).
    """
    if len(images) != len(pages):
        raise ValueError(f"이미지/페이지 수 불일치: images={len(images)} pages={len(pages)}")
    texts: list[list[str | None]] = [[None] * len(page.lines) for page in pages]
    positions = sorted(
        (
            (page_index, line_index)
            for page_index, page in enumerate(pages)
            for line_index in range(len(page.lines))
        ),
        key=lambda position: pages[position[0]].lines[position[1]].confidence,
    )
    if not positions:
        return texts
    crops = await asyncio.to_thread(_encode_line_crops, images, pages)
    deadline = asyncio.get_running_loop().time() + settings.vlm_reread_budget_seconds
    # gather는 인자 순서대로 task를 띄우므로 세마포어 대기열(FIFO)도 우선순위 순서가 된다.
    outcomes = await asyncio.gather(
        *(
            _reread_one(crops[page_index][line_index], deadline)
            for page_index, line_index in positions
        )
    )
    for (page_index, line_index), (text, _reason) in zip(positions, outcomes, strict=True):
        texts[page_index][line_index] = text
    _log_reread_summary([reason for _text, reason in outcomes])
    return texts


async def _reread_one(crop_b64: str | None, deadline: float) -> tuple[str | None, str]:
    """crop 1장을 재판독한다 — ``(텍스트 | None, 결과 분류)``. 예외를 올리지 않는다."""
    if crop_b64 is None:
        return None, _REREAD_SKIPPED
    prompt = _PADDLE_OCR_TASK if _is_paddle_model() else VLM_LINE_PROMPT
    try:
        async with asyncio.timeout_at(deadline):
            text = await _chat(
                crop_b64,
                prompt,
                max_tokens=settings.vlm_reread_max_tokens,
                http_timeout=_reread_timeout(),
            )
    except VlmClientError as exc:
        return None, exc.reason
    except TimeoutError:
        return None, _REREAD_BUDGET
    # 한 줄 crop이라 줄바꿈은 위아래 줄 획이 섞인 흔적일 뿐이다 — 한 줄로 편다. 공백 정규화는
    # PII 패턴(구분자 하나를 허용)을 깨지 않는다.
    line = " ".join(text.split())
    return (line, _REREAD_OK) if line else (None, _REREAD_EMPTY)


def _log_reread_summary(reasons: list[str]) -> None:
    """재판독 결과 분포를 1건으로 남긴다 — 실패가 있으면 warning(개수·사유만, §9)."""
    counts = Counter(reasons)
    failures = {reason: n for reason, n in counts.items() if reason not in _NOT_FAILURES}
    log = logger.warning if failures else logger.info
    log(
        "vlm_line_reread",
        lines=len(reasons),
        reread=counts[_REREAD_OK],
        empty=counts[_REREAD_EMPTY],
        skipped=counts[_REREAD_SKIPPED],
        failed=sum(failures.values()),
        failure_reasons=dict(sorted(failures.items())),
    )


async def _chat(
    image_b64: str, prompt: str, *, max_tokens: int, http_timeout: httpx.Timeout
) -> str:
    """이미지 1장 + 프롬프트로 chat completions를 호출하고 정리된 본문을 돌려준다.

    Raises:
        VlmClientError: 호출 실패·형식 이상·퇴행 출력(``reason``으로 구분).
    """
    payload: dict[str, Any] = {
        "model": settings.vlm_model,
        "messages": [
            {
                "role": "user",
                # 이미지 먼저, 텍스트(태스크 토큰) 다음 — 모델 카드 예시·청커 call_vlm과 같은 순서.
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/png;base64,{image_b64}"},
                    },
                    {"type": "text", "text": prompt},
                ],
            }
        ],
        # temperature=0: 창작 경향을 낮춰 환각(이미지에 없는 내용 지어내기)을 줄인다. 완전히
        # 막진 못하므로 쪽 전사는 pipeline._looks_grounded()가 별도로 검증한다.
        "temperature": 0.0,
        "max_tokens": max_tokens,
    }
    client = _get_vlm_client()
    async with _get_semaphore():
        try:
            response = await client.post(_CHAT_COMPLETIONS_PATH, json=payload, timeout=http_timeout)
            response.raise_for_status()
            body = response.json()
        except httpx.TimeoutException as exc:
            raise VlmClientError("VLM 호출 시간 초과", reason="timeout") from exc
        except httpx.HTTPError as exc:
            raise VlmClientError("VLM 호출 실패", reason="http") from exc
        except ValueError as exc:
            # 비-JSON 본문(json.JSONDecodeError). 여기서 변환 안 하면 pipeline._extract_pages의
            # OCR 엔진 폴백을 못 타고 작업 자체가 실패한다(예: 앞단 프록시가 에러 페이지를 200으로
            # 반환).
            raise VlmClientError("VLM 응답 형식이 올바르지 않음", reason="malformed") from exc
    content, finish_reason = _parse_choice(body)
    if finish_reason == "length":
        # max_tokens에서 잘렸다. 상한까지 가는 응답은 대개 반복 폭주이고(청커 기록 2026-10-22:
        # 운영 기준선 호출의 28%가 상한에서 잘렸고, 그 모양 — 백지 입력의 반복 — 이 PaddleOCR-VL
        # Colab 실험에서 재현됐다), 잘린 표는 행이 끊겨 구조가 틀리다 — 반쪽을 싣지 않고 버린다.
        raise VlmClientError("VLM 출력이 max_tokens에서 잘림", reason="truncated")
    try:
        text = normalize_output(content)
        looping = _looping_lines(text) or _looping_cells(text)
    except (ValueError, IndexError, KeyError, TypeError) as exc:
        # 정리 단계도 모델 출력을 다루는 경계다. 여기 예외가 그대로 새면 그 쪽만 OCR 엔진 결과로
        # 폴백하는 대신 작업 전체가 실패한다. 원인 예외의 메시지는 응답 조각을 실을 수 있어
        # (예: int()의 "invalid literal" 메시지) 체인을 끊고 타입 이름만 남긴다(§9).
        raise VlmClientError(
            f"VLM 응답 정리 실패({type(exc).__name__})", reason="malformed"
        ) from None
    if looping:
        raise VlmClientError("VLM 출력이 반복 루프", reason="loop")
    return text


def _parse_choice(body: Any) -> tuple[str, str | None]:
    """chat completions 응답에서 ``(본문, finish_reason)``을 꺼낸다(외부 미지정 데이터 경계)."""
    try:
        choice = body["choices"][0]
        content = choice["message"]["content"]
        finish_reason = choice.get("finish_reason")
    except (KeyError, IndexError, TypeError, AttributeError) as exc:
        raise VlmClientError("VLM 응답 형식이 올바르지 않음", reason="malformed") from exc
    if not isinstance(content, str):
        raise VlmClientError("VLM 응답 본문이 문자열이 아님", reason="malformed")
    return content, finish_reason if isinstance(finish_reason, str) else None


def _png_base64(image: PageImage) -> str:
    """PIL 이미지를 base64 PNG로 인코딩한다(동기 — ``to_thread``용)."""
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def _encode_line_crops(
    images: Sequence[PageImage], pages: Sequence[OcrPage]
) -> list[list[str | None]]:
    """페이지별·라인별 bbox crop을 base64 PNG로 만든다(동기 — ``to_thread``용).

    한 스레드 호출에서 순차로 처리한다 — 같은 원본 이미지를 여러 스레드가 동시에 crop하지 않게.
    """
    return [
        [_crop_base64(image, line.bbox) for line in page.lines]
        for image, page in zip(images, pages, strict=True)
    ]


def _crop_base64(image: PageImage, bbox: tuple[float, float, float, float]) -> str | None:
    """bbox(+여백)를 이미지 경계 안에서 잘라 base64 PNG로 만든다. 면적이 없으면 ``None``."""
    x0, y0, x1, y1 = bbox
    left = max(0, math.floor(x0) - _LINE_CROP_PAD_PX)
    top = max(0, math.floor(y0) - _LINE_CROP_PAD_PX)
    right = min(int(image.width), math.ceil(x1) + _LINE_CROP_PAD_PX)
    bottom = min(int(image.height), math.ceil(y1) + _LINE_CROP_PAD_PX)
    if right <= left or bottom <= top:
        return None  # 좌표 역전·화면 밖 — 보낼 픽셀이 없다(요청하지 않는다)
    return _png_base64(image.crop((left, top, right, bottom)))


# ── 출력 정리(순수) ───────────────────────────────────────────────
# 코드펜스·위첨자 정규화, HTML/OTSL 표 → pipe markdown, 퇴행 검출은 Policy-Chunker
# insurance_chunker/extractor.py(main 059908c)의 _strip_fences·_normalize_superscripts·
# _html_table_to_markdown·_otsl_to_markdown·_looping_lines·_looping_cells를 옮겼다. 같은 vLLM·같은
# 모델의 출력으로 운영 검증된 코드다. 여기서 달라진 점:
# - 표 블록을 **제자리에서** 바꾼다. 청커는 표 전용 태스크라 응답 전체를 표로 보지만, 여기는 쪽
#   전체 전사라 표 앞뒤 본문을 잃으면 안 된다.
# - 셀 안 줄바꿈은 <br>이 아니라 공백으로 편다. 마스킹 정규식의 라벨-값 구분자(공백·콜론·|·*)가
#   <br>을 못 넘어, "성명<br>홍길동" 같은 셀의 이름을 놓친다(patterns.PERSON_LABEL_NAME_RE).
#   같은 이유로 HTML 엔티티(&nbsp; 등)도 풀고, 표 밖에 남은 태그도 걷는다(청커는 표만 다뤘다).
# - 병합 셀 수를 자른다(_MAX_CELL_SPAN) — 퇴행 출력의 거대한 colspan 방어.
_FENCE_OPEN_RE = re.compile(r"^```[a-zA-Z]*\n?")
_FENCE_CLOSE_RE = re.compile(r"\n?```$")
# PaddleOCR-VL이 위첨자를 LaTeX 조각으로 낸다: \(^{*}\)·$ ^{*} $ → *.
_SUPERSCRIPT_RE = re.compile(r"\\\(\s*\^\{([^{}]*)\}\s*\\\)|\$\s*\^\{([^{}]*)\}\s*\$")
_HTML_TABLE_RE = re.compile(r"<table[^>]*>.*?</table>", re.S | re.I)
_HTML_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
# (?![a-zA-Z])가 없으면 <thead>가 <th>+속성 "ead"로 잡혀 속성·셀의 짝이 어긋난다(청커 실측).
_HTML_CELL_RE = re.compile(r"<t[hd](?![a-zA-Z])[^>]*>(.*?)</t[hd]>", re.S | re.I)
_HTML_CELL_ATTR_RE = re.compile(r"<t[hd](?![a-zA-Z])([^>]*)>", re.I)
# 자릿수를 6으로 묶는다 — int()는 4,300자리를 넘는 문자열에 ValueError를 낸다(QA 재현: colspan
# 4,301자리). 값은 어차피 _MAX_CELL_SPAN으로 자르므로 앞 6자리면 충분하다.
_ROWSPAN_RE = re.compile(r'rowspan="?(\d{1,6})', re.I)
_COLSPAN_RE = re.compile(r'colspan="?(\d{1,6})', re.I)
_BR_RE = re.compile(r"<br\s*/?>", re.I)
_TAG_RE = re.compile(r"<[^>]+>")
# 표 밖(그리고 OTSL 셀 안)에 남은 HTML 태그. 이름을 아는 태그만 걷는다 — 모르는 이름의 꺾쇠
# ("<https://…>" autolink 등)는 본문일 수 있어 남기고, "a < b"처럼 꺾쇠 바로 뒤가 글자가 아니면
# 태그로 보지 않는다. 남겨 두면 라벨-값·숫자 묶음 사이에 태그가 껴 마스킹 정규식과 잔류 검사가
# 둘 다 놓친다(QA 재현: "주민등록번호 901010<br>1234567" → 마스킹 안 됨·잔류 0건).
_HTML_TAG_RE = re.compile(r"</?([A-Za-z][A-Za-z0-9]*)\b[^<>]*>")
# 걷는 방식(masking/patterns.py·verify.py의 구분자·경계로 확인했다). 인라인 태그는 사이에 글자가
# 없는 연속(예: "</b><b>")을 한 덩어리로 보고, 덩어리 양옆의 **진짜 글자**로 정한다:
# - <br>: 공백(셀과 같다). 줄바꿈이면 카드번호(CARD_RE·잔류 패턴의 [- \t])가 못 넘는다. 주민·전화
#   ([-\s]?)·이름 라벨(공백·콜론·|·* 반복)은 공백 하나를 허용한다. 단 하이픈 바로 뒤의 <br>은
#   줄 넘김으로 잘린 값("901010-<br>1234567")이라 공백 없이 붙인다 — 공백이 끼면 구분자가
#   2자가 된다.
# - <sup>·<sub>(그리고 LaTeX 위첨자 ^{…}): 각주·지수 표시라 **별도 토큰**이다. 양옆이 둘 다
#   글자·숫자면 공백 하나를 둔다. 붙이면 "901010-1234567<sup>1</sup>"이 "901010-12345671"이 돼
#   주민·전화·카드 정규식과 잔류 게이트의 숫자 경계((?<!\d)·(?!\d))가 함께 깨진다(QA W2-R).
# - 서식 태그(<b>·<span> 등): 렌더링처럼 기본은 그냥 걷는다(빈 문자열). "<b>9010</b>10-…"처럼 수
#   하나를 감싼 서식이 붙어야 주민번호(구분자 0~1자)가 잡힌다. 다만 양옆이 **둘 다 한글**이면 공백
#   하나를 둔다 — "홍길동<b>입니다</b>"가 "홍길동입니다"로 붙으면 이름 경계((?![가-힣]))가 사라진다.
#   양옆이 숫자·영문이면 붙인다: 띄우면 수 안쪽 서식에서 주민·카드가 깨지고, KCD("J20.9")·증권번호
#   같은 영숫자 식별자가 쪼개진다. 영문↔숫자로 붙어 RRN_RE의 영문 경계에 걸리는 경우는 잔류 게이트
#   (verify.py, 숫자 경계만 본다)가 잡아 그 쪽을 OCR 엔진 결과로 폴백시킨다.
#   알려진 대가: 한 단어 안을 서식이 가른 경우("<span>홍</span><span>길동</span>")는 "홍 길동"으로
#   벌어져 정규식이 이름을 놓친다(태그를 그대로 두던 dev도 같은 자리에서 놓쳤다 — NER 몫).
# - 블록 태그: 줄바꿈. 빈 문자열로 붙이면 "<p>성명: 홍길동</p><p>입원일…"이 "홍길동입원일"이 돼
#   이름 정규식의 경계((?![가-힣]))가 깨진다.
_INLINE_TAGS = frozenset(
    {
        "a", "abbr", "b", "big", "cite", "code", "del", "em", "font", "i", "img", "ins",
        "kbd", "label", "mark", "q", "s", "samp", "small", "span", "strike", "strong",
        "sub", "sup", "tt", "u", "var",
    }
)  # fmt: skip
_BLOCK_TAGS = frozenset(
    {
        "address", "article", "aside", "blockquote", "caption", "center", "col", "colgroup",
        "dd", "div", "dl", "dt", "figcaption", "figure", "footer", "h1", "h2", "h3", "h4",
        "h5", "h6", "header", "hr", "li", "main", "nav", "ol", "p", "pre", "section", "table",
        "tbody", "td", "tfoot", "th", "thead", "tr", "ul",
    }
)  # fmt: skip
_SCRIPT_TAGS = frozenset({"sup", "sub"})
# <br>·인라인 태그가 사이 글자 없이 이어진 한 덩어리(긴 이름부터 — "b"가 "br"·"big"을 먹지 않게).
_INLINE_RUN_RE = re.compile(
    r"(?:</?(?:" + "|".join(sorted(_INLINE_TAGS | {"br"}, key=len, reverse=True)) + r")\b[^<>]*>)+",
    re.I,
)
_TAG_NAME_RE = re.compile(r"</?([A-Za-z][A-Za-z0-9]*)")
# 하이픈 바로 뒤의 줄 넘김 — 줄바꿈으로 잘린 값이라 하이픈에 붙인다(셀 안 포함).
_HYPHEN_BR_RE = re.compile(r"-[ \t]*(?:<br\b[^<>]*>[ \t]*)+", re.I)
_HYPHEN_LINEBREAK_RE = re.compile(r"-[ \t]*(?:\\n|\n)+\s*")
# 줄바꿈이 아닌 공백류(탭·&nbsp;의 \xa0·전각 공백 등) → 일반 공백. 카드번호 구분자 [- \t]가
# \xa0을 못 넘기 때문이다.
_NON_NEWLINE_SPACE_RE = re.compile(r"[^\S\n ]")
# OTSL: <fcel>텍스트 = 셀, <ecel> = 빈 셀, <lcel> = 왼쪽 병합, <ucel>/<xcel> = 위 병합,
# <nl> = 행 끝.
# <nl>은 뒤 글자를 먹지 않게 따로 둔다 — 표 뒤 본문이 마지막 행 토큰에 흡수돼 사라지지 않게.
_OTSL_RUN_RE = re.compile(r"(?:<(?:fcel|ecel|lcel|ucel|xcel)>[^<]*|<nl>)+")
_OTSL_TOKEN_RE = re.compile(r"<(fcel|ecel|lcel|ucel|xcel|nl)>([^<]*)")
_OTSL_LINEBREAK_RE = re.compile(r"\s*(?:\\n|\n)+\s*")
_WHITESPACE_RE = re.compile(r"\s+")
_EXCESS_BLANK_LINES_RE = re.compile(r"\n{3,}")


def normalize_output(text: str) -> str:
    """VLM 응답 본문을 파이프라인이 쓰는 텍스트로 정리한다(순수).

    코드펜스를 벗기고 위첨자 LaTeX를 기호로 되돌린 뒤, HTML·OTSL 표를 **제자리에서** pipe
    markdown으로 바꾼다(표 앞뒤 본문은 그대로). markdown 표는 마스킹 정규식이 라벨-값을 잡는
    형태(``| 환자 성명 | 홍길동 |``)이고, ``_looks_grounded``의 토큰 비교에도 태그가 끼지 않는다.
    표 밖에 남은 HTML 태그도 걷고(``<br>`` → 공백, 인라인 → 제거, 블록 → 줄바꿈) 엔티티를 풀어,
    태그·``&nbsp;``가 PII 값 사이에 끼어 마스킹과 잔류 검사를 빠져나가지 않게 한다.

    Args:
        text: chat completions 응답의 ``message.content``.

    Returns:
        정리된 텍스트(앞뒤 공백 제거).
    """
    text = _strip_fences(_normalize_superscripts(text))
    # 표 변환 **전에** <br>·인라인 태그를 걷는다 — OTSL 셀 안의 태그가 토큰 연속을 끊어 표가
    # 쪼개지고 값이 표 밖으로 밀려나지 않게. 블록 태그(<td>·<tr> 등)는 표 변환이 써야 해서 남긴다.
    text = _HYPHEN_BR_RE.sub("-", text)
    text = _INLINE_RUN_RE.sub(_inline_run_replacement, text)
    text = _HTML_TABLE_RE.sub(_html_block_to_markdown, text)
    text = _OTSL_RUN_RE.sub(_otsl_block_to_markdown, text)
    # 표 변환에서 빠진 블록 태그(표 밖 <p>·<div>, 짝 잃은 중첩 표 태그 등)는 줄바꿈으로 걷는다.
    text = _HTML_TAG_RE.sub(_block_tag_replacement, text)
    text = _NON_NEWLINE_SPACE_RE.sub(" ", html.unescape(text))
    return _EXCESS_BLANK_LINES_RE.sub("\n\n", text).strip()


def _strip_fences(text: str) -> str:
    """응답을 감싼 ``` 코드펜스를 벗긴다."""
    text = _FENCE_OPEN_RE.sub("", text.strip())
    return _FENCE_CLOSE_RE.sub("", text).strip()


def _normalize_superscripts(text: str) -> str:
    """위첨자 LaTeX 조각(``\\(^{*}\\)``·``$ ^{*} $``)을 내용만 남겨 되돌린다(별도 토큰).

    내용이 앞뒤 글자·숫자와 맞닿으면 공백 하나를 둔다 — 각주 숫자가 값에 붙어 숫자 경계가
    깨지지 않게(``901010-1234567$^{1}$`` → ``901010-1234567 1``). 기호(``*``)는 그대로 붙는다.
    """
    return _SUPERSCRIPT_RE.sub(_superscript_replacement, text)


def _superscript_replacement(match: re.Match[str]) -> str:
    """LaTeX 위첨자 하나를 내용으로 바꾼다(``re.sub`` 콜백, ``<sup>``과 같은 규칙)."""
    content = match.group(1) if match.group(1) is not None else match.group(2)
    before, after = _neighbors(match)
    prefix = " " if before.isalnum() and content[:1].isalnum() else ""
    suffix = " " if after.isalnum() and content[-1:].isalnum() else ""
    return f"{prefix}{content}{suffix}"


def _neighbors(match: re.Match[str]) -> tuple[str, str]:
    """매치 바로 앞·뒤 글자(없으면 빈 문자열) — 치환 전 원문 기준."""
    source = match.string
    before = source[match.start() - 1] if match.start() > 0 else ""
    after = source[match.end()] if match.end() < len(source) else ""
    return before, after


def _is_hangul(char: str) -> bool:
    """한글 음절인가(patterns.py의 이름 정규식 [가-힣]과 같은 범위)."""
    return "\uac00" <= char <= "\ud7a3"


def _inline_run_replacement(match: re.Match[str]) -> str:
    """<br>·인라인 태그 한 덩어리를 공백 또는 빈 문자열로 바꾼다(규칙은 위 상수 주석).

    덩어리 단위로 판정해 ``</b><b>``처럼 붙은 태그도 양옆의 진짜 글자를 본다.
    """
    names = {name.lower() for name in _TAG_NAME_RE.findall(match.group(0))}
    before, after = _neighbors(match)
    if "br" in names:
        return " "
    if names & _SCRIPT_TAGS:
        return " " if before.isalnum() and after.isalnum() else ""
    return " " if _is_hangul(before) and _is_hangul(after) else ""


def _block_tag_replacement(match: re.Match[str]) -> str:
    """블록 태그 → 줄바꿈, 모르는 이름은 그대로(``re.sub`` 콜백)."""
    return "\n" if match.group(1).lower() in _BLOCK_TAGS else match.group(0)


def _html_block_to_markdown(match: re.Match[str]) -> str:
    """``<table>`` 블록 하나를 pipe markdown으로 바꾼다(``re.sub`` 콜백)."""
    markdown = _html_table_to_markdown(match.group(0))
    if markdown is None:
        # 행(<tr>)을 못 찾은 표 — 태그만 걷어 글자를 살린다(버리면 그 본문이 사라진다).
        return _TAG_RE.sub(" ", match.group(0))
    # 앞뒤 본문과 빈 줄로 띄운다(붙으면 표 행이 본문 줄과 이어진다). 과한 빈 줄은 뒤에서 접는다.
    return f"\n\n{markdown}\n\n"


def _otsl_block_to_markdown(match: re.Match[str]) -> str:
    """OTSL 토큰 연속 구간 하나를 pipe markdown으로 바꾼다(``re.sub`` 콜백)."""
    markdown = _otsl_to_markdown(match.group(0))
    return f"\n\n{markdown}\n\n" if markdown is not None else "\n"


def _cell_text(raw: str) -> str:
    """셀 내용 공통 정리 — 태그 제거, 엔티티 해제, 줄바꿈·공백을 공백 하나로, ``|`` 치환.

    인라인 태그·``<br>``은 표 변환 전에 이미 정리됐다. 여기 남은 태그(셀 안의 ``<p>``·``<div>`` 등
    블록)는 단어가 붙지 않게 공백으로 걷는다. 하이픈 바로 뒤 줄바꿈은 잘린 값이라 붙인다.
    """
    text = _TAG_RE.sub(" ", _BR_RE.sub(" ", raw))
    text = _HYPHEN_LINEBREAK_RE.sub("-", text)
    text = html.unescape(text)
    return _WHITESPACE_RE.sub(" ", text).strip().replace("|", _CELL_PIPE_SUBSTITUTE)


def _grid_to_markdown(grid: list[list[str]]) -> str | None:
    """2차원 셀 격자를 pipe markdown 표로 만든다(짧은 행은 빈 칸으로 채움)."""
    if not grid:
        return None
    width = max(len(row) for row in grid)
    lines = ["| " + " | ".join(row + [""] * (width - len(row))) + " |" for row in grid]
    lines.insert(1, "|" + "---|" * width)
    return "\n".join(lines)


def _cell_span(pattern: re.Pattern[str], attrs: str) -> int:
    """셀 속성의 rowspan/colspan 값(없으면 1, 1~``_MAX_CELL_SPAN``로 자름)."""
    match = pattern.search(attrs)
    if match is None:
        return 1
    return max(1, min(int(match.group(1)), _MAX_CELL_SPAN))


def _fill_rowspans(pending: dict[int, tuple[str, int]], row: list[str], col: int) -> int:
    """위 행의 rowspan이 이 행 ``col``부터 차지하는 칸을 채우고 다음 빈 열 번호를 돌려준다."""
    while col in pending:
        value, rows_left = pending.pop(col)
        row.append(value)
        if rows_left > 1:
            pending[col] = (value, rows_left - 1)
        col += 1
    return col


def _html_table_to_markdown(table_html: str) -> str | None:
    """HTML 표 1개 → pipe markdown. 행이 없으면 ``None``.

    병합 셀: 세로(rowspan)는 값을 반복하고, 가로(colspan)는 첫 칸만 값·나머지는 빈 칸이다 —
    가로까지 반복하면 전폭 안내 문단이 열 수만큼 복제된다(청커 실측).
    """
    rows = _HTML_ROW_RE.findall(table_html)
    if not rows:
        return None
    grid: list[list[str]] = []
    pending: dict[int, tuple[str, int]] = {}  # 열 → (아래 행에 이어질 값, 남은 행 수)
    for row_html in rows:
        out_row: list[str] = []
        col = 0
        cells = zip(
            _HTML_CELL_ATTR_RE.findall(row_html), _HTML_CELL_RE.findall(row_html), strict=False
        )
        for attrs, raw_cell in cells:
            col = _fill_rowspans(pending, out_row, col)
            text = _cell_text(raw_cell)
            rowspan = _cell_span(_ROWSPAN_RE, attrs)
            for offset in range(_cell_span(_COLSPAN_RE, attrs)):
                value = text if offset == 0 else ""  # 가로 병합은 첫 칸만
                out_row.append(value)
                if rowspan > 1:
                    pending[col] = (value, rowspan - 1)
                col += 1
        _fill_rowspans(pending, out_row, col)
        if out_row:
            grid.append(out_row)
    return _grid_to_markdown(grid)


def _otsl_to_markdown(otsl: str) -> str | None:
    """OTSL 토큰열 → pipe markdown. 병합 규칙은 HTML 변환과 같다(세로만 반복, 가로는 빈 칸)."""
    tokens = _OTSL_TOKEN_RE.findall(otsl)
    if not tokens:
        return None
    grid: list[list[str]] = []
    row: list[str] = []
    for tag, raw in tokens:
        text = _HYPHEN_LINEBREAK_RE.sub("-", raw.strip())  # 하이픈 뒤 줄바꿈은 잘린 값 — 붙인다
        text = html.unescape(_OTSL_LINEBREAK_RE.sub(" ", text))
        text = text.replace("|", _CELL_PIPE_SUBSTITUTE)
        if tag == "nl":
            if row:
                grid.append(row)
            row = []
        elif tag == "fcel":
            row.append(text)
        elif tag in ("ecel", "lcel"):
            row.append("")  # 빈 셀 · 가로 병합(첫 칸만 값)
        else:  # ucel / xcel — 위 행 같은 열 값 복제(세로 병합)
            col = len(row)
            row.append(grid[-1][col] if grid and col < len(grid[-1]) else "")
    if row:
        grid.append(row)
    return _grid_to_markdown(grid)


def _looping_lines(text: str) -> bool:
    """같은 줄이 ``_LOOP_REPEAT``번 연속되는가 — 반복 루프에 빠진 출력의 지문(빈 줄 무시)."""
    streak, previous = 0, None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped == previous:
            streak += 1
            if streak >= _LOOP_REPEAT:
                return True
        else:
            streak, previous = 1, stripped
    return False


def _looping_cells(text: str) -> bool:
    """markdown 표에서 같은 값의 셀이 ``_LOOP_REPEAT``번 연속되는가.

    빈 칸(가로 병합)과 구분선(---)은 세지 않는다. 행이 바뀌어도 연속은 이어진다 — 루프 출력은
    행 구분조차 반복하기 때문이다. 정상 표의 반복(세로 병합 반복 등)은 행마다 다른 셀이 끼어
    끊긴다(청커 실측).
    """
    streak, previous = 0, None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            streak, previous = 0, None
            continue
        for cell in stripped.strip("|").split("|"):
            value = cell.strip()
            if not value or set(value) <= {"-", ":"}:
                continue
            if value == previous:
                streak += 1
                if streak >= _LOOP_REPEAT:
                    return True
            else:
                streak, previous = 1, value
    return False
