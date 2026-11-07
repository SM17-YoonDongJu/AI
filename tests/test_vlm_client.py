"""vlm_client.py VLM(vision) 클라이언트 테스트.

httpx.MockTransport로 OpenAI 호환 ``/v1/chat/completions`` 응답을 가짜로 주입해(실제 GPU/vLLM
없이) 다음을 확인한다. test_ai_client.py와 같은 패턴(공유 클라이언트를 MockTransport로 교체).

- 요청 형태: 이미지 data URL이 먼저, 태스크 토큰(PaddleOCR-VL) 또는 한국어 지시문(범용 VLM)이
  다음. model·max_tokens·temperature 0·타임아웃(읽기/접속 분리).
- 퇴행 출력 폐기: ``finish_reason == "length"``(잘림)·같은 줄/셀 반복 루프 → ``VlmClientError``.
  예전 Ollama 경로의 num_ctx 잘림 테스트가 막던 위험("잘린 전사를 정상처럼 채택")을 이게 덮는다.
- 에러 변환: HTTP·연결·타임아웃·형식 오류 → ``VlmClientError``(파이프라인 surya 폴백의 전제).
- 출력 정리: 코드펜스·위첨자·HTML/OTSL 표 → pipe markdown(표 앞뒤 본문 보존).
- 라인 재판독: 라인마다 crop 1장, 실패 라인만 ``None``, 신뢰도 낮은 라인부터, 동시도 상한,
  단계 예산. 예전 grounding 테스트가 막던 위험(VLM 실패가 작업을 죽이거나 이미지 마스킹을
  비우는 것)은 "실패는 라인 단위 None + 나머지 라인 유지"로 다시 덮는다.
"""

import asyncio
import base64
import io
import json
from typing import Any

import httpx
import pytest
from PIL import Image
from structlog.testing import capture_logs

from ocr_worker import vlm_client
from ocr_worker.masking.masker import Masker
from ocr_worker.masking.spans import PiiLabel
from ocr_worker.masking.verify import MaskingError, assert_no_residual
from ocr_worker.ocr import OcrLine, OcrPage

_PADDLE_MODEL = "PaddleOCR-VL-1.6-0.9B"
_DATA_URL_PREFIX = "data:image/png;base64,"


def _install_mock(handler) -> None:  # type: ignore[no-untyped-def]
    vlm_client._vlm_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://test"
    )


@pytest.fixture(autouse=True)
async def _reset_client(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    # 개발자 env(VLM_MODEL 등)가 새어 들지 않게 기본 모델을 고정한다 — 프롬프트 분기가
    # 모델명에 달렸다.
    monkeypatch.setattr(vlm_client.settings, "vlm_model", _PADDLE_MODEL)
    yield
    await vlm_client.close_client()


def _image(width: int = 10, height: int = 10) -> Image.Image:
    return Image.new("RGB", (width, height), "white")


def _completion(content: Any, finish_reason: str | None = "stop") -> dict[str, Any]:
    return {
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": finish_reason,
            }
        ]
    }


def _body(request: httpx.Request) -> dict[str, Any]:
    return json.loads(request.content)


def _prompt(request: httpx.Request) -> str:
    return _body(request)["messages"][0]["content"][1]["text"]


def _sent_image_size(request: httpx.Request) -> tuple[int, int]:
    """요청에 실린 data URL을 디코드해 이미지 크기를 돌려준다(crop 식별용)."""
    url = _body(request)["messages"][0]["content"][0]["image_url"]["url"]
    assert url.startswith(_DATA_URL_PREFIX)
    with Image.open(io.BytesIO(base64.b64decode(url[len(_DATA_URL_PREFIX) :]))) as image:
        return image.size


def _line(
    text: str, bbox: tuple[float, float, float, float], *, confidence: float = 0.5
) -> OcrLine:
    x0, y0, x1, y1 = bbox
    return OcrLine(
        text=text,
        bbox=bbox,
        polygon=((x0, y0), (x1, y0), (x1, y1), (x0, y1)),
        confidence=confidence,
    )


# 200x100 이미지 위의 라인 bbox 3개. crop은 bbox + 여백 4px이라 너비가 58·108·158로 서로 달라,
# 핸들러가 디코드한 crop 너비로 어느 라인의 요청인지 가린다(높이는 모두 20 + 8 = 28).
_BBOX_A = (10.0, 10.0, 60.0, 30.0)
_BBOX_B = (10.0, 40.0, 110.0, 60.0)
_BBOX_C = (10.0, 70.0, 160.0, 90.0)
_WIDTH_A, _WIDTH_B, _WIDTH_C = 58, 108, 158


def _page(*lines: OcrLine, index: int = 0) -> OcrPage:
    return OcrPage(index=index, width=200, height=100, lines=lines)


# ── transcribe_table: 요청 형태 ──────────────────────────────────
async def test_transcribe_table_posts_image_then_ocr_task_token() -> None:
    # Arrange
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=_completion("ok"))

    _install_mock(handler)

    # Act
    await vlm_client.transcribe_table(_image(30, 20))

    # Assert: OpenAI 호환 chat completions, 이미지(쪽 전체 그대로) 먼저 + 태스크 토큰 다음.
    request = captured[0]
    body = _body(request)
    content = body["messages"][0]["content"]
    assert request.method == "POST"
    assert request.url.path == "/v1/chat/completions"
    assert [part["type"] for part in content] == ["image_url", "text"]
    assert _sent_image_size(request) == (30, 20)
    assert content[1]["text"] == "OCR:"  # PaddleOCR-VL은 태스크 토큰이 곧 프롬프트
    assert body["model"] == _PADDLE_MODEL  # vLLM --served-model-name과 같아야 한다
    assert body["temperature"] == 0


async def test_transcribe_table_sends_korean_instruction_to_non_paddle_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 범용 VLM(Ollama 등)으로 되돌린 경우 — 태스크 토큰을 주면 지시를 못 알아듣는다.
    monkeypatch.setattr(vlm_client.settings, "vlm_model", "qwen2.5vl:7b")
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=_completion("ok"))

    _install_mock(handler)

    # Act
    await vlm_client.transcribe_table(_image())

    # Assert
    assert _prompt(captured[0]) == vlm_client.VLM_TABLE_PROMPT
    assert _body(captured[0])["model"] == "qwen2.5vl:7b"


async def test_transcribe_table_uses_page_max_tokens_and_timeouts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 쪽 전사는 max_tokens가 곧 최악 시간이고, 접속 타임아웃은 짧게 따로 건다.
    monkeypatch.setattr(vlm_client.settings, "vlm_max_tokens", 1234)
    monkeypatch.setattr(vlm_client.settings, "vlm_timeout_seconds", 77.0)
    monkeypatch.setattr(vlm_client.settings, "vlm_connect_timeout_seconds", 3.0)
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=_completion("ok"))

    _install_mock(handler)

    # Act
    await vlm_client.transcribe_table(_image())

    # Assert
    timeout = captured[0].extensions["timeout"]
    assert _body(captured[0])["max_tokens"] == 1234
    assert timeout["read"] == 77.0
    assert timeout["connect"] == 3.0


async def test_transcribe_table_returns_message_content() -> None:
    # Arrange
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion("| 항목 | 금액 |\n|---|---|"))

    _install_mock(handler)

    # Act
    result = await vlm_client.transcribe_table(_image())

    # Assert
    assert result == "| 항목 | 금액 |\n|---|---|"


async def test_transcribe_table_converts_fenced_html_table_to_markdown() -> None:
    # Arrange: PaddleOCR-VL이 표를 HTML로 내는 경우 — 태그가 남으면 마스킹 정규식이
    # 라벨-값을 못 잇는다.
    content = (
        "```html\n진료비 영수증\n<table><tr><td>환자 성명</td><td>홍길동</td></tr></table>\n```"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion(content))

    _install_mock(handler)

    # Act
    result = await vlm_client.transcribe_table(_image())

    # Assert
    assert result == "진료비 영수증\n\n| 환자 성명 | 홍길동 |\n|---|---|"


# ── transcribe_table: 퇴행 출력 폐기 ─────────────────────────────
async def test_transcribe_table_discards_output_cut_at_max_tokens() -> None:
    # Arrange: max_tokens에서 잘린 응답 — 대개 반복 폭주이고, 잘린 표는 행 구조가 틀리다.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion("| 항목 | 금액 |\n| 진찰", "length"))

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError) as exc_info:
        await vlm_client.transcribe_table(_image())
    assert exc_info.value.reason == "truncated"


async def test_transcribe_table_discards_line_repetition_loop() -> None:
    # Arrange: 같은 줄 20회 연속 — max_tokens 전에 멈춘 루프는 finish_reason으로 못 잡는다.
    content = "보험금 지급결과 안내문\n" + "지급결정 내역\n" * 20

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion(content))

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError) as exc_info:
        await vlm_client.transcribe_table(_image())
    assert exc_info.value.reason == "loop"


async def test_transcribe_table_keeps_repetition_below_loop_threshold() -> None:
    # Arrange: 19회 반복은 정상 출력으로 본다(경계값).
    content = "지급결정 내역\n" * 19

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion(content))

    _install_mock(handler)

    # Act
    result = await vlm_client.transcribe_table(_image())

    # Assert
    assert result.count("지급결정 내역") == 19


async def test_transcribe_table_discards_table_cell_loop() -> None:
    # Arrange: 한 행에 같은 셀 20개 — 줄 반복 검사로는 안 잡히는 표 루프.
    content = "| " + " | ".join(["10,000"] * 20) + " |"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion(content))

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError) as exc_info:
        await vlm_client.transcribe_table(_image())
    assert exc_info.value.reason == "loop"


async def test_vlm_error_message_carries_no_response_text() -> None:
    # Arrange: 예외 메시지도 PII 경계다(§9) — 폐기 사유에 원문이 실리면 로그로 샌다.
    content = "환자 성명 홍길동 901010-1234567\n" * 25

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion(content))

    _install_mock(handler)

    # Act
    with pytest.raises(vlm_client.VlmClientError) as exc_info:
        await vlm_client.transcribe_table(_image())

    # Assert
    assert "홍길동" not in repr(exc_info.value)
    assert "901010" not in repr(exc_info.value)
    assert exc_info.value.__cause__ is None


# ── transcribe_table: 에러 변환(surya 폴백의 전제) ────────────────
async def test_transcribe_table_wraps_http_error() -> None:
    # Arrange: 5xx 응답
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError) as exc_info:
        await vlm_client.transcribe_table(_image())
    assert exc_info.value.reason == "http"


async def test_transcribe_table_wraps_malformed_response() -> None:
    # Arrange: "choices" 키 누락(예: 모델명이 틀려 다른 형태의 에러 본문이 200으로 온 경우)
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError) as exc_info:
        await vlm_client.transcribe_table(_image())
    assert exc_info.value.reason == "malformed"


async def test_transcribe_table_wraps_empty_choices() -> None:
    # Arrange
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": []})

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError):
        await vlm_client.transcribe_table(_image())


async def test_transcribe_table_wraps_null_content() -> None:
    # Arrange: content가 null인 응답(일부 서버가 빈 생성에 내는 형태)
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion(None))

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError) as exc_info:
        await vlm_client.transcribe_table(_image())
    assert exc_info.value.reason == "malformed"


async def test_transcribe_table_wraps_non_json_response() -> None:
    # Arrange — 코드리뷰 지적: resp.json()이 던지는 JSONDecodeError(ValueError 하위)가
    # 그대로 전파되면 VlmClientError로 안 잡혀 pipeline._extract_pages의 surya 폴백을
    # 못 타고 작업 자체가 실패한다(예: 앞단 프록시가 에러 페이지를 200으로 반환).
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>error</html>")

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError):
        await vlm_client.transcribe_table(_image())


async def test_transcribe_table_wraps_connect_error() -> None:
    # Arrange: 서버 없음(brbs-etl 정지 등)
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("연결 거부", request=request)

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError) as exc_info:
        await vlm_client.transcribe_table(_image())
    assert exc_info.value.reason == "http"


async def test_transcribe_table_wraps_read_timeout() -> None:
    # Arrange
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("응답 없음", request=request)

    _install_mock(handler)

    # Act / Assert
    with pytest.raises(vlm_client.VlmClientError) as exc_info:
        await vlm_client.transcribe_table(_image())
    assert exc_info.value.reason == "timeout"


async def test_transcribe_table_calls_share_concurrency_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 파이프라인이 쪽들을 동시에 던져도 vLLM에 걸리는 요청은 vlm_concurrency까지다.
    monkeypatch.setattr(vlm_client.settings, "vlm_concurrency", 2)
    in_flight = 0
    peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return httpx.Response(200, json=_completion("ok"))

    _install_mock(handler)

    # Act
    await asyncio.gather(*(vlm_client.transcribe_table(_image()) for _ in range(5)))

    # Assert
    assert peak == 2


# ── normalize_output(순수): 표 변환 ───────────────────────────────
def test_normalize_output_converts_html_table_in_place_keeping_surrounding_text() -> None:
    # Arrange: 쪽 전사라 표 앞뒤 본문을 잃으면 안 된다(청커는 응답 전체를 표로 봤다).
    text = (
        "진료비 영수증\n"
        "<table><tr><td>환자 성명</td><td>홍길동</td></tr>"
        "<tr><td>진찰료</td><td>10,000</td></tr></table>\n"
        "발급일 2026-01-01"
    )

    # Act
    result = vlm_client.normalize_output(text)

    # Assert
    assert result == (
        "진료비 영수증\n\n"
        "| 환자 성명 | 홍길동 |\n|---|---|\n| 진찰료 | 10,000 |\n\n"
        "발급일 2026-01-01"
    )


def test_normalized_table_keeps_label_value_detectable_by_masking() -> None:
    # Arrange: 변환의 목적 — HTML 그대로면 라벨과 값 사이에 태그가 껴 이름 정규식이 끊긴다.
    text = "<table><tr><td>환자 성명</td><td>홍길동</td></tr></table>"
    masker = Masker(use_ner=False)

    # Act
    normalized = vlm_client.normalize_output(text)

    # Assert
    assert [span.label for span in masker.detect(text)] == []
    assert [span.label for span in masker.detect(normalized)] == [PiiLabel.NAME]


def test_normalize_output_html_rowspan_repeats_down_colspan_fills_first_only() -> None:
    # Arrange: 세로 병합은 값 반복, 가로 병합은 첫 칸만 값(가로까지 반복하면 문단이 복제된다).
    text = (
        '<table><tr><td rowspan="2">입원</td><td colspan="2">기간</td></tr>'
        "<tr><td>3일</td><td>2인실</td></tr></table>"
    )

    # Act
    result = vlm_client.normalize_output(text)

    # Assert
    assert result == "| 입원 | 기간 |  |\n|---|---|---|\n| 입원 | 3일 | 2인실 |"


def test_normalize_output_handles_thead_tbody() -> None:
    # Arrange
    text = (
        "<table><thead><tr><th>항목</th><th>금액</th></tr></thead>"
        "<tbody><tr><td>진찰료</td><td>10,000</td></tr></tbody></table>"
    )

    # Act
    result = vlm_client.normalize_output(text)

    # Assert
    assert result == "| 항목 | 금액 |\n|---|---|\n| 진찰료 | 10,000 |"


def test_normalize_output_cell_line_breaks_become_spaces_and_pipes_escaped() -> None:
    # Arrange: <br>을 남기면 "성명<br>홍길동"의 이름을 정규식이 못 잡는다. "|"는 행을 깨므로 치환.
    text = "<table><tr><td>성명<br/>홍길동</td><td>A|B</td><td>&nbsp;김철수</td></tr></table>"

    # Act
    result = vlm_client.normalize_output(text)

    # Assert
    slash = vlm_client._CELL_PIPE_SUBSTITUTE  # 전각 슬래시(U+FF0F)
    assert result.splitlines()[0] == f"| 성명 홍길동 | A{slash}B | 김철수 |"


def test_normalize_output_caps_degenerate_colspan() -> None:
    # Arrange: 퇴행 출력의 거대한 colspan이 행을 폭발시키지 않게 자른다.
    text = '<table><tr><td colspan="99999">x</td></tr></table>'

    # Act
    result = vlm_client.normalize_output(text)

    # Assert
    assert result.splitlines()[0].count("|") == 65  # 셀 64개 = 구분자 65개


def test_normalize_output_strips_leftover_tags_of_nested_table() -> None:
    # Arrange: 중첩 표는 바깥 닫힘 태그가 짝을 잃는다 — 태그는 걷고 글자는 남긴다.
    text = "<table><tr><td><table><tr><td>내부</td></tr></table></td></tr></table> 끝"

    # Act
    result = vlm_client.normalize_output(text)

    # Assert
    assert "<" not in result
    assert "내부" in result
    assert "끝" in result


def test_normalize_output_keeps_text_of_table_without_rows() -> None:
    # Act
    result = vlm_client.normalize_output("<table>설명만</table>")

    # Assert
    assert result == "설명만"


def test_normalize_output_converts_otsl_with_merges() -> None:
    # Arrange: <ucel>은 위 행 값 복제(세로 병합), <lcel>은 빈 칸(가로 병합).
    text = (
        "<fcel>항목<fcel>금액<nl><fcel>진찰료<fcel>10,000<nl>"
        "<ucel><fcel>5,000<nl><fcel>합계<lcel><fcel>15,000<nl>"
    )

    # Act
    result = vlm_client.normalize_output(text)

    # Assert
    assert result == (
        "| 항목 | 금액 |  |\n|---|---|---|\n"
        "| 진찰료 | 10,000 |  |\n| 진찰료 | 5,000 |  |\n| 합계 |  | 15,000 |"
    )


def test_normalize_output_keeps_text_after_otsl_table() -> None:
    # Arrange: 마지막 <nl> 뒤 본문이 행 토큰에 흡수돼 사라지면 안 된다.
    text = "<fcel>성명<fcel>홍길동<nl>위 금액을 영수합니다"

    # Act
    result = vlm_client.normalize_output(text)

    # Assert
    assert result == "| 성명 | 홍길동 |\n|---|---|\n\n위 금액을 영수합니다"


def test_normalize_output_strips_code_fence_and_superscripts() -> None:
    # Arrange: PaddleOCR-VL은 위첨자를 LaTeX 조각으로 낸다.
    text = "```markdown\n보장 \\(^{*}\\) 항목 $ ^{1} $\n```"

    # Act
    result = vlm_client.normalize_output(text)

    # Assert
    assert result == "보장 * 항목 1"


# ── normalize_output(순수): 표 밖 태그 정리 — PII 경계(QA W2) ─────
# 남은 태그가 라벨-값·숫자 묶음 사이에 끼면 마스킹 정규식과 잔류 검사를 둘 다 빠져나간다.
# 치환 기준은 masking/patterns.py·verify.py의 구분자다: <br>→공백(카드번호 [- \t]가 개행을 못
# 넘는다), 인라인 태그→제거(렌더링도 공백을 안 만든다), 블록 태그→줄바꿈(단어가 붙지 않게).
@pytest.mark.parametrize(
    ("raw", "secret", "label"),
    [
        ("주민등록번호 901010<br>1234567", "1234567", PiiLabel.RRN),
        ("<b>901010</b>-1234567", "1234567", PiiLabel.RRN),
        ("<fcel>주민등록번호<fcel>901010<br>1234567<nl>", "1234567", PiiLabel.RRN),
        ("카드번호 1234<br>5678<br>9012<br>3456", "9012", PiiLabel.CARD),
        ("카드번호 1234&nbsp;5678&nbsp;9012&nbsp;3456", "9012", PiiLabel.CARD),
        ("연락처 010<br>1234<br>5678", "5678", PiiLabel.PHONE),
        ("<b>환자 성명</b> 홍길동", "홍길동", PiiLabel.NAME),
        ("<p>성명: 홍길동</p><p>입원일 2026-01-01</p>", "홍길동", PiiLabel.NAME),
        # QA 2회차 W2-R: 각주·위첨자를 값에 붙이면 숫자 경계가 깨진다(dev·1회차에선 가려졌다).
        ("901010-1234567<sup>1</sup>", "1234567", PiiLabel.RRN),
        ("<sup>1</sup>901010-1234567", "1234567", PiiLabel.RRN),
        ("연락처 010-1234-5678<sup>2</sup>", "5678", PiiLabel.PHONE),
        ("연락처 <sup>3</sup>010-1234-5678", "5678", PiiLabel.PHONE),
        ("카드 1234-5678-9012-3456<sup>1</sup>", "3456", PiiLabel.CARD),
        ("환자 성명 홍길동<b>입니다</b>", "홍길동", PiiLabel.NAME),
        # LaTeX 위첨자(PaddleOCR-VL이 위첨자를 내는 형태)도 같은 규칙.
        ("901010-1234567$^{1}$", "1234567", PiiLabel.RRN),
        ("901010-1234567\\(^{2}\\)", "1234567", PiiLabel.RRN),
        # 하이픈 바로 뒤 <br>(줄 넘김으로 잘린 값) — 표 셀 안 포함.
        ("901010-<br>1234567", "1234567", PiiLabel.RRN),
        (
            "<table><tr><td>주민등록번호</td><td>901010-<br>1234567</td></tr></table>",
            "1234567",
            PiiLabel.RRN,
        ),
        ("연락처 010-1234-<br>5678", "5678", PiiLabel.PHONE),
        ("카드 1234-5678-<br>9012-3456", "3456", PiiLabel.CARD),
        ("<fcel>주민등록번호<fcel>901010-\\n1234567<nl>", "1234567", PiiLabel.RRN),
        ("<table><tr><td>901010-\n1234567</td></tr></table>", "1234567", PiiLabel.RRN),
        # 셀 안에 남은 블록 태그는 공백으로 — 붙이면 "홍길동입원"이 돼 이름 경계가 사라진다.
        (
            "<table><tr><td>성명</td><td><div>홍길동</div><div>입원</div></td></tr></table>",
            "홍길동",
            PiiLabel.NAME,
        ),
    ],
)
def test_normalized_markup_keeps_pii_maskable(raw: str, secret: str, label: PiiLabel) -> None:
    # Arrange
    masker = Masker(use_ner=False)

    # Act
    masked = masker.mask(vlm_client.normalize_output(raw))

    # Assert
    assert secret not in masked
    assert label.token in masked


@pytest.mark.parametrize(
    "raw",
    [
        "주민등록번호 901010<br>1234567",
        "<fcel>주민등록번호<fcel>901010<br>1234567<nl>",
        "카드번호 1234<br>5678<br>9012<br>3456",
        "카드번호 1234&nbsp;5678&nbsp;9012&nbsp;3456",
        "901010-1234567<sup>1</sup>",
        "901010-1234567$^{1}$",
        "카드 1234-5678-9012-3456<sup>1</sup>",
        "901010-<br>1234567",
    ],
)
def test_normalized_markup_leaves_high_sensitivity_pii_visible_to_residual_gate(raw: str) -> None:
    # Arrange: 정리된 원문을 마스킹 없이 잔류 게이트에 넣는다 — 마스커가 놓쳐도 게이트가 막아야
    # 한다(정리 전에는 "901010<br>1234567"이 게이트도 통과했다).
    normalized = vlm_client.normalize_output(raw)

    # Act / Assert
    with pytest.raises(MaskingError):
        assert_no_residual(normalized)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # <sup>·<sub>·LaTeX 위첨자는 별도 토큰 — 글자·숫자와 맞닿으면 공백 하나.
        ("901010-1234567<sup>1</sup>", "901010-1234567 1"),
        ("<sup>1</sup>901010-1234567", "1 901010-1234567"),
        ("901010-1234567$^{1}$", "901010-1234567 1"),
        ("보장\\(^{*}\\) 항목", "보장* 항목"),  # 기호는 그대로 붙는다
        ("H<sub>2</sub>O", "H 2 O"),
        # 서식 태그는 렌더링처럼 붙인다 — 수 하나를 감싼 서식, 영숫자 식별자(KCD).
        ("<b>9010</b>10-1234567", "901010-1234567"),
        ("상병코드 J<b>20.9</b>", "상병코드 J20.9"),
        # 단 양옆이 둘 다 한글이면 띄운다(이름 + 조사·서술어). 붙은 태그는 한 덩어리로 본다.
        ("환자 성명 홍길동<b>입니다</b>", "환자 성명 홍길동 입니다"),
        ("<b>환자 성명</b><b>홍길동</b>", "환자 성명 홍길동"),
        # 하이픈 바로 뒤 줄 넘김만 붙인다(줄 앞 하이픈 — 목록 기호 — 은 그대로 띄운다).
        ("901010-<br>1234567", "901010-1234567"),
        ("진단명<br>- 위염", "진단명 - 위염"),
    ],
)
def test_normalize_output_tag_spacing_rule(raw: str, expected: str) -> None:
    # Act
    result = vlm_client.normalize_output(raw)

    # Assert
    assert result == expected


def test_normalize_output_separates_block_tags_and_drops_inline_tags() -> None:
    # Arrange
    text = "<div><b>성명</b>: 홍길동</div><p>연락처: 010-1234-5678</p>"

    # Act
    result = vlm_client.normalize_output(text)

    # Assert: 블록 경계는 줄로 남고(이름이 다음 단어와 붙지 않는다), 인라인 태그는 흔적 없이 빠진다.
    assert result == "성명: 홍길동\n\n연락처: 010-1234-5678"


def test_normalize_output_keeps_angle_brackets_that_are_not_html() -> None:
    # Arrange: 이름을 모르는 꺾쇠(autolink)와 비교식은 본문일 수 있다 — 지우면 글자를 잃는다.
    text = "홈페이지 <https://www.example.co.kr> 혈압 < 140 > 90"

    # Act
    result = vlm_client.normalize_output(text)

    # Assert
    assert result == text


# ── normalize_output: 정리 단계 견고성(QA W3) ─────────────────────
def test_normalize_output_bounds_huge_span_digits() -> None:
    # Arrange: int()는 4,300자리를 넘는 문자열에 ValueError를 낸다 — 정규식이 자릿수를 묶어야 한다.
    text = '<table><tr><td colspan="' + "9" * 4301 + '">x</td></tr></table>'

    # Act
    result = vlm_client.normalize_output(text)

    # Assert: 예외 없이 상한(64칸)으로 잘린다.
    assert result.splitlines()[0].count("|") == 65


async def test_transcribe_table_converts_normalization_failure_without_response_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 정리 단계가 예상 밖 예외를 낸다(메시지에 응답 조각이 실린 최악의 경우를 흉내).
    def broken_normalize(text: str) -> str:
        raise ValueError(f"invalid literal: {text}")

    monkeypatch.setattr(vlm_client, "normalize_output", broken_normalize)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion("환자 성명 홍길동 901010-1234567"))

    _install_mock(handler)

    # Act: VlmClientError여야 파이프라인이 그 쪽만 surya로 폴백한다(그 밖의 예외면 작업 실패).
    with pytest.raises(vlm_client.VlmClientError) as exc_info:
        await vlm_client.transcribe_table(_image())

    # Assert: 사유는 고정 어휘, 메시지·원인 체인에 응답 원문이 없다(체인은 끊는다).
    assert exc_info.value.reason == "malformed"
    assert "홍길동" not in repr(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


async def test_reread_lines_normalization_failure_is_line_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 라인 재판독에서도 정리 실패는 그 라인만 None이다(작업·다른 라인은 산다).
    def broken_normalize(text: str) -> str:
        raise ValueError("정리 실패")

    monkeypatch.setattr(vlm_client, "normalize_output", broken_normalize)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion("x"))

    _install_mock(handler)

    # Act
    with capture_logs() as logs:
        result = await vlm_client.reread_lines([_image(200, 100)], [_page(_line("a", _BBOX_A))])

    # Assert
    assert result == [[None]]
    summary = next(entry for entry in logs if entry["event"] == "vlm_line_reread")
    assert summary["failure_reasons"] == {"malformed": 1}


# ── reread_lines: 라인 재판독 ────────────────────────────────────
async def test_reread_lines_sends_one_line_crop_per_line_with_ocr_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setattr(vlm_client.settings, "vlm_reread_max_tokens", 128)
    monkeypatch.setattr(vlm_client.settings, "vlm_reread_timeout_seconds", 30.0)
    monkeypatch.setattr(vlm_client.settings, "vlm_connect_timeout_seconds", 5.0)
    seen: list[tuple[tuple[int, int], str, int, float, float]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        timeout = request.extensions["timeout"]
        seen.append(
            (
                _sent_image_size(request),
                _prompt(request),
                _body(request)["max_tokens"],
                timeout["read"],
                timeout["connect"],
            )
        )
        return httpx.Response(200, json=_completion("x"))

    _install_mock(handler)
    page = _page(_line("a", _BBOX_A), _line("b", _BBOX_B))

    # Act
    await vlm_client.reread_lines([_image(200, 100)], [page])

    # Assert: 라인마다 bbox + 여백 crop 1장(원본 해상도), 태스크 토큰, 라인용 상한·타임아웃.
    assert sorted(seen) == [
        ((_WIDTH_A, 28), "OCR:", 128, 30.0, 5.0),
        ((_WIDTH_B, 28), "OCR:", 128, 30.0, 5.0),
    ]


async def test_reread_lines_returns_texts_in_line_order() -> None:
    # Arrange: 요청은 신뢰도 순으로 나가도 결과는 원래 라인 자리로 돌아와야 한다.
    by_width = {_WIDTH_A: "첫째 줄", _WIDTH_B: "둘째 줄", _WIDTH_C: "셋째 줄"}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion(by_width[_sent_image_size(request)[0]]))

    _install_mock(handler)
    page = _page(
        _line("a", _BBOX_A, confidence=0.9),
        _line("b", _BBOX_B, confidence=0.2),
        _line("c", _BBOX_C, confidence=0.5),
    )

    # Act
    result = await vlm_client.reread_lines([_image(200, 100)], [page])

    # Assert
    assert result == [["첫째 줄", "둘째 줄", "셋째 줄"]]


async def test_reread_lines_requests_lowest_confidence_lines_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 예산이 끝나면 남는 게 신뢰도 높은 라인이어야 한다(오독은 저신뢰 라인에 몰린다).
    monkeypatch.setattr(vlm_client.settings, "vlm_concurrency", 1)
    order: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        order.append(_sent_image_size(request)[0])
        return httpx.Response(200, json=_completion("x"))

    _install_mock(handler)
    first_page = _page(_line("a", _BBOX_A, confidence=0.9), _line("b", _BBOX_B, confidence=0.2))
    second_page = _page(_line("c", _BBOX_C, confidence=0.5), index=1)

    # Act
    await vlm_client.reread_lines([_image(200, 100), _image(200, 100)], [first_page, second_page])

    # Assert: 페이지를 가로질러 신뢰도 오름차순(0.2 → 0.5 → 0.9)
    assert order == [_WIDTH_B, _WIDTH_C, _WIDTH_A]


async def test_reread_lines_failed_line_is_none_and_other_lines_survive() -> None:
    # Arrange: 한 라인의 실패가 작업이나 다른 라인을 죽이지 않는다(fail-open, 라인 단위).
    def handler(request: httpx.Request) -> httpx.Response:
        if _sent_image_size(request)[0] == _WIDTH_B:
            return httpx.Response(500, json={"error": "boom"})
        return httpx.Response(200, json=_completion("x"))

    _install_mock(handler)
    page = _page(_line("a", _BBOX_A), _line("b", _BBOX_B), _line("c", _BBOX_C))

    # Act
    with capture_logs() as logs:
        result = await vlm_client.reread_lines([_image(200, 100)], [page])

    # Assert: 실패한 라인만 None. 실패 수·사유는 로그 1건으로 남는다.
    assert result == [["x", None, "x"]]
    summaries = [entry for entry in logs if entry["event"] == "vlm_line_reread"]
    assert len(summaries) == 1
    assert summaries[0]["log_level"] == "warning"
    assert summaries[0]["lines"] == 3
    assert summaries[0]["reread"] == 2
    assert summaries[0]["failed"] == 1
    assert summaries[0]["failure_reasons"] == {"http": 1}


async def test_reread_lines_truncated_or_empty_output_is_none() -> None:
    # Arrange: max_tokens에서 잘린 줄(반복 폭주)과 빈 응답은 쓰지 않고, 정상 줄은 한 줄로 편다.
    def handler(request: httpx.Request) -> httpx.Response:
        width = _sent_image_size(request)[0]
        if width == _WIDTH_A:
            return httpx.Response(200, json=_completion("홍홍홍홍", "length"))
        if width == _WIDTH_B:
            return httpx.Response(200, json=_completion(""))
        return httpx.Response(200, json=_completion("  홍  길동 \n"))

    _install_mock(handler)
    page = _page(_line("a", _BBOX_A), _line("b", _BBOX_B), _line("c", _BBOX_C))

    # Act
    with capture_logs() as logs:
        result = await vlm_client.reread_lines([_image(200, 100)], [page])

    # Assert
    assert result == [[None, None, "홍 길동"]]
    summary = next(entry for entry in logs if entry["event"] == "vlm_line_reread")
    assert summary["failure_reasons"] == {"truncated": 1}
    assert summary["empty"] == 1


async def test_reread_lines_skips_degenerate_bbox_without_request() -> None:
    # Arrange: 좌표 역전·화면 밖 bbox는 보낼 픽셀이 없다 — 요청하지 않고 None.
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_completion("x"))

    _install_mock(handler)
    page = _page(
        _line("역전", (50.0, 50.0, 40.0, 60.0)),
        _line("화면 밖", (300.0, 300.0, 320.0, 310.0)),
        _line("정상", _BBOX_A),
    )

    # Act
    with capture_logs() as logs:
        result = await vlm_client.reread_lines([_image(200, 100)], [page])

    # Assert
    assert result == [[None, None, "x"]]
    assert len(requests) == 1
    summary = next(entry for entry in logs if entry["event"] == "vlm_line_reread")
    assert summary["skipped"] == 2
    assert summary["failed"] == 0


async def test_reread_lines_respects_concurrency_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: 같은 vLLM을 청커와 나눠 쓴다 — 문서 1건이 거는 동시 요청은 상한까지.
    monkeypatch.setattr(vlm_client.settings, "vlm_concurrency", 2)
    in_flight = 0
    peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return httpx.Response(200, json=_completion("x"))

    _install_mock(handler)
    lines = [_line(f"l{i}", (10.0, 5.0 + i * 15, 60.0, 15.0 + i * 15)) for i in range(6)]

    # Act
    result = await vlm_client.reread_lines([_image(200, 100)], [_page(*lines)])

    # Assert
    assert result == [["x"] * 6]
    assert peak == 2


async def test_reread_lines_budget_exceeded_leaves_remaining_lines_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: 서버가 한 라인에서 멎는다. 단건 타임아웃만으론 라인 수에 비례해 SQS 가시성을
    # 넘길 수 있으므로, 단계 예산이 끝나면 남은 라인은 건너뛰고 끝난 결과는 그대로 쓴다.
    monkeypatch.setattr(vlm_client.settings, "vlm_concurrency", 1)
    monkeypatch.setattr(vlm_client.settings, "vlm_reread_budget_seconds", 0.2)
    never = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if _sent_image_size(request)[0] == _WIDTH_A:
            return httpx.Response(200, json=_completion("먼저"))
        await never.wait()  # 응답하지 않는 서버
        return httpx.Response(200, json=_completion("도달 안 함"))

    _install_mock(handler)
    page = _page(
        _line("a", _BBOX_A, confidence=0.1),
        _line("b", _BBOX_B, confidence=0.5),
        _line("c", _BBOX_C, confidence=0.9),
    )

    # Act: 예산이 동작하지 않으면 영원히 멎으므로 상한을 둔다.
    with capture_logs() as logs:
        async with asyncio.timeout(5):
            result = await vlm_client.reread_lines([_image(200, 100)], [page])

    # Assert
    assert result == [["먼저", None, None]]
    summary = next(entry for entry in logs if entry["event"] == "vlm_line_reread")
    assert summary["failure_reasons"] == {"budget": 2}


async def test_reread_lines_uses_line_instruction_for_non_paddle_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setattr(vlm_client.settings, "vlm_model", "qwen2.5vl:7b")
    prompts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        prompts.append(_prompt(request))
        return httpx.Response(200, json=_completion("x"))

    _install_mock(handler)

    # Act
    await vlm_client.reread_lines([_image(200, 100)], [_page(_line("a", _BBOX_A))])

    # Assert
    assert prompts == [vlm_client.VLM_LINE_PROMPT]


async def test_reread_lines_logs_counts_but_never_reread_text() -> None:
    # Arrange: 재판독 텍스트는 마스킹 전 평문이다(§9).
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion("홍길동 901010-1234567"))

    _install_mock(handler)

    # Act
    with capture_logs() as logs:
        await vlm_client.reread_lines([_image(200, 100)], [_page(_line("828", _BBOX_A))])

    # Assert
    dumped = json.dumps(logs, default=str, ensure_ascii=False)
    assert "홍길동" not in dumped
    assert "901010" not in dumped


async def test_reread_lines_without_lines_sends_nothing() -> None:
    # Arrange
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_completion("x"))

    _install_mock(handler)

    # Act
    result = await vlm_client.reread_lines([_image(200, 100)], [_page()])

    # Assert
    assert result == [[]]
    assert requests == []


async def test_reread_lines_rejects_image_page_count_mismatch() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="수 불일치"):
        await vlm_client.reread_lines([_image(), _image()], [_page(_line("a", _BBOX_A))])
