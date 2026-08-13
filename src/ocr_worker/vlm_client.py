"""ollama VLM(vision) 클라이언트 — 표 구조 보존 보조 추출(하이브리드 경로).

core/ai_client.py는 OpenAI 호환 chat/embeddings 전용이라 이미지 페이로드를 지원하지
않는다. VLM은 다중 항목 표 문서(지급결과서·청구서·입원확인서·진료비영수증)에서
surya 라인 순서가 표 구조를 보존하지 못하는 문제를 보완하는 OCR 전용 보조 단계라
core가 아닌 여기 별도 모듈로 둔다.
"""

import base64
import io
import json
from typing import Any

import httpx

from core.config import settings
from ocr_worker.masking.spans import PiiLabel
from ocr_worker.ocr import PageImage

_vlm_client: httpx.AsyncClient | None = None

VLM_TABLE_PROMPT = (
    "이 이미지는 한국 보험/의료 문서입니다. 이미지 안의 모든 텍스트를 표 구조를 "
    "유지해 가능한 한 정확히 그대로 옮겨 적어주세요. 표뿐 아니라 문서 상단의 "
    "발급기관·환자/계약자 정보 같은 라벨-값 블록도 빠짐없이 포함해주세요. "
    "표는 마크다운 표로, 항목-금액 쌍은 누락 없이 옮겨주세요. 추측하지 말고 "
    "보이는 대로만 적어주세요. 특정 부분이 흐리거나 읽을 수 없으면 내용을 "
    "지어내지 말고 '[읽을 수 없음]'이라고만 적어주세요."
)

# PiiLabel의 값(예: "이름", "주민등록번호")을 그대로 라벨 어휘로 써서, 응답을 파싱할
# 때 별도 매핑표 없이 PiiLabel(raw_label)로 바로 역직렬화한다.
GROUNDING_PROMPT = (
    "이 이미지에서 개인정보(사람 이름, 주민등록번호, 전화번호, 환자등록번호, 계좌번호, "
    "주소)가 적힌 위치를 찾아주세요. 각 항목마다 종류(label)와 위치를 bounding box로 "
    "알려주세요. label은 문서에 적힌 표현(예: '환자 성명', '연락처')이 아니라 반드시 "
    "다음 6개 단어 중 하나만 쓰세요: 이름, 주민등록번호, 전화번호, 환자등록번호, "
    "계좌번호, 주소. 좌표는 이미지 왼쪽 위를 (0,0), 오른쪽 아래를 (1000,1000)으로 "
    "정규화해서 JSON 배열로만 답하세요. 형식: "
    '[{"label": "이름", "bbox": [x1,y1,x2,y2]}, ...] '
    "다른 설명 없이 JSON만 출력하세요. 개인정보가 없으면 빈 배열 []만 출력하세요."
)


class VlmClientError(RuntimeError):
    """VLM 호출 실패의 기반 예외."""


def _get_vlm_client() -> httpx.AsyncClient:
    """VLM 추론용 공유 AsyncClient(지연 생성)."""
    global _vlm_client
    if _vlm_client is None:
        _vlm_client = httpx.AsyncClient(
            base_url=settings.vlm_base_url, timeout=settings.vlm_timeout_seconds
        )
    return _vlm_client


async def close_client() -> None:
    """공유 AsyncClient를 종료한다(앱 종료 시 1회, core/ai_client.close_client와 함께 호출)."""
    global _vlm_client
    if _vlm_client is not None:
        await _vlm_client.aclose()
    _vlm_client = None


async def transcribe_table(image: PageImage) -> str:
    """페이지 이미지 1장을 VLM으로 표 구조 보존 전사한다.

    Args:
        image: ``render_to_images``가 만든 PIL 페이지 이미지.

    Returns:
        마크다운 표를 포함한 전사 텍스트(원문 그대로 — PII 포함 가능, 호출측이 마스킹).

    Raises:
        VlmClientError: HTTP 오류·타임아웃·응답 형식 이상.
    """
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    client = _get_vlm_client()
    payload: dict[str, Any] = {
        "model": settings.vlm_model,
        "prompt": VLM_TABLE_PROMPT,
        "images": [b64],
        "stream": False,
        # temperature=0: 창작 경향을 낮춰 환각(이미지에 없는 내용 지어내기)을 줄인다.
        # 완전히 막진 못하므로 pipeline._looks_grounded()가 별도 안전장치로 검증한다.
        # num_ctx: 명시하지 않으면 ollama 기본값에 걸려 페이지 이미지 토큰만으로 컨텍스트
        # 대부분이 차, 표가 많은 문서에서 응답이 에러 없이(200 OK) 문장 중간에서 잘렸다
        # (실측 확인). settings.vlm_num_ctx 참고.
        "options": {"temperature": 0.0, "num_ctx": settings.vlm_num_ctx},
    }
    try:
        resp = await client.post("/api/generate", json=payload)
        resp.raise_for_status()
        return resp.json()["response"]
    except httpx.HTTPError as exc:
        raise VlmClientError("VLM 표 전사 호출 실패") from exc
    except (KeyError, TypeError, ValueError) as exc:
        # ValueError: resp.json()이 비-JSON 본문에서 던지는 json.JSONDecodeError 포함.
        # 여기서 변환 안 하면 예외가 그대로 전파돼 pipeline._extract_pages의 surya
        # 폴백을 못 타고 작업 자체가 실패한다(코드리뷰 지적).
        raise VlmClientError("VLM 응답 형식이 올바르지 않음") from exc


async def ground_pii(image: PageImage) -> list[tuple[PiiLabel, tuple[int, int, int, int]]]:
    """이미지에서 PII로 보이는 영역의 위치를 찾아 픽셀 bbox로 반환한다(이미지 마스킹 보조).

    surya가 텍스트를 완전히 다른 문자로 오독하면(예: 이름을 숫자열로 오독) 이미지
    검은블록은 그 라인을 PII로 판정하지 못해 그대로 노출한다(§masked_lines 문서 참고) —
    이 함수는 surya 판독과 무관하게 VLM이 직접 이미지에서 PII 위치를 짚게 해 그 gap을
    메운다. 실측 확인: 이름을 "828"로 완전히 오독한 케이스에서도 VLM은 올바른 텍스트와
    위치를 정확히 짚었다(좌표는 반드시 0~1000 정규화로 요청해야 한다 — 같은 모델도
    픽셀 좌표로 물으면 엉뚱한 영역을 가리켰다, 실측 확인).

    호출측(``ImageMasker``)은 이 결과를 기존 라인 기반 검은블록에 **추가만** 한다 —
    실패하거나 좌표가 틀려도 최악의 경우 과잉 마스킹일 뿐, 기존 안전성보다 후퇴하지
    않는다. 그래서 이 함수는 예외를 던지지 않고 실패 시 빈 리스트를 반환한다.

    Args:
        image: ``render_to_images``가 만든 PIL 페이지 이미지.

    Returns:
        ``(PiiLabel, (x1,y1,x2,y2))`` 목록(픽셀 좌표). 실패·형식 이상이면 빈 리스트.
    """
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    client = _get_vlm_client()
    payload: dict[str, Any] = {
        # vlm_model(표 전사)과 별도 모델 — 실측: 표 전사에 더 정확한 모델이 grounding
        # 좌표는 오히려 부정확했다(설정 docstring 참고). 좌표 정밀도가 안전에 직결돼
        # 검증된 모델을 고정해서 쓴다.
        "model": settings.vlm_grounding_model,
        "prompt": GROUNDING_PROMPT,
        "images": [b64],
        "stream": False,
        # num_ctx: transcribe_table과 동일 이유(§ 위 주석) — 짧은 JSON 응답이라 출력
        # 잘림 위험은 낮지만, 입력(이미지) 처리에도 같은 컨텍스트를 쓰므로 일관되게 맞춘다.
        "options": {"temperature": 0.0, "num_ctx": settings.vlm_num_ctx},
    }
    try:
        resp = await client.post("/api/generate", json=payload)
        resp.raise_for_status()
        text = resp.json()["response"]
    except httpx.HTTPError:
        return []
    except (KeyError, TypeError, ValueError):
        return []
    return _parse_grounding_boxes(text, image.width, image.height)


# 실측 확인: VLM은 프롬프트가 지시한 단일 단어("이름") 대신 문서 자체의 필드 캡션을
# 그대로 라벨로 쓰는 경향이 있다(예: "환자 성명", "연락처") — PiiLabel 값과 정확히
# 일치하지 않아 엄격한 PiiLabel(raw_label)이 조용히 다 놓치는 사례를 실측으로 확인했다
# (이름이 하나도 안 가려짐). 부분 문자열 매칭으로 완화한다.
_LABEL_ALIASES: dict[PiiLabel, tuple[str, ...]] = {
    PiiLabel.NAME: ("이름", "성명"),
    PiiLabel.RRN: ("주민등록번호", "주민번호"),
    PiiLabel.PHONE: ("전화번호", "연락처", "휴대전화", "휴대폰"),
    PiiLabel.PATIENT_ID: ("환자등록번호", "환자번호", "접수번호"),
    PiiLabel.ACCOUNT: ("계좌번호", "계좌"),
    PiiLabel.ADDRESS: ("주소",),
}


def _match_label(raw_label: str) -> PiiLabel | None:
    """VLM이 자유롭게 쓴 라벨 문자열을 ``PiiLabel``로 정규화한다(부분 문자열 매칭)."""
    for label, aliases in _LABEL_ALIASES.items():
        if any(alias in raw_label for alias in aliases):
            return label
    return None


def _parse_grounding_boxes(
    text: str, width: int, height: int
) -> list[tuple[PiiLabel, tuple[int, int, int, int]]]:
    """VLM 응답(순수 JSON 또는 ```json 코드펜스)을 픽셀 bbox 목록으로 변환한다.

    참고용 보조 신호라 fail-closed일 필요는 없다 — 형식이 깨졌거나 항목 하나가
    이상해도 예외를 던지지 않고, 파싱 가능한 항목만 채택하고 나머지는 건너뛴다.
    """
    stripped = text.strip().strip("`")
    if stripped.startswith("json"):
        stripped = stripped[4:]
    try:
        items = json.loads(stripped)
    except ValueError:
        return []
    if not isinstance(items, list):
        return []
    boxes: list[tuple[PiiLabel, tuple[int, int, int, int]]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        raw_label = item.get("label")
        bbox = item.get("bbox")
        if not isinstance(raw_label, str) or not (isinstance(bbox, list) and len(bbox) == 4):
            continue
        label = _match_label(raw_label)
        if label is None:
            continue  # 알려진 PII 분류와 매칭 안 됨 — 무시
        try:
            x1, y1, x2, y2 = (float(v) for v in bbox)
        except (TypeError, ValueError):
            continue
        if not (0 <= x1 < x2 <= 1000 and 0 <= y1 < y2 <= 1000):
            continue  # 좌표 역전·범위 밖 — 이 항목만 무시
        boxes.append(
            (
                label,
                (
                    int(x1 / 1000 * width),
                    int(y1 / 1000 * height),
                    int(x2 / 1000 * width),
                    int(y2 / 1000 * height),
                ),
            )
        )
    return boxes
