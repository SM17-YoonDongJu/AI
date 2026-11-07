"""OCR 이미지 빌드 검사 — 모델 프리페치와 같은 프로세스 스모크.

실행: ``python -m ocr_worker.build_check {prefetch | smoke --font <ttf>}``.

Dockerfile 전용이다. 런타임 코드 경로(``PaddleOcrEngine``·``NerDetector``)를 그대로 써서 둘을 한다:

- ``prefetch``: 설정의 OCR 모델(검출·인식·줄 방향)과 PoC용 mobile 검출 모델, NER 모델을 받아
  이미지 안(``PADDLE_PDX_CACHE_HOME``·``HF_HOME`` = ``/opt/models``)에 넣는다. 런타임 모델
  다운로드·외부 egress가 없어지고, ``/root/.cache``에 PVC가 붙어도 가려지지 않는다.
- ``smoke``: 외부 연결을 끈 상태로 **같은 프로세스에서** PaddleOCR(한국어 합성 이미지 1장) →
  NER(문장 1개) → PaddleOCR 다시를 실제로 돌린다. paddle과 torch를 한 프로세스에 올렸을 때의
  OpenMP 충돌·모델 로드 실패를 배포 전에(빌드에서) 잡는다. 워커와 같은 순서(OCR 뒤 NER)다.

출력은 개수·신뢰도·메모리만 남긴다(합성 문장이지만 원문을 찍는 습관을 들이지 않는다, §9).
"""

import argparse
import resource
import sys

from core.config import get_settings
from ocr_worker.masking.ner_detector import NerDetector
from ocr_worker.masking.spans import PiiLabel
from ocr_worker.ocr import PaddleOcrEngine, PageImage

# PoC에서 env(OCR_DET_MODEL)로 바꿔 볼 수 있게 이미지에 함께 넣는 검출 모델.
POC_DET_MODELS = ("PP-OCRv5_mobile_det",)
# 합성 이미지에 그릴 문장 — 인식 결과에 이 두 단어가 있어야 통과한다.
_SMOKE_LINES = ("보험금 청구서", "환자 성명 홍길동")
_SMOKE_WORDS = ("청구서", "홍길동")
_SMOKE_NER_SENTENCE = "홍길동 씨가 어제 서울대학교병원에 입원했습니다."


def prefetch() -> None:
    """설정 모델 + PoC 검출 모델 + NER 모델을 캐시 디렉터리로 받는다(빌드 때 1회)."""
    settings = get_settings()
    for det_model in dict.fromkeys((settings.ocr_det_model, *POC_DET_MODELS)):
        PaddleOcrEngine(
            det_model=det_model,
            rec_model=settings.ocr_rec_model,
            textline_orientation=True,  # 줄 방향 모델도 받아 둔다(설정으로 켜고 끌 수 있게)
            cpu_threads=settings.ocr_cpu_threads,
            enable_mkldnn=settings.ocr_enable_mkldnn,
        ).load()
    NerDetector(settings.ner_model, settings.ner_score_threshold).load()
    print(f"prefetch ok: det={len(dict.fromkeys((settings.ocr_det_model, *POC_DET_MODELS)))}종")


def smoke(font_path: str) -> None:
    """같은 프로세스에서 OCR → NER → OCR을 돌려 로드·추론·OpenMP 공존을 확인한다.

    Raises:
        SystemExit: 인식 결과에 기대 단어가 없거나 NER이 이름을 못 찾으면(빌드 실패).
    """
    settings = get_settings()
    engine = PaddleOcrEngine.from_settings(settings)
    image = _synthetic_page(font_path)

    first = _check_ocr(engine, image, "ocr#1")
    spans = NerDetector(settings.ner_model, settings.ner_score_threshold).detect(
        _SMOKE_NER_SENTENCE
    )
    names = [span for span in spans if span.label is PiiLabel.NAME]
    if not names:
        raise SystemExit("빌드 스모크 실패: NER이 이름을 찾지 못했다")
    second = _check_ocr(engine, image, "ocr#2(NER 로드 뒤)")
    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # linux: KB
    print(f"smoke ok: ocr_lines={first}/{second} ner_names={len(names)} peak_rss_mb={rss_mb:.0f}")


def _check_ocr(engine: PaddleOcrEngine, image: PageImage, label: str) -> int:
    """한 쪽을 인식해 기대 단어가 다 나왔는지 본다. 줄 수를 돌려준다."""
    lines = engine.recognize([image])[0]
    text = "\n".join(line.text for line in lines)
    missing = [word for word in _SMOKE_WORDS if word not in text]
    if missing:
        raise SystemExit(
            f"빌드 스모크 실패({label}): 인식 줄 {len(lines)}개, 기대 단어 {len(missing)}개 누락"
        )
    mean = sum(line.confidence for line in lines) / len(lines)
    print(f"{label}: lines={len(lines)} mean_confidence={mean:.3f}")
    return len(lines)


def _synthetic_page(font_path: str) -> PageImage:
    """흰 바탕에 한국어 두 줄을 그린 합성 쪽 이미지(PIL)."""
    from PIL import Image, ImageDraw, ImageFont

    font = ImageFont.truetype(font_path, 44)
    image = Image.new("RGB", (900, 260), "white")
    draw = ImageDraw.Draw(image)
    for index, line in enumerate(_SMOKE_LINES):
        draw.text((40, 40 + index * 100), line, fill="black", font=font)
    return image


def main(argv: list[str] | None = None) -> None:
    """``prefetch`` | ``smoke --font <ttf>``."""
    parser = argparse.ArgumentParser(prog="python -m ocr_worker.build_check")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("prefetch", help="모델을 이미지 안 캐시로 받는다")
    smoke_parser = sub.add_parser("smoke", help="OCR·NER 같은 프로세스 스모크")
    smoke_parser.add_argument("--font", required=True, help="한글 TTF 경로(합성 이미지용)")
    args = parser.parse_args(argv)
    if args.command == "prefetch":
        prefetch()
    else:
        smoke(args.font)
    sys.stdout.flush()


if __name__ == "__main__":
    main()
