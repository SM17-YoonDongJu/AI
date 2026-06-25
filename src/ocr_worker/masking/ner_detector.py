"""NER PII 디텍터 — 비정형 PII(이름)용. ``USE_NER``일 때만, lazy import.

노션 "텍스트 마스킹 상세"와 테스트 결론:
- **이름(PS)만 NER로 잡는다.** 주소(LC)는 NER 출력이 noisy(precision 0.22)라
  정규식(``ADDRESS_RE``)이 담당 — 여기서는 LC를 쓰지 않는다.
- **확정 모델**: ``Leo97/KoELECTRA-small-v3-modu-ner`` (small·CPU·~0.067s/샘플,
  의료용어 오탐 0). 같은 OCR 워커에서 torch **CPU 빌드**로 in-process 실행
  (paddle-gpu와 CUDA 충돌 없음, 노션 §6 A안).
- **로컬 실행 필수**: PII를 외부 API로 보내면 위반(CODE_CONVENTIONS §13).

``transformers``/``torch``는 ``ner`` extra라 무겁다 → 모듈 import가 아니라 첫
``detect`` 호출 시점에 lazy import + 파이프라인 1회 로드(이후 재사용).
"""

from ocr_worker.masking.spans import PiiLabel, Span

# modu NER 태그셋에서 인물(이름) 그룹 접두. aggregation_strategy="simple" 사용 시
# entity_group이 "PS"로 집계된다.
_PERSON_GROUP_PREFIX = "PS"


class NerDetector:
    """이름(PS) 전용 NER 디텍터. 파이프라인은 최초 detect 시 lazy 로드된다."""

    def __init__(self, model_name: str, score_threshold: float = 0.5) -> None:
        """디텍터를 만든다(모델은 아직 로드하지 않음).

        Args:
            model_name: HuggingFace 모델 ID(예: ``Leo97/KoELECTRA-small-v3-modu-ner``).
            score_threshold: 이 확률 미만 엔티티는 버린다. recall 우선이라 낮게
                두되, 과잉 마스킹 시 상향(노션 §2).
        """
        self._model_name = model_name
        self._score_threshold = score_threshold
        self._pipeline: object | None = None

    def _ensure_pipeline(self) -> object:
        """transformers 파이프라인을 1회 lazy 로드한다(CPU 강제)."""
        if self._pipeline is None:
            # 무거운 의존성 → 함수 내부 lazy import (모듈 import 비용 회피).
            from transformers import pipeline

            self._pipeline = pipeline(
                task="ner",
                model=self._model_name,
                tokenizer=self._model_name,
                aggregation_strategy="simple",
                device=-1,  # CPU 강제 — paddle-gpu와 CUDA 버전 충돌 방지
            )
        return self._pipeline

    def detect(self, text: str) -> list[Span]:
        """텍스트에서 인물명(PS) 스팬을 찾아 반환한다.

        Args:
            text: OCR로 추출된 원본 텍스트.

        Returns:
            라벨이 ``PiiLabel.NAME``인 ``Span`` 목록(임계값 미만은 제외).
        """
        if not text.strip():
            return []
        pipe = self._ensure_pipeline()
        spans: list[Span] = []
        for ent in pipe(text):  # type: ignore[operator]
            group = str(ent.get("entity_group", ""))
            score = float(ent.get("score", 0.0))
            if group.startswith(_PERSON_GROUP_PREFIX) and score >= self._score_threshold:
                spans.append(
                    Span(
                        start=int(ent["start"]),
                        end=int(ent["end"]),
                        label=PiiLabel.NAME,
                        source="ner",
                        score=score,
                    )
                )
        return spans
