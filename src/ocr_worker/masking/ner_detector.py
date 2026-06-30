"""NER PII 디텍터 — 비정형 PII(이름)용. ``USE_NER``일 때만, lazy import.

노션 "텍스트 마스킹 상세"와 테스트 결론:
- **이름(PS)만 NER로 잡는다.** 주소(LC)는 NER 출력이 noisy(precision 0.22)라
  정규식(``ADDRESS_RE``)이 담당 — 여기서는 LC를 쓰지 않는다.
- **확정 모델**: ``Leo97/KoELECTRA-small-v3-modu-ner`` (small·CPU·~0.067s/샘플,
  의료용어 오탐 0). 같은 OCR 워커에서 torch **CPU**로 in-process 실행 — OCR(surya)도
  PyTorch라 프레임워크가 통일되며, NER은 가벼워 CPU로 두고 GPU는 OCR 전용(노션 §6 A안).
- **로컬 실행 필수**: PII를 외부 API로 보내면 위반(CODE_CONVENTIONS §13).

``transformers``/``torch``는 ``ner`` extra라 무겁다 → 모듈 import가 아니라 첫
``detect`` 호출 시점에 lazy import + 파이프라인 1회 로드(이후 재사용).
"""

import threading
from typing import Any

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
        # 외부 객체(transformers Pipeline) → Any(§3). lazy 로드를 락으로 보호한다.
        self._pipeline: Any = None
        self._lock = threading.Lock()

    def _ensure_pipeline(self) -> Any:
        """transformers 파이프라인을 1회 lazy 로드한다(CPU 강제, 스레드 안전).

        ``to_thread``로 병렬 호출돼도 모델이 한 번만 로드되도록 더블체크 락을 쓴다.
        """
        if self._pipeline is None:
            with self._lock:
                if self._pipeline is None:  # 락 대기 중 다른 스레드가 이미 로드했을 수 있음
                    # 무거운 의존성 → 함수 내부 lazy import (모듈 import 비용 회피).
                    from transformers import pipeline

                    self._pipeline = pipeline(
                        task="ner",
                        model=self._model_name,
                        tokenizer=self._model_name,
                        aggregation_strategy="simple",
                        device=-1,  # CPU 강제 — 모델 경량, GPU는 OCR(surya) 전용
                    )
        return self._pipeline

    def detect(self, text: str) -> list[Span]:
        """텍스트에서 인물명(PS) 스팬을 찾아 반환한다.

        **라인 단위로 NER을 돌린다(중요).** 모델 입력은 ~512 토큰이 상한이라 다중
        페이지 텍스트를 통째로 넣으면 뒤쪽이 조용히 잘려 **꼬리의 이름이 누락**된다
        (이름은 NER이 유일한 방어선이라 곧 PII 누출). OCR 텍스트는 ``"\\n"``으로 라인이
        구분되므로, 라인별로 잘라 **배치 추론**하고 각 스팬 오프셋을 원문 기준으로
        보정한다(라인은 짧아 상한에 걸리지 않는다).

        Args:
            text: OCR로 추출된 원본 텍스트(라인은 ``"\\n"`` 구분).

        Returns:
            라벨이 ``PiiLabel.NAME``인 ``Span`` 목록(임계값 미만은 제외).
        """
        if not text.strip():
            return []
        lines = text.split("\n")
        line_offsets = self._line_offsets(lines)
        # 빈 라인은 이름이 없으니 추론 대상에서 제외(오프셋은 그대로 유지).
        indexed = [(index, line) for index, line in enumerate(lines) if line.strip()]
        if not indexed:
            return []

        pipe = self._ensure_pipeline()
        batch_results = pipe([line for _, line in indexed])  # 라인별 결과 리스트

        spans: list[Span] = []
        for (line_index, _), entities in zip(indexed, batch_results, strict=True):
            spans.extend(self._to_spans(entities, line_offsets[line_index]))
        return spans

    @staticmethod
    def _line_offsets(lines: list[str]) -> list[int]:
        """각 라인이 원문(``"\\n".join``)에서 시작하는 문자 오프셋."""
        offsets: list[int] = []
        cursor = 0
        for line in lines:
            offsets.append(cursor)
            cursor += len(line) + 1  # +1 = "\n"
        return offsets

    def _to_spans(self, entities: list[dict[str, Any]], base: int) -> list[Span]:
        """한 라인의 NER 엔티티를 원문 기준 ``Span``으로 변환한다(오프셋 보정).

        ``start``/``end``는 fast tokenizer에서만 보장되므로, 없으면 건너뛴다(KeyError 방지).
        """
        spans: list[Span] = []
        for ent in entities:
            start = ent.get("start")
            end = ent.get("end")
            if start is None or end is None:
                continue  # 오프셋 없는 엔티티(slow tokenizer 등)는 스킵
            group = str(ent.get("entity_group", ""))
            score = float(ent.get("score", 0.0))
            if group.startswith(_PERSON_GROUP_PREFIX) and score >= self._score_threshold:
                spans.append(
                    Span(
                        start=int(start) + base,
                        end=int(end) + base,
                        label=PiiLabel.NAME,
                        source="ner",
                        score=score,
                    )
                )
        return spans
