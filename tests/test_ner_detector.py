"""NER 디텍터 단위 테스트 (이슈 #18).

transformers는 미설치(ner extra)라, 모델 파이프라인을 **가짜로 주입**해
라인 단위 추론·오프셋 보정·가드 로직만 검증한다(모델 정확도는 Colab에서 별도 검증).
``_pipeline``을 직접 세팅하면 ``_ensure_pipeline``의 lazy 로드를 우회한다.
"""

from typing import Any

from ocr_worker.masking.ner_detector import NerDetector
from ocr_worker.masking.spans import PiiLabel


class _NameFindingPipe:
    """라인에서 '홍길동'을 찾아 PS 엔티티(라인 기준 start/end)를 돌려주는 가짜 NER."""

    def __call__(self, lines: list[str]) -> list[list[dict[str, Any]]]:
        results: list[list[dict[str, Any]]] = []
        for line in lines:
            index = line.find("홍길동")
            results.append(
                [{"entity_group": "PS", "score": 0.99, "start": index, "end": index + 3}]
                if index >= 0
                else []
            )
        return results


def _detector(pipe: Any) -> NerDetector:
    detector = NerDetector("dummy-model")
    detector._pipeline = pipe  # 모델 로드 우회(transformers 미설치)
    return detector


def test_detects_name_in_tail_line_with_correct_offset() -> None:
    # 앞에 긴 본문(통짜 추론이면 512 토큰에 잘려 누락됐을 위치) + 마지막 줄에 이름
    text = "머리말 줄입니다\n" * 60 + "환자명 홍길동"
    detector = _detector(_NameFindingPipe())

    spans = detector.detect(text)

    assert len(spans) == 1
    span = spans[0]
    # 핵심: 오프셋이 원문 기준으로 정확히 보정됐는가
    assert text[span.start : span.end] == "홍길동"
    assert span.label is PiiLabel.NAME
    assert span.source == "ner"


def test_detects_names_across_multiple_lines() -> None:
    text = "보호자 홍길동\n특이사항 없음\n작성 홍길동"
    detector = _detector(_NameFindingPipe())

    spans = detector.detect(text)

    assert len(spans) == 2
    assert all(text[s.start : s.end] == "홍길동" for s in spans)


def test_skips_entity_without_offsets() -> None:
    # slow tokenizer 등으로 start/end가 없으면 KeyError 없이 스킵
    class _NoOffsetPipe:
        def __call__(self, lines: list[str]) -> list[list[dict[str, Any]]]:
            return [[{"entity_group": "PS", "score": 0.99}] for _ in lines]

    detector = _detector(_NoOffsetPipe())

    assert detector.detect("환자 홍길동") == []


def test_score_below_threshold_is_dropped() -> None:
    class _LowScorePipe:
        def __call__(self, lines: list[str]) -> list[list[dict[str, Any]]]:
            return [[{"entity_group": "PS", "score": 0.10, "start": 0, "end": 3}] for _ in lines]

    detector = _detector(_LowScorePipe())  # 기본 임계 0.5

    assert detector.detect("홍길동") == []


def test_empty_text_returns_empty() -> None:
    detector = _detector(_NameFindingPipe())

    assert detector.detect("   \n  ") == []


def test_inference_is_serialized_across_threads() -> None:
    # CPU torch 모델 동시 forward 방지 — 여러 스레드가 detect를 동시에 불러도
    # 파이프라인 호출은 한 번에 하나만 들어가야 한다.
    import threading
    import time

    counter = threading.Lock()
    inside = 0
    max_inside = 0

    class _ConcurrencyProbe:
        def __call__(self, lines: list[str]) -> list[list[dict[str, Any]]]:
            nonlocal inside, max_inside
            with counter:
                inside += 1
                max_inside = max(max_inside, inside)
            time.sleep(0.02)  # 겹칠 시간 확보
            with counter:
                inside -= 1
            return [[] for _ in lines]

    detector = _detector(_ConcurrencyProbe())
    threads = [threading.Thread(target=lambda: detector.detect("홍길동")) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert max_inside == 1  # 직렬화됨(동시 진입 0건)
