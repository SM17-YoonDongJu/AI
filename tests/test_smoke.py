"""패키지 골격 스모크 테스트.

각 패키지가 import 가능한지(구조·`PYTHONPATH=src` 설정) 확인한다. 구현이 채워지면
계약·통합 테스트로 확장한다.
"""


def test_packages_importable() -> None:
    import chatbot  # noqa: F401
    import core  # noqa: F401
    import guardrail  # noqa: F401
    import ocr_worker  # noqa: F401
    import rag  # noqa: F401
    import report_worker  # noqa: F401
