"""가드레일 (06) — 입력/생성/출력 3단계 공용 모듈. report_worker·chatbot이 공유.

공개 진입점은 guard_input·guard_generation·guard_output. 결과 모델(InputGuardResult/
OutputGuardResult)은 core.contracts가 단일 출처다. 구현은 guards 모듈에 있다.
"""

from guardrail.guards import (
    DISCLAIMER,
    guard_generation,
    guard_input,
    guard_output,
)

__all__ = ["DISCLAIMER", "guard_generation", "guard_input", "guard_output"]
