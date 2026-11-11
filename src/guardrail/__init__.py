"""가드레일 (06) — 입력/생성/출력 3단계 + 외부 전송 전 PII 마스킹. report_worker가 사용.

공개 진입점은 guard_input·guard_generation·guard_output·mask_pii. 결과 모델(InputGuardResult/
OutputGuardResult)은 core.contracts가 단일 출처다. 구현은 guards 모듈에 있다.

mask_pii는 ocr_worker.masking(OCR 입력단 마스커)을 그대로 호출한다. 잔류 PII 때문에 거절할 때
던지는 MaskingError도 여기서 다시 내보낸다. 호출자가 ocr_worker를 직접 import하지 않아도 되게
하려는 것이다.
"""

from guardrail.guards import (
    DISCLAIMER,
    guard_generation,
    guard_input,
    guard_output,
    mask_pii,
)
from ocr_worker.masking import MaskingError

__all__ = [
    "DISCLAIMER",
    "MaskingError",
    "guard_generation",
    "guard_input",
    "guard_output",
    "mask_pii",
]
