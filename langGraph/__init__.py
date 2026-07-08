"""langGraph — 리포트 멀티에이전트 실험 패키지 (이슈 #11).

실험 진입점(``python -m langGraph.x``)에서 src/ 공용 모듈(core·rag·guardrail)을 임포트할 수
있도록 ``<repo>/src`` 를 sys.path에 얹는다. ``python -m`` 은 서브모듈 임포트 전에 이 패키지
``__init__`` 을 먼저 실행하므로 가장 이른 훅이다. (P1에서 설치형 패키지로 전환하면 제거)
"""

import pathlib
import sys

_SRC = pathlib.Path(__file__).resolve().parent.parent / "src"
if _SRC.is_dir() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
