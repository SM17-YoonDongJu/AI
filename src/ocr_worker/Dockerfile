# OCR Worker — CPU 노드 배포 (OCR 엔진 PaddleOCR PP-OCRv5 + NER, 둘 다 CPU in-process).
# build context = 레포 루트:  docker build -f src/ocr_worker/Dockerfile -t ocr-worker .
#   paddlepaddle CPU 휠은 linux x86_64만 있다(aarch64 없음) — Apple Silicon에서 빌드하면
#   --platform linux/amd64를 붙인다(에뮬레이션이라 느리다).
#
# GPU가 필요 없다: OCR 엔진(PaddleOCR)과 NER(KoELECTRA, torch)이 모두 CPU로 돌고, VLM
# (PaddleOCR-VL-1.6)은 클러스터 vLLM(charts/etl-vllm)을 HTTP로 부른다. CUDA 런타임·드라이버가
# 이미지에 들어갈 이유가 없다 — torch는 CPU 휠만 받는다(pyproject의 pytorch-cpu 인덱스, 아래 검사).
#
# 모델은 빌드 때 이미지 안(/opt/models)에 넣는다(런타임 다운로드·외부 egress 없음). /root/.cache는
# 배포에서 PVC가 붙을 수 있는 자리라(K8s-manifests PR #66) 그 아래에 두면 가려진다.
#
# 스테이지: builder(.venv) → runtime(라이브러리·모델) → smoke(빌드 검사) → 최종(runtime + 검사 표식).
# 최종 스테이지가 smoke 결과를 COPY하므로 `docker build`(target 없음, CI와 같다)가 검사를 건너뛸 수
# 없다. 두 스테이지가 같은 python:3.12-slim을 써야 .venv의 인터프리터 심볼릭
# (/usr/local/bin/python3.12)이 runtime에도 그대로 있다.

# ---- builder: 의존성 설치 ----
FROM python:3.12-slim AS builder
ENV DEBIAN_FRONTEND=noninteractive \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_PYTHON=/usr/local/bin/python3.12 \
    UV_PYTHON_PREFERENCE=only-system \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
# 의존성만 먼저 설치(레이어 캐시). --locked: pyproject와 uv.lock이 어긋나면 빌드를 멈춘다(CPU torch
# 인덱스 고정이 lock에 박혀 있다). ocr = PaddleOCR·렌더·S3, ner = transformers + CPU torch.
# ner를 빼면 USE_NER=true(이름 마스킹의 유일한 방어선)가 첫 OCR에서 ImportError로 깨진다.
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev --locked --extra ocr --extra ner \
    && /app/.venv/bin/python -c "import torch; assert torch.version.cuda is None, torch.version.cuda" \
    && if ls /app/.venv/lib/python3.12/site-packages | grep -i -E '^(nvidia|triton|cuda)'; then \
        echo "CUDA 패키지가 이미지에 들어왔다(CPU 휠만 허용)"; exit 1; fi

# ---- runtime: .venv + 시스템 라이브러리 + 모델 ----
FROM python:3.12-slim AS runtime
# PADDLE_PDX_CACHE_HOME: paddlex 모델 캐시(official_models/<모델명>)를 이미지 안으로.
# HF_HOME: NER 모델(HuggingFace) 캐시를 이미지 안으로.
# OMP_NUM_THREADS: torch(NER) 스레드 상한 — 배포 env로 OCR_CPU_THREADS와 함께 맞춘다(기본 2).
# MALLOC_ARENA_MAX: glibc 아레나 수 상한. 스레드(asyncio.to_thread·OpenMP)마다 아레나가 생기면 해제된
#   메모리가 흩어져 남아 작업 사이 RSS가 불어난다. 작업마다 malloc_trim(__main__._release_memory)과 짝이다.
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PATH="/app/.venv/bin:$PATH" \
    PADDLE_PDX_CACHE_HOME=/opt/models/paddlex \
    HF_HOME=/opt/models/huggingface \
    OMP_NUM_THREADS=2 \
    MALLOC_ARENA_MAX=2
# libgomp1: paddle 코어(libpaddle.so)가 OpenMP 런타임을 연다(모델을 만들 때 로드).
# libgl1·libglib2.0-0: paddlex가 GUI판 opencv-contrib-python을 버전까지 고정해 끌어오고, 그 cv2가
# import 시점에 libGL.so.1·libgthread를 연다(headless판으로 바꾸면 paddlex 고정과 충돌 — 청커와 같다).
# ca-certificates: S3·SQS·RDS·vLLM TLS 검증.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 libgl1 libglib2.0-0 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY src ./src
COPY migrations ./migrations
# 모델 프리페치(빌드 때 1회, 네트워크 필요): 설정의 검출·인식·줄 방향 모델 + PoC용 mobile 검출 +
# NER. 런타임 코드 경로(PaddleOcrEngine·NerDetector)로 받으므로 실제로 쓰는 파일과 같다.
RUN python -m ocr_worker.build_check prefetch
# 여기부터는 외부 연결 없이 돈다. paddlex는 캐시에 모델이 있으면 내려받지 않지만, 없는 모델 이름이
# 들어오면 모델 호스터 연결 검사부터 하므로 끈다("True" — 빈 문자열이 아니면 켜진다).
# HF_HUB_OFFLINE·TRANSFORMERS_OFFLINE: NER 로드 때 허브 조회를 하지 않는다.
ENV PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK=True \
    HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1

# ---- smoke: 빌드 검사(외부 연결 없이 같은 프로세스에서 OCR → NER → OCR) ----
# paddle과 torch를 한 프로세스에 올렸을 때의 OpenMP 충돌·모델 로드 실패를 배포 전에 잡는다.
# 한글 폰트(합성 이미지용)는 이 스테이지에만 깐다 — 최종 이미지엔 들어가지 않는다.
FROM runtime AS smoke
RUN apt-get update && apt-get install -y --no-install-recommends fonts-nanum \
    && rm -rf /var/lib/apt/lists/*
RUN python -m ocr_worker.build_check smoke \
        --font /usr/share/fonts/truetype/nanum/NanumGothic.ttf > /tmp/build-smoke.txt \
    && cat /tmp/build-smoke.txt

# ---- 최종: runtime + 빌드 검사 표식 ----
FROM runtime
COPY --from=smoke /tmp/build-smoke.txt /opt/models/build-smoke.txt
# 부팅 로그에 찍어 새 이미지가 실제로 떴는지 확인한다(report 이미지와 같은 방식, CI build-args).
ARG GIT_COMMIT=unknown
ENV GIT_COMMIT=${GIT_COMMIT}

# 엔트리포인트(ocr_worker/__main__.py).
CMD ["python", "-m", "ocr_worker"]
