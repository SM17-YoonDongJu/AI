# OCR Worker — GPU 노드 배포 (surya-ocr 인프로세스, PyTorch).
# build context = 레포 루트:  docker build -f src/ocr_worker/Dockerfile -t ocr-worker .
#
# CUDA 중복 제거: torch(pip 휠)가 CUDA 런타임 라이브러리(nvidia-cublas/cudnn/cusparse/…)를
# 자체 번들하므로 nvidia/cuda 베이스 이미지가 필요 없다. GPU는 호스트 NVIDIA 드라이버 +
# nvidia-container-toolkit가 libcuda를 주입해 쓴다.
# 베이스를 python:3.12-slim으로 바꿔 CUDA/cuDNN 이중 포함(수 GB)을 제거 → 이미지 크기 대폭 감소.
#
# GPU 대상: 이 이미지 안에는 surya만 돈다. VLM(PaddleOCR-VL-1.6)은 클러스터 vLLM(charts/etl-vllm)을
# HTTP로 부르므로 예전 Ollama Qwen 35B 때문에 필요했던 L40S(48GB)급이 더는 필요 없다. 옮겨 갈
# 노드는 brbs-etl(g4dn.xlarge, T4 16GB)이고 vLLM과 GPU 1장을 타임슬라이스로 나눈다(K8s 차트는 후속).
# - T4(SM 7.5)는 bf16을 못 쓰지만 surya 0.17은 CUDA에서 float16을 쓰므로 그대로 돈다.
# - surya 기본 배치(RECOGNITION_BATCH_SIZE 256·DETECTOR_BATCH_SIZE 32, GPU 기준)는 vLLM
#   (--gpu-memory-utilization 0.5)과 16GB를 나눠 쓰기엔 크다 — 이미지엔 박지 않고 배포 env로 낮춘다
#   (값은 T4에서 재서 정한다).
# 멀티스테이지: builder가 .venv를 구성 → runtime엔 .venv만 복사(uv·apt 캐시·빌드 도구 제외).
#
# 주의: 두 스테이지가 동일한 python:3.12-slim을 써야 .venv의 인터프리터 심볼릭
#       (/usr/local/bin/python3.12)이 runtime에도 그대로 존재한다.

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
# 의존성만 먼저 설치(레이어 캐시). uv.lock 커밋 후 --frozen 권장.
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev --extra ocr

# ---- runtime: .venv만 얹은 경량 이미지 ----
FROM python:3.12-slim AS runtime
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PATH="/app/.venv/bin:$PATH"
# surya/opencv/pillow 런타임 시스템 라이브러리(libGL·glib)와 TLS 인증서. CUDA는 torch 번들 사용.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgl1 libglib2.0-0 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY src ./src
COPY migrations ./migrations

# 엔트리포인트(ocr_worker/__main__.py).
CMD ["python", "-m", "ocr_worker"]
