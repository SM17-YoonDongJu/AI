# OCR Worker — GPU 노드 배포 (surya-ocr 인프로세스, PyTorch).
# build context = 레포 루트:  docker build -f src/ocr_worker/Dockerfile -t ocr-worker .
#
# CUDA 런타임 베이스. surya는 PyTorch 기반 — torch CUDA 빌드(cu12x)와
# GPU 노드 드라이버·베이스 CUDA 버전을 맞춰야 한다.
FROM nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04 AS base
ENV DEBIAN_FRONTEND=noninteractive PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        software-properties-common ca-certificates \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y --no-install-recommends \
        python3.12 python3.12-venv libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
ENV UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_PYTHON=python3.12 \
    PYTHONPATH=/app/src \
    PATH="/app/.venv/bin:$PATH"

# 의존성만 설치(프로젝트는 PYTHONPATH로 실행). uv.lock 커밋 후 --frozen 권장.
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev --extra ocr

COPY src ./src

# 엔트리포인트(ocr_worker/__main__.py)는 구현 단계에서 추가.
CMD ["python", "-m", "ocr_worker"]
