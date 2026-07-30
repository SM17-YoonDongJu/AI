# Corpus Worker — 약관 Notion→S3 스테이징(GPU 불필요·경량).
# build context = 레포 루트:  docker build -f src/corpus_worker/Dockerfile -t corpus-worker .
#
# OCR 워커와 달리 CUDA/torch/surya가 없다 — 순수 파이썬(httpx·asyncpg·boto3).
FROM python:3.12-slim AS base
ENV DEBIAN_FRONTEND=noninteractive PYTHONUNBUFFERED=1

# ca-certificates: Notion(api.notion.com)·S3 TLS 검증용. 그 외 시스템 의존 없음.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
ENV UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_PYTHON=python3.12 \
    PYTHONPATH=/app/src \
    PATH="/app/.venv/bin:$PATH"

# 의존성만 설치(프로젝트는 PYTHONPATH로 실행). base + corpus(boto3), torch/surya 제외.
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev --extra corpus

COPY src ./src

# 상시 데몬(sync + drain). SIGTERM에 우아하게 종료(진행 중 완료 후 정리).
CMD ["python", "-m", "corpus_worker"]
