# Report Worker — 범용 노드 배포 (LangGraph + LLM HTTP 호출, GPU 불필요).
# build context = 레포 루트:  docker build -f src/report_worker/Dockerfile -t report-worker .
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev --extra report

FROM python:3.12-slim AS runtime
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PATH="/app/.venv/bin:$PATH"
COPY --from=builder /app/.venv /app/.venv
COPY src ./src

# report-worker 전용 컨슈머 그룹·서비스명 (settings 기본값 ocr-worker 오버라이드)
ENV KAFKA_CONSUMER_GROUP=report-worker \
    SERVICE_NAME=report-worker

# 진입점: src/report_worker/__main__.py (report-job 컨슈머)
CMD ["python", "-m", "report_worker"]
