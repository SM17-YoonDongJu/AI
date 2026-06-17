# Chatbot — 범용 노드 배포 (FastAPI WebSocket 직결, GPU 불필요).
# build context = 레포 루트:  docker build -f src/chatbot/Dockerfile -t chatbot .
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev --extra chatbot

FROM python:3.12-slim AS runtime
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PATH="/app/.venv/bin:$PATH"
COPY --from=builder /app/.venv /app/.venv
COPY src ./src
EXPOSE 8000

# 엔트리포인트(chatbot/app.py:app)는 구현 단계에서 추가.
CMD ["uvicorn", "chatbot.app:app", "--host", "0.0.0.0", "--port", "8000"]
