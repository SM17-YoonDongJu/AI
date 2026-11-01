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
# report가 소유한 ai 테이블 마이그레이션만 싣는다 — 기동 시 run_migrations로 적용한다(__main__의
# _MIGRATIONS_DIR는 WORKDIR 기준 상대 경로). GPU EC2의 OCR 워커가 꺼져 있어도 report가 자기
# 테이블(report_drafts·report_job_failures)을 보장하려는 것이다.
# migrations/ai 전체를 싣지 않는 이유: OCR용 003·004·006은 매 기동 ai.ocr_results에 ACCESS EXCLUSIVE
# 잠금을 잡고 CHECK를 다시 만든다 — report까지 돌리면 긴 조회에 막혀 기동이 멈추고, 두 워커의 이미지
# 버전이 엇갈리면 CHECK가 옛 정의로 되돌아갈 수 있다. 여기 싣는 파일은 CREATE IF NOT EXISTS·GRANT만
# 있어 어느 워커가 몇 번 돌려도 같다. 디렉터리 이름(ai)은 OCR과 같게 둬 advisory lock 키를 공유한다.
# report 소유 마이그레이션을 새로 추가하면 이 줄에도 넣는다(테스트가 목록을 고정한다).
COPY migrations/ai/007_report_drafts.sql migrations/ai/012_report_job_failures.sql ./migrations/ai/

# 빌드 커밋 — deploy.yml이 build-args로 넘긴다. 부팅 로그에 찍어 Argo CD 반영 후 새 이미지인지 확인한다.
ARG GIT_COMMIT=unknown
ENV GIT_COMMIT=${GIT_COMMIT}

# report-worker 서비스명 (관측성 로그/메트릭 라벨). SQS 큐 URL·리전은 런타임 env로 주입.
ENV SERVICE_NAME=report-worker

# 진입점: src/report_worker/__main__.py (report-job 컨슈머)
CMD ["python", "-m", "report_worker"]
