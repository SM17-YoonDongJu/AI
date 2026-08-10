"""e2e 통합 테스트 공용 픽스처 (이슈 #20).

실 SQS(LocalStack)·PostgreSQL(pgvector)에 붙어 OCR 워커의 소비→저장→발행 경로를 **끝에서
끝까지** 검증한다. GPU(surya)와 S3만 페이크로 주입하고, SQS 컨슈머·프로듀서·ack(DeleteMessage)·
poison 스킵·마이그레이션·``ocr_results`` 업서트는 전부 진짜를 쓴다.

기동: ``docker compose up -d`` (LocalStack SQS localhost:4566 · postgres localhost:5432).
인프라(또는 boto3)가 없으면 이 디렉터리의 테스트는 **skip**된다(단위 CI 무해).
env로 접속 정보를 덮을 수 있다: ``E2E_SQS_ENDPOINT`` · ``E2E_DATABASE_URL``.

boto3는 배포(ocr/report extra)에만 설치되는 선택 의존이라 conftest 최상위에서 import하지
않는다 — ``infra`` 게이트가 ``importorskip``으로 없으면 skip한다(로컬 dev 무해).
"""

import contextlib
import os
import socket
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any

import asyncpg
import pytest

from core.config import Settings
from core.db import create_pool, run_migrations

# 접속 대상(기본 = docker-compose 로컬). env로 덮을 수 있다.
E2E_SQS_ENDPOINT = os.getenv("E2E_SQS_ENDPOINT", "http://localhost:4566")
E2E_DATABASE_URL = os.getenv(
    "E2E_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ai_engine"
)
# 마이그레이션 SQL 위치(리포지토리 루트 기준). ai_owner/corpus_owner 전용 서브디렉터리로
# 분리돼 있다(#48~#52) — e2e는 단일 superuser(postgres)로 접속하므로 소유권 문제 없이
# 둘 다 순서대로 적용한다.
_MIGRATIONS_DIRS = ("migrations/ai", "migrations/corpus")


def _tcp_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """``host:port``에 TCP 연결이 되는지 빠르게 확인한다(인프라 가용성 프로브)."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _dsn_host_port(dsn: str) -> tuple[str, int]:
    """``postgresql://user:pw@host:port/db``에서 ``(host, port)``만 뽑는다."""
    authority = dsn.split("@", 1)[1].split("/", 1)[0]
    host, _, port = authority.partition(":")
    return host, int(port or 5432)


def _endpoint_host_port(url: str) -> tuple[str, int]:
    """``http://host:port`` 엔드포인트에서 ``(host, port)``만 뽑는다."""
    authority = url.split("://", 1)[-1].split("/", 1)[0]
    host, _, port = authority.partition(":")
    return host, int(port or 80)


@pytest.fixture(scope="session")
def infra() -> Any:
    """LocalStack SQS·PostgreSQL·boto3 가용성 게이트. 하나라도 없으면 e2e 전체를 skip한다.

    워커의 ``core.sqs.client``는 표준 자격증명 체인을 쓰므로 LocalStack용 더미 키를 env에
    선주입한다(실 AWS는 IAM Role이라 불필요). 반환값은 boto3 모듈(픽스처 재사용).
    """
    boto3 = pytest.importorskip("boto3")  # ocr/report extra 미설치 dev 환경은 skip
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_DEFAULT_REGION", "ap-northeast-2")
    if not _tcp_open(*_endpoint_host_port(E2E_SQS_ENDPOINT)):
        pytest.skip(f"LocalStack SQS 미가용({E2E_SQS_ENDPOINT}) — `docker compose up -d` 후 재시도")
    if not _tcp_open(*_dsn_host_port(E2E_DATABASE_URL)):
        pytest.skip("PostgreSQL 미가용 — `docker compose up -d` 후 재시도")
    return boto3


@pytest.fixture
def sqs_client(infra: Any) -> Any:
    """LocalStack을 가리키는 boto3 SQS 클라이언트(테스트 설정·검증용, 더미 자격증명)."""
    return infra.client(
        "sqs",
        region_name="ap-northeast-2",
        endpoint_url=E2E_SQS_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture
def e2e_queues(sqs_client: Any) -> Iterator[dict[str, str]]:
    """이 테스트가 쓰는 큐(ocr·report)를 **유니크 이름**으로 만들고 URL을 돌려준다.

    이름을 유니크하게 만들어 반복·병렬 실행 간 메시지 오염을 막는다. 끝나면 삭제한다.
    큐 기본 VisibilityTimeout은 짧게 잡아 실패 경로(재전달·poison) 테스트가 빨리 돈다 —
    컨슈머는 ReceiveMessage마다 자체 visibility(e2e_settings)를 넘기므로 소비엔 영향 없다.
    """
    tag = uuid.uuid4().hex[:8]
    names = {"ocr": f"e2e-ocr-{tag}", "report": f"e2e-report-{tag}"}
    urls: dict[str, str] = {}
    for key, name in names.items():
        urls[key] = sqs_client.create_queue(QueueName=name, Attributes={"VisibilityTimeout": "2"})[
            "QueueUrl"
        ]
    try:
        yield urls
    finally:
        for url in urls.values():
            with contextlib.suppress(Exception):
                sqs_client.delete_queue(QueueUrl=url)


@pytest.fixture
def e2e_settings(e2e_queues: dict[str, str]) -> Settings:
    """이 테스트의 큐 URL·LocalStack 엔드포인트를 박은 설정.

    타이밍 값(wait/visibility/재전달 상한)을 작게 잡아 실패·poison 경로를 빠르게 관찰한다.
    init 인자는 env·.env보다 우선하므로(pydantic-settings) 로컬 셸 환경과 무관하게 고정된다.
    """
    return Settings(
        sqs_ocr_job_queue_url=e2e_queues["ocr"],
        sqs_report_job_queue_url=e2e_queues["report"],
        sqs_endpoint_url=E2E_SQS_ENDPOINT,
        sqs_wait_time_seconds=1,  # 폴 빠르게(테스트 반응성)
        sqs_visibility_timeout=2,  # 실패 시 재전달을 빨리 관찰
        sqs_max_receive_count=3,  # poison 스킵을 빨리 관찰
        database_url=E2E_DATABASE_URL,
        rds_ca_path=None,  # 로컬 PG는 SSL 끔
    )


@pytest.fixture
async def e2e_pool(e2e_settings: Settings) -> AsyncIterator[asyncpg.Pool]:
    """실 PG 연결 풀 + 마이그레이션 적용.

    ``create_pool``의 커넥션 init이 ``register_vector``를 부르므로 **풀 생성 전에**
    ``vector`` 확장을 선반영한다(fresh DB의 닭-달걀 방지). 이후 워커와 동일하게
    ``run_migrations``로 ``ocr_results``까지 멱등 적용한다.
    """
    bootstrap = await asyncpg.connect(dsn=E2E_DATABASE_URL, ssl=False)
    try:
        await bootstrap.execute("CREATE EXTENSION IF NOT EXISTS vector")
    finally:
        await bootstrap.close()

    pool = await create_pool(e2e_settings)
    for migrations_dir in _MIGRATIONS_DIRS:
        await run_migrations(pool, migrations_dir)
    try:
        yield pool
    finally:
        await pool.close()
