"""e2e 통합 테스트 공용 픽스처 (이슈 #20).

실 Kafka(apache/kafka KRaft)·PostgreSQL(pgvector)에 붙어 OCR 워커의 소비→저장→발행
경로를 **끝에서 끝까지** 검증한다. GPU(surya)와 S3만 페이크로 주입하고, Kafka 컨슈머·
프로듀서·DLQ·수동커밋·마이그레이션·`ocr_results` 업서트는 전부 진짜를 쓴다.

기동: ``docker compose up -d`` (kafka localhost:9092 · postgres localhost:5432).
인프라가 안 떠 있으면 이 디렉터리의 테스트는 **skip**된다(단위 CI 무해).
env로 접속 정보를 덮을 수 있다: ``E2E_KAFKA`` · ``E2E_DATABASE_URL``.
"""

import contextlib
import os
import socket
import uuid
from collections.abc import AsyncIterator

import asyncpg
import pytest
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from core.config import Settings
from core.db import create_pool, run_migrations

# 접속 대상(기본 = docker-compose 로컬). env로 덮어쓸 수 있다.
E2E_KAFKA = os.getenv("E2E_KAFKA", "localhost:9092")
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


@pytest.fixture(scope="session")
def infra() -> bool:
    """Kafka·PostgreSQL 가용성 게이트. 하나라도 안 되면 e2e 전체를 skip한다."""
    kafka_host, _, kafka_port = E2E_KAFKA.partition(":")
    if not _tcp_open(kafka_host, int(kafka_port or 9092)):
        pytest.skip(f"Kafka 미가용({E2E_KAFKA}) — `docker compose up -d` 후 재시도")
    if not _tcp_open(*_dsn_host_port(E2E_DATABASE_URL)):
        pytest.skip("PostgreSQL 미가용 — `docker compose up -d` 후 재시도")
    return True


@pytest.fixture
def e2e_settings(infra: bool) -> Settings:
    """테스트마다 **고유 토픽·컨슈머그룹**을 쓰는 설정.

    토픽·그룹을 유니크하게 만들어 반복 실행·병렬 실행 간 오프셋/메시지 오염을 막는다.
    init 인자는 env·.env보다 우선하므로(pydantic-settings) 로컬 셸 환경과 무관하게 고정된다.
    """
    tag = uuid.uuid4().hex[:8]
    return Settings(
        kafka_bootstrap_servers=E2E_KAFKA,
        kafka_ocr_job_topic=f"e2e-ocr-{tag}",
        kafka_report_job_topic=f"e2e-report-{tag}",
        kafka_consumer_group=f"e2e-grp-{tag}",
        kafka_dlq_suffix=".dlq",
        kafka_max_retries=2,  # 핸들러 실패 경로를 빠르게(백오프 최소)
        database_url=E2E_DATABASE_URL,
        rds_ca_path=None,  # 로컬 PG는 SSL 끔
    )


@pytest.fixture(autouse=True)
async def e2e_topics(e2e_settings: Settings) -> None:
    """이 테스트가 쓰는 토픽(ocr·report·dlq)을 미리 만든다(단일 파티션).

    자동생성에 의존하면 갓 만들어진 토픽이 컨슈머 메타데이터에 아직 안 보여
    "topic not found in cluster metadata" 레이스가 난다. 사전 생성으로 제거한다.
    """
    ocr = e2e_settings.kafka_ocr_job_topic
    names = [ocr, e2e_settings.kafka_report_job_topic, f"{ocr}{e2e_settings.kafka_dlq_suffix}"]
    admin = AIOKafkaAdminClient(bootstrap_servers=e2e_settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        topics = [NewTopic(name, num_partitions=1, replication_factor=1) for name in names]
        with contextlib.suppress(TopicAlreadyExistsError):
            await admin.create_topics(topics)
    finally:
        await admin.close()


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
