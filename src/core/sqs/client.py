"""AWS SQS 클라이언트 팩토리 — 로컬(LocalStack) ↔ 실 AWS(IAM Role) 단일 지점.

전송 계층(자격증명·리전·엔드포인트)만 담당한다. 큐 URL·메시지 계약(OcrJob·ReportJob JSON)·
수신/발행 로직은 consumer/producer 래퍼 소관이며 이 모듈이 건드리지 않는다. 자격증명은 표준
AWS 체인(워커 IAM Role)에서 온다 — 정적 키를 코드·env에 두지 않는다(CODE_CONVENTIONS §13).
로컬은 ``sqs_endpoint_url``(LocalStack, 예: http://localhost:4566)만 주면 되고 더미 자격증명은
env(AWS_ACCESS_KEY_ID·AWS_SECRET_ACCESS_KEY=test)로 넣는다.

boto3는 동기 SDK이자 배포(ocr·report extra)에서만 설치되는 선택 의존이라 **lazy import**한다 —
로컬 CPU 개발 환경엔 미설치이며, 단위 테스트는 클라이언트를 페이크로 주입한다(§7).
"""

from typing import Any

from core.config import Settings, get_settings
from core.logging import get_logger

logger = get_logger(__name__)

# 프로세스 1회 생성해 재사용하는 boto3 SQS 클라이언트(연결 풀 재사용, 호출은 스레드 안전).
_sqs_client: Any | None = None


def get_sqs_client(settings: Settings | None = None) -> Any:
    """캐시된 boto3 SQS 클라이언트를 반환한다(첫 호출 시 lazy 생성).

    ``endpoint_url``이 비면(실 AWS) boto3 기본 엔드포인트를, 있으면(로컬) LocalStack을 쓴다.
    빈 문자열은 ``None``으로 정규화해 boto3가 기본값을 선택하게 한다.
    """
    global _sqs_client
    if _sqs_client is None:
        import boto3  # lazy: 배포 ocr·report extra에만 설치

        settings = settings or get_settings()
        _sqs_client = boto3.client(
            "sqs",
            region_name=settings.aws_region,
            endpoint_url=settings.sqs_endpoint_url or None,
        )
        logger.info(
            "sqs client created",
            region=settings.aws_region,
            local_endpoint=bool(settings.sqs_endpoint_url),
        )
    return _sqs_client


def queue_name(queue_url: str) -> str:
    """큐 URL에서 큐 이름(마지막 경로 세그먼트)만 뽑는다.

    로그·상관관계용 — 전체 URL은 account id를 담으므로 이름만 남겨 로그를 간결하게 한다.
    """
    return queue_url.rsplit("/", 1)[-1] or queue_url
