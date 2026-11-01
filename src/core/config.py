"""환경설정 단일 출처 (pydantic-settings) — 정본(canonical) 슈퍼셋.

전 워커·모듈(OCR·RAG·리포트·가드레일)이 공유하는 설정을 한 곳에 모은다. 모델·엔드포인트는
코드에 하드코딩하지 않고 env로 주입하며, 시크릿(DB 비밀번호 등)은 코드·로그·커밋에 두지 않는다.
아래 기본값은 로컬 개발(docker-compose.dev)용이며 실제 값이 아니다.

필드 네이밍은 팀 정본 규약을 따른다 — 인프라(DB·SQS·AI)는 서술적 이름, 관측성은 환경/서비스
식별자. `os.getenv` 산재를 금지하고 여기서만 로드한다.
"""

from functools import lru_cache
from typing import Literal

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# 임베딩 차원은 계약상 고정값 — 코퍼스 벡터 컬럼 halfvec(1024)와 같아야 한다.
# qwen3-embedding은 1024d이고, voyage-4-large는 요청의 output_dimension으로 1024를 맞춘다.
DEFAULT_EMBEDDING_DIM = 1024

# CD 스모크: shared(core) 변경으로 3개 서비스 배포 경로 검증(값·동작 무변경). 재트리거 6.
# migrations/만 건드리는 변경은 ocr 필터(src/ocr_worker/**)엔 안 걸린다 — migrations/가
# Dockerfile COPY로 이미지에 박히므로 재빌드 없인 옛 마이그레이션이 그대로 남는다.
# 이번엔 CREATE SCHEMA IF NOT EXISTS 권한 에러 핫픽스(같은 커밋)를 ocr_worker에도
# 반영하려고 재트리거.


class Settings(BaseSettings):
    """전 워커가 공유하는 환경설정. env 변수명은 필드명과 동일(대소문자 무시)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- 환경·관측성 ---
    # 배포 템플릿(.env.*.example·docker-compose*.yml)이 전부 `ENV`를 주입하는데 별칭이
    # 없으면 pydantic-settings는 `ENVIRONMENT`만 찾아 이 필드가 항상 기본값(local)으로
    # 고정된다. core.crypto.get_pii_dek()가 이 값으로 dev(평문 env 키)/prod(KMS)를
    # 가르므로, 지금은 로그 레벨 정도에만 영향이던 오배선이 prod에서 KMS를 건너뛰는
    # 보안 버그가 된다 — `ENV`·`ENVIRONMENT` 둘 다 받도록 명시적으로 별칭을 둔다.
    environment: str = Field("local", validation_alias=AliasChoices("environment", "ENV"))
    # local | dev | prod
    log_level: str = ""  # 빈값이면 환경별 결정(local=DEBUG, 그 외=INFO)
    service_name: str = "ai-engine"  # 워커별 env(SERVICE_NAME)로 덮어씀
    instance_id: str = ""  # 인스턴스/Pod 식별자(비면 hostname)
    git_commit: str = "unknown"  # 이미지 빌드 커밋(Dockerfile ARG→ENV GIT_COMMIT). 배포 반영 확인용

    # --- Database (AWS RDS PostgreSQL / 로컬 PG) ---
    database_url: str = "postgresql://postgres:postgres@localhost:5432/ai_engine"
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10
    rds_ca_path: str | None = None  # RDS TLS CA 번들 경로. 로컬 PG면 비움(SSL 끔)

    # --- SQS (AWS SQS Standard 큐 / 로컬 LocalStack) ---
    # 큐는 전체 URL로 지정한다(토픽명이 아님). 자격증명은 표준 AWS 체인(워커 IAM Role) —
    # 정적 키 없음. 로컬은 sqs_endpoint_url(LocalStack)만 주면 된다. 전송 계층 배선은
    # core.sqs.client, 수신/발행은 core.sqs.consumer/producer.
    sqs_ocr_job_queue_url: str = ""  # 소비 큐(Spring → ocr_worker) 전체 URL
    sqs_report_job_queue_url: str = ""  # 발행 큐(ocr_worker → report_worker) 전체 URL
    # 로컬 LocalStack 엔드포인트(예: http://localhost:4566). 비면 실 AWS 기본 엔드포인트.
    sqs_endpoint_url: str | None = None
    sqs_wait_time_seconds: int = 20  # 롱폴링 대기(0~20). 빈 큐 폴링 비용·지연 절감
    sqs_visibility_timeout: int = 600  # 처리시간보다 길게(초). 짧으면 처리 중 재전달로 중복 처리
    sqs_max_messages: int = 1  # ReceiveMessage 배치 크기(1~10). 워커는 1건씩 처리
    # 이 횟수를 넘겨 재수신되면 poison(못 살리는 메시지)으로 보고 명시적 삭제(스킵)한다 —
    # DLQ 미도입 상태의 자체 방어(4일 재전달 루프 차단). 향후 DLQ 도입 시 redrive policy로 대체.
    sqs_max_receive_count: int = 5

    # --- AWS (S3 원본 GetObject · SQS 리전) ---
    aws_region: str = "ap-northeast-2"
    s3_bucket: str = ""

    # --- PII 컬럼 복호화 (백엔드가 AES-256-GCM으로 암호화한 RDS 컬럼 읽기 — core.crypto) ---
    # 키 소유는 백엔드다. 여기선 복호화만 하며, 출처가 환경별로 갈린다(core.crypto.get_pii_dek).
    # local·dev: KMS 없이 이 값(base64 32바이트)을 그대로 DEK로 쓴다. 시크릿 — 코드·커밋 금지.
    pii_enc_key: str | None = None
    # prod: core.encryption_keys의 wrapped DEK를 이 CMK로 푼다. ARN 하드코딩 금지(계정별로 다름).
    pii_kms_key_arn: str | None = None

    # --- 원본 삭제 outbox (ocr_results.original_delete_* — ocr_worker 스윕) ---
    # 마스킹 검증을 통과한 원본 삭제가 실패하면 ocr_results 행에 남겨 재시도한다.
    ocr_delete_max_attempts: int = 5  # 소진 시 'exhausted'(종결) — S3 라이프사이클이 백스톱
    # 스윕 주기이자 실패 후 고정 백오프 간격(초). 지수 백오프는 두지 않는다 — 삭제 실패는
    # 부하가 아니라 권한·네트워크 등 지속성 오류가 대부분이라 간격을 늘려도 이득이 없다.
    ocr_delete_retry_interval_seconds: float = 900.0

    # --- Notion (약관 코퍼스 소스 — jjg 인테그레이션, 공식 REST API) ---
    notion_token: str = ""  # 시크릿(ntn_...). SSM/.env 주입, 코드·커밋 금지
    # 데이터 출처 카탈로그(컨트롤 테이블). /v1/databases/{id}/query용 **database id**
    # (collection/data source id 아님 — 그건 relation·view 참조용).
    notion_catalog_database_id: str = "e6fa0ba5efb946c6934c195774e9b6e7"
    # terms(약관) 파일 DB(카탈로그 `출처` relation 대상)의 database id.
    notion_corpus_database_id: str = "e2bde980f10741d8bcf35f2d60709171"
    notion_api_version: str = "2022-06-28"  # Notion-Version 헤더 고정
    notion_sync_interval_seconds: int = 3600  # 카탈로그→PG 주기 동기화 간격
    notion_rate_limit_rps: float = 3.0  # Notion API 평균 레이트리밋 상한

    # --- Corpus S3 스테이징 (Notion 첨부 → S3, 청킹/임베딩은 범위 밖) ---
    corpus_categories: str = "terms"  # MVP 필터. 확장 시 "terms,precedent,medical,legal"
    s3_corpus_prefix: str = "corpus/"  # 키 = corpus/{category}/{sha256}{ext} (OCR 키공간과 분리)
    s3_corpus_sse: str = "AES256"  # SSE-S3(공개 약관·비-PII)
    corpus_poll_interval_seconds: float = 5.0  # 우선순위 큐 폴링 간격
    corpus_max_concurrent_uploads: int = 4  # 동시 업로드 수
    # 대용량 스트리밍 임시(처리 후 즉시 삭제). env로 재정의 가능한 기본값이라 S108 무시.
    corpus_download_tmp_dir: str = "/tmp"  # noqa: S108
    corpus_max_attempts: int = 5  # 문서 처리 재시도 상한(초과 시 status='failed')
    corpus_stale_reclaim_seconds: int = 900  # in_progress 좀비 회수 TTL(초)
    corpus_full_reconcile_every_cycles: int = 24  # N 사이클마다 full 동기화(삭제 반영)

    # --- 수요도 우선순위 (가중치·상수만 env; 상품종류 룩업표는 priority 모듈) ---
    corpus_w_demand: float = 0.6  # 수요도(상품종류) 가중
    corpus_w_urgency: float = 0.4  # 긴급도(시행일 신선도) 가중
    corpus_demand_halflife_days: float = 7.0  # 수요 부스트 반감기(일)
    corpus_demand_boost_cap: float = 40.0  # 수요 부스트 상한

    # --- PII 마스킹 ---
    use_ner: bool = False  # NER 디텍터 활성 여부(false면 정규식만)
    # NER 모델(로컬 실행 — 외부 API로 PII 전송 금지). 이름(PS)만 사용.
    ner_model: str = "Leo97/KoELECTRA-small-v3-modu-ner"
    # NER 확률 임계값. 마스킹은 recall 우선이라 낮게; 과잉 마스킹 시 상향(노션 §2).
    ner_score_threshold: float = 0.5

    # --- AI 서빙 — 챗·임베딩 provider 스위치(env), 모델·엔드포인트도 env 주입 ---
    # 챗 기본값은 Claude(anthropic — 공식 SDK, ai_base_url 미사용, 키는 ai_api_key)다. 로컬 Ollama
    # 등 OpenAI 호환 서버를 쓰려면 LLM_PROVIDER=openai_compat으로 명시한다. 모델명은 기본값 없이
    # env(LLM_MODEL)로만 받는다(하드코딩 금지).
    # 임베딩 기본값은 아직 로컬(openai_compat)이다 — 코퍼스를 다른 모델로 재임베딩하기 전에
    # 쿼리 쪽만 바뀌면 오류 없이 검색 결과만 틀어지므로, 재임베딩(`python -m rag.reembed`) 후
    # env로 바꾼다.
    # 챗과 임베딩은 업체·노드가 다를 수 있어 엔드포인트와 인증키를 분리한다.
    llm_provider: Literal["openai_compat", "anthropic"] = "anthropic"
    ai_base_url: str = "http://localhost:11434/v1"  # openai_compat 챗 엔드포인트
    # 챗 인증키(시크릿). openai_compat이면 Bearer 헤더(로컬은 미사용), anthropic이면 Claude API 키.
    ai_api_key: str = "not-needed"
    # 예: claude-opus-5-5 (anthropic) | qwen3.6:35b-a3b (openai_compat — EXAONE은 상업 사용 불가)
    llm_model: str = ""
    # anthropic 전용 output_config.effort. Opus 5.5는 thinking이 항상 켜져 있어 effort가 유일한
    # 깊이 조절이고, API 기본값이 medium이라 리포트 품질 기준(high)을 명시한다.
    llm_effort: Literal["low", "medium", "high", "xhigh", "max"] = "high"
    # anthropic 전용 — thinking과 응답을 합친 출력 상한. 스트리밍으로 받으므로 넉넉히 둔다.
    llm_max_tokens: int = 64000
    llm_timeout_seconds: float = 600.0  # anthropic 전용 SDK 요청 타임아웃(초)
    # voyage: Voyage(MongoDB Atlas 모델 API) — 요청에 input_type·output_dimension을 싣는다.
    embedding_provider: Literal["openai_compat", "voyage"] = "openai_compat"
    embedding_base_url: str = "http://localhost:11434/v1"  # 임베딩 엔드포인트(별도 노드/업체 가능)
    # 임베딩 전용 인증키(시크릿). 챗 키와 분리한다 — 운영 AI_API_KEY는 Claude 키라 임베딩
    # 요청에 실리면 임베딩 업체로 새어 나간다. not-needed·빈값이면 인증 헤더를 붙이지 않는다.
    embedding_api_key: str = "not-needed"
    embedding_model: str = ""  # 예: qwen3-embedding (1024d) | voyage-4-large
    embedding_dim: int = DEFAULT_EMBEDDING_DIM
    ai_timeout_seconds: float = 60.0  # httpx 요청 타임아웃(openai_compat 챗 + 모든 임베딩)

    # --- VLM(Vision) 서빙 — Ollama 네이티브 /api/generate(OpenAI 호환 아님) ---
    # 다중 항목 표 문서(지급결과서·청구서·입원확인서·진료비영수증)에서 surya 라인
    # 순서가 표 구조를 못 살릴 때 보완하는 하이브리드 경로(ocr_worker.vlm_client) 전용.
    vlm_base_url: str = "http://localhost:11434"  # ai_base_url과 별도(엔드포인트 형태 다름)
    # 표 전사(텍스트만, 좌표 없음) — 팀 합의 모델. 실측: qwen3-vl:8b-instruct보다 텍스트
    # 정확도·속도(~11초 vs ~15초) 모두 좋음.
    vlm_model: str = "qwen3.6:35b-a3b"
    # 이미지 마스킹 보강용 PII 위치 grounding(좌표 출력) 전용 — vlm_model과 별도 모델.
    # 실측: qwen3.6:35b-a3b는 vision capability는 있지만 grounding 좌표가 실제 텍스트
    # 위치에서 벗어남(이름 박스가 이름 글자를 완전히 빗나감) — qwen3-vl:8b-instruct는
    # 같은 문서에서 정확했다. 좌표 정밀도가 안전(과소 마스킹 방지)에 직결돼 텍스트
    # 전사보다 훨씬 보수적으로 골라야 하므로, 검증된 모델을 별도로 고정한다.
    vlm_grounding_model: str = "qwen3-vl:8b-instruct"
    vlm_timeout_seconds: float = 60.0  # 실측 평균 15초·최대 34초 — 여유 두고 60초
    # ollama 기본 num_ctx는 페이지 이미지 토큰만으로 대부분 차 표 전사 응답이 문장
    # 중간에서 조용히 잘렸다(실측: 지급결과서 응답이 "피" 한 글자에서 끊김 — 에러 없이
    # 200 OK로 절단된 텍스트만 옴). 16384로 명시하니 4개 표 문서 전부 완전히 전사됐다.
    vlm_num_ctx: int = 16384


@lru_cache
def get_settings() -> Settings:
    """프로세스 단일 `Settings` 인스턴스를 반환한다(캐시).

    Returns:
        env에서 로드된 설정. 동일 인스턴스가 재사용된다.
    """
    return Settings()


# 단일 settings 인스턴스. `from core.config import settings` 또는 `get_settings()` 사용.
settings = get_settings()
