"""PII 컬럼 복호화 (AES-256-GCM 봉투암호화) — 백엔드와의 암호문 계약.

RDS의 PII 컬럼(`user_claims`·`user_insurances` 일부)은 백엔드가 AES-256-GCM으로 암호화해
`bytea`로 저장한다. 이 모듈은 그 봉투(envelope)를 푸는 **읽기 전용** 경로만 제공한다 —
암호화와 키 로테이션은 백엔드 소관이라 여기서 하지 않는다.

봉투 바이트 레이아웃(백엔드와 합의한 고정 포맷):

    offset  길이  내용
    0       1     version    = 0x01
    1       1     aadScope   = 0x01 (컬럼 바인딩, 고정)
    2       2     keyVersion (big-endian unsigned short)
    4       12    nonce
    16      N     ciphertext || GCM tag(16바이트)

AAD = 헤더 4바이트(offset 0~3) || UTF-8("{table}:{column}"). 컬럼 단위 바인딩이므로 같은
암호문을 다른 컬럼 값으로 옮겨 붙이면 태그 검증이 깨진다(행 단위 바인딩은 아니다).

DEK(데이터 암호화 키) 출처는 환경에 따라 갈리지만, 호출자는 `await get_pii_dek()` 한 줄만
쓰면 되고 어디서 오는지 알 필요가 없다:
- local·dev: `settings.pii_enc_key`(base64 32바이트)를 디코딩해 그대로 사용. KMS 미사용.
- 그 외(prod): `core.encryption_keys`(백엔드 소유 테이블)의 wrapped DEK를 KMS로 푼다.

DEK는 프로세스당 1회만 확보해 메모리에 캐시한다 — 잡마다 KMS를 호출하면 지연·비용이 붙고
KMS 스로틀에도 걸린다. 평문 DEK와 복호화 결과(원문 PII)는 절대 로그에 남기지 않는다(§9·§13).
"""

import asyncio
import base64

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from core.config import Settings, get_settings
from core.db import get_pool
from core.exceptions import PiiCryptoError
from core.logging import get_logger

logger = get_logger(__name__)

VERSION = 0x01  # 지원하는 봉투 포맷 버전. 다른 값이면 계약 불일치이므로 즉시 실패한다.
HEADER_LEN = 4  # version(1) + aadScope(1) + keyVersion(2)
NONCE_LEN = 12  # GCM 표준 96비트 nonce
GCM_TAG_LEN = 16  # 128비트 인증 태그. ciphertext 뒤에 붙어 온다.
DEK_LEN = 32  # AES-256
# 평문이 0바이트여도 헤더+nonce+태그는 반드시 있다.
MIN_ENVELOPE_LEN = HEADER_LEN + NONCE_LEN + GCM_TAG_LEN


def decrypt_pii(envelope: bytes, table: str, column: str, dek: bytes) -> str:
    """PII 봉투를 복호화해 원문 문자열을 돌려준다.

    Args:
        envelope: `bytea` 컬럼 값 전체(헤더 || nonce || ciphertext || tag).
        table: 컬럼이 속한 테이블명(AAD 바인딩용, 예: "user_claims").
        column: 컬럼명(AAD 바인딩용, 예: "patient_name").
        dek: 32바이트 평문 DEK(`get_pii_dek()`).

    Returns:
        UTF-8로 디코딩한 평문. PII이므로 로깅·외부 전송 금지.

    Raises:
        PiiCryptoError: 봉투 길이가 최소치 미만·지원하지 않는 버전·태그 검증 실패(키·AAD·
            암호문 불일치, 원인은 `cryptography.exceptions.InvalidTag`로 체이닝됨)인 경우.
    """
    if len(envelope) < MIN_ENVELOPE_LEN:
        raise PiiCryptoError("invalid envelope length")
    version = envelope[0]
    if version != VERSION:
        raise PiiCryptoError(f"unsupported envelope version: {version}")
    header = envelope[:HEADER_LEN]
    nonce = envelope[HEADER_LEN : HEADER_LEN + NONCE_LEN]
    ciphertext_and_tag = envelope[HEADER_LEN + NONCE_LEN :]
    aad = header + f"{table}:{column}".encode()
    try:
        plaintext = AESGCM(dek).decrypt(nonce, ciphertext_and_tag, aad)
    except InvalidTag as exc:
        # 호출자가 cryptography를 직접 import하지 않고도 이 예외 하나만 잡으면 되게 한다
        # (core.exceptions.CorpusStagingError와 같은 이유 — 경계 예외는 도메인 계층으로 통일).
        raise PiiCryptoError(f"tag verification failed: {table}.{column}") from exc
    return plaintext.decode("utf-8")


def maybe_decrypt(value: bytes | str | None, table: str, column: str, dek: bytes) -> str | None:
    """컬럼 값의 **타입**으로 암호화 여부를 판별해 필요할 때만 복호화한다.

    컬럼마다 암호화 전환 배포 시점이 달라, 어떤 컬럼이 이미 `bytea`인지 코드가 알 필요가
    없게 만든 어댑터다. asyncpg는 `bytea`를 `bytes`로, `text`를 `str`로 돌려주므로 이 차이가
    그대로 판별 기준이 된다 — 전환이 끝나면 `decrypt_pii` 직접 호출로 정리할 수 있다.

    Args:
        value: asyncpg가 돌려준 컬럼 값(`bytes`=암호화됨, `str`=평문, `None`=NULL).
        table: 컬럼이 속한 테이블명(AAD 바인딩용).
        column: 컬럼명(AAD 바인딩용).
        dek: 32바이트 평문 DEK(`get_pii_dek()`).

    Returns:
        평문 문자열. 입력이 `None`이면 `None`.

    Raises:
        TypeError: `bytes`·`str`·`None` 이외의 타입인 경우(스키마 변경 신호).
        PiiCryptoError: 봉투 포맷이 계약과 다르거나 태그 검증에 실패한 경우.
    """
    if value is None:
        return None
    if isinstance(value, bytes):
        return decrypt_pii(value, table, column, dek)
    if isinstance(value, str):
        return value  # 아직 이 컬럼은 암호화 미배포 — 평문 그대로
    raise TypeError(f"unexpected type for {table}.{column}: {type(value)}")


def parse_key_version(envelope: bytes) -> int:
    """봉투 헤더의 `keyVersion`을 읽는다.

    키 로테이션이 없어 복호화 경로에선 쓰지 않지만(DEK는 항상 활성 키 1개), 마이그레이션·
    운영 점검에서 "어느 키로 암호화된 행인지" 확인할 수 있게 파싱만 노출해 둔다.

    Args:
        envelope: `bytea` 컬럼 값 전체.

    Returns:
        big-endian unsigned short로 해석한 키 버전.

    Raises:
        PiiCryptoError: 헤더조차 담기지 않는 짧은 값인 경우.
    """
    if len(envelope) < HEADER_LEN:
        raise PiiCryptoError("invalid envelope length")
    return int.from_bytes(envelope[2:HEADER_LEN], "big")


# --------------------------------------------------------------------------- #
# DEK 확보 — 환경별 분기 + 프로세스 1회 캐시
# --------------------------------------------------------------------------- #

# env 기반 DEK를 쓰는 환경. 그 외(prod, 그리고 아직 없는 환경 이름까지)는 KMS로 보낸다 —
# 환경값 오설정 시 운영에서 env 평문 키로 조용히 폴백하는 것보다 KMS 실패가 안전하다.
_ENV_DEK_ENVIRONMENTS = frozenset({"local", "dev"})

# 백엔드 소유 테이블(이 레포 마이그레이션에 없음). 활성 PII 키는 1행이라는 계약.
_ACTIVE_PII_KEY_SQL = """
SELECT key_version, wrapped_dek
FROM core.encryption_keys
WHERE purpose = 'PII' AND is_active = true
"""

_dek_cache: bytes | None = None
# 워커 시작 직후 여러 잡이 동시에 첫 복호화를 시도해도 KMS 호출은 1회만 나가게 한다.
_dek_lock = asyncio.Lock()


def _decode_env_dek(settings: Settings) -> bytes:
    """`settings.pii_enc_key`(base64)를 32바이트 DEK로 디코딩한다(local·dev 전용).

    Raises:
        PiiCryptoError: 값이 비었거나 base64가 아니거나 길이가 32바이트가 아닌 경우.
    """
    if not settings.pii_enc_key:
        raise PiiCryptoError("pii_enc_key가 비어 있습니다 (local·dev의 PII DEK)")
    try:
        # binascii.Error는 ValueError의 하위 타입이라 이 한 줄로 잡힌다.
        dek = base64.b64decode(settings.pii_enc_key, validate=True)
    except ValueError as exc:
        # 예외 메시지에 키 값을 싣지 않는다 — 로그·스택트레이스로 새어나간다.
        raise PiiCryptoError("pii_enc_key가 유효한 base64가 아닙니다") from exc
    if len(dek) != DEK_LEN:
        raise PiiCryptoError(f"pii_enc_key는 {DEK_LEN}바이트여야 합니다: {len(dek)}바이트")
    return dek


def _kms_decrypt_dek(wrapped_dek: bytes, settings: Settings) -> bytes:
    """KMS로 wrapped DEK를 풀어 평문 DEK를 돌려준다(동기 boto3 — 스레드에서 호출할 것).

    boto3는 배포 extra에만 설치되는 선택 의존이라 lazy import한다(core.sqs.client와 동일).
    자격증명은 표준 AWS 체인(워커 IAM Role)에서 온다 — 정적 키를 코드·env에 두지 않는다.
    """
    import boto3  # lazy: 배포 extra에만 설치

    client = boto3.client("kms", region_name=settings.aws_region)
    # 대칭 CMK는 KeyId 생략이 가능하지만, 생략하면 "암호문에 적힌 키"를 그대로 신뢰하게 된다.
    # 의도한 CMK로만 풀리도록 명시한다(AWS 권장).
    response = client.decrypt(CiphertextBlob=wrapped_dek, KeyId=settings.pii_kms_key_arn)
    return bytes(response["Plaintext"])


async def _load_dek_from_kms(settings: Settings) -> bytes:
    """`core.encryption_keys`의 활성 wrapped DEK를 읽어 KMS로 푼다(prod 경로).

    Raises:
        PiiCryptoError: `pii_kms_key_arn` 미설정, 활성 키 행 부재, 길이 이상.
        PoolNotInitializedError: `init_pool()` 전에 호출한 경우.
    """
    if not settings.pii_kms_key_arn:
        raise PiiCryptoError("pii_kms_key_arn 미설정 — PII 복호화에는 CMK ARN이 필요합니다")
    row = await get_pool().fetchrow(_ACTIVE_PII_KEY_SQL)
    if row is None:
        raise PiiCryptoError("core.encryption_keys에 활성 PII 키(is_active=true)가 없습니다")
    key_version = int(row["key_version"])
    wrapped_dek = bytes(row["wrapped_dek"])
    dek = await asyncio.to_thread(_kms_decrypt_dek, wrapped_dek, settings)
    if len(dek) != DEK_LEN:
        raise PiiCryptoError(f"KMS가 돌려준 DEK가 {DEK_LEN}바이트가 아닙니다: {len(dek)}바이트")
    logger.info("pii dek loaded", source="kms", key_version=key_version)
    return dek


async def _load_dek(settings: Settings) -> bytes:
    """환경에 맞는 출처에서 평문 DEK를 확보한다(캐시 미스일 때만 호출)."""
    if settings.environment in _ENV_DEK_ENVIRONMENTS:
        dek = _decode_env_dek(settings)
        logger.info("pii dek loaded", source="env", environment=settings.environment)
        return dek
    return await _load_dek_from_kms(settings)


async def get_pii_dek(settings: Settings | None = None) -> bytes:
    """PII 복호화용 평문 DEK를 반환한다(프로세스 1회 확보 후 캐시).

    호출자는 환경별 출처(local·dev의 env 키 / prod의 KMS)를 몰라도 된다. `settings`는
    테스트 주입용이며, 캐시가 이미 찼으면 무시된다.

    Args:
        settings: 사용할 설정. 생략하면 전역 `get_settings()`.

    Returns:
        32바이트 평문 DEK. 로깅·외부 전송 금지.

    Raises:
        PiiCryptoError: 키 설정 누락·형식 오류, 활성 키 행 부재.
        PoolNotInitializedError: KMS 경로에서 `init_pool()` 전에 호출한 경우.
    """
    global _dek_cache
    cached = _dek_cache
    if cached is not None:
        return cached
    async with _dek_lock:
        # 락을 기다리는 동안 다른 태스크가 이미 채웠을 수 있다(이중 확인).
        if _dek_cache is not None:
            return _dek_cache
        dek = await _load_dek(settings or get_settings())
        _dek_cache = dek
        return dek
