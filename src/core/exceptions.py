"""도메인 예외 계층 (CODE_CONVENTIONS §8).

호출자가 종류별로 대응할 수 있도록 ``AppError`` 아래에 모듈별 예외를 둔다.
복구 불가한 외부 실패(S3·OCR 엔진 등)는 컨텍스트를 붙여 이 계층으로 변환해
재발생시킨다(``raise OcrError(...) from e``) — 원인 예외를 삼키지 않는다.
"""


class AppError(Exception):
    """프로젝트 공통 예외 베이스. 모든 도메인 예외가 이를 상속한다."""


class NonRetryableError(Exception):
    """재전달해도 결과가 같은 결정적 실패. 컨슈머가 즉시 ack하고 실패 기록으로 종결한다.

    믹스인 마커다 — 단독으로 던지지 말고 도메인 예외에 함께 상속시킨다
    (예: ``class UnreadableFileError(OcrError, NonRetryableError)``). 그래야
    ``except OcrError``로 잡던 기존 호출부가 그대로 동작하면서, 컨슈머만
    "재시도 가치가 있는가"를 추가로 구분할 수 있다.

    판정 기준은 **입력이 같으면 결과도 같은가**다. 마스킹 잔류·파일 디코드 실패처럼
    같은 바이트를 몇 번 더 읽어도 같은 결론이 나오는 실패만 여기에 넣는다. S3 다운로드
    실패(권한 전파 지연·일시적 네트워크)처럼 시간이 해결할 수 있는 실패는 넣지 않는다 —
    잘못 분류하면 회복 가능한 작업을 첫 시도에서 영구 실패로 확정해버린다.
    """


class OcrError(AppError):
    """OCR 처리 실패(S3 다운로드·문서 렌더·OCR 엔진 추론 등)."""


class UnreadableFileError(OcrError, NonRetryableError):
    """원본 파일을 페이지 이미지로 만들 수 없음(PDF 렌더·이미지 디코드 실패).

    손상·암호화된 파일이거나 확장자와 실제 포맷이 다른 경우다. 같은 바이트를 다시
    받아도 결과가 같으므로 재전달할 가치가 없다 — 사용자에게 "다시 업로드해 달라"고
    알리는 게 유일한 복구 경로라 첫 시도에서 종결하고 실패 저널에 남긴다.
    """


class CorpusSyncError(AppError):
    """약관 코퍼스 카탈로그 동기화 실패(Notion REST API 호출·응답 파싱·PG 미러 upsert)."""


class CorpusStagingError(AppError):
    """약관 첨부 S3 스테이징 실패(다운로드·HeadObject·업로드 등 S3 경계 오류).

    boto3/botocore 예외는 배포(ocr extra)에만 설치되는 선택 의존이라, S3 경계 모듈이
    이 항상-임포트 가능한 도메인 예외로 감싸 전파한다 — 소비자(pipeline)가 botocore를
    import하지 않고도 실패를 포착할 수 있게 한다.
    """


class PiiCryptoError(AppError):
    """PII 컬럼 복호화 실패(봉투 포맷 불일치·DEK 설정 누락·활성 키 부재 등).

    태그 검증 실패(``cryptography.exceptions.InvalidTag``)는 이미 구체적 예외라 감싸지
    않고 그대로 전파한다 — 이 예외는 그 앞 단계(포맷·설정) 검증 실패만 담당한다.
    """
