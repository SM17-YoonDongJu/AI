"""도메인 예외 계층 (CODE_CONVENTIONS §8).

호출자가 종류별로 대응할 수 있도록 ``AppError`` 아래에 모듈별 예외를 둔다.
복구 불가한 외부 실패(S3·OCR 엔진 등)는 컨텍스트를 붙여 이 계층으로 변환해
재발생시킨다(``raise OcrError(...) from e``) — 원인 예외를 삼키지 않는다.
"""


class AppError(Exception):
    """프로젝트 공통 예외 베이스. 모든 도메인 예외가 이를 상속한다."""


class OcrError(AppError):
    """OCR 처리 실패(S3 다운로드·문서 렌더·OCR 엔진 추론 등)."""


class CorpusSyncError(AppError):
    """약관 코퍼스 카탈로그 동기화 실패(Notion REST API 호출·응답 파싱·PG 미러 upsert)."""
