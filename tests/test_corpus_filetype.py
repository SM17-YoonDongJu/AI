"""corpus_worker.filetype.detect 테스트.

약관 첨부가 전부 PDF는 아니다 — 파일명에서 확장자·ContentType을 정직하게 판정하는지,
복합 확장자(.hwp.zip)가 컨테이너 정보를 보존하는지, 모르는/없는 파일명은 추측하지 않고
실패하는지 검증한다.
"""

import pytest

from core.exceptions import CorpusSyncError
from corpus_worker.filetype import FileType, detect


def test_detect_simple_extension() -> None:
    assert detect("표준약관.pdf") == FileType(ext=".pdf", content_type="application/pdf")
    assert detect("특별약관.hwp") == FileType(ext=".hwp", content_type="application/x-hwp")
    assert detect("안내문.txt") == FileType(ext=".txt", content_type="text/plain")


def test_detect_is_case_insensitive() -> None:
    assert detect("약관.PDF") == FileType(ext=".pdf", content_type="application/pdf")


def test_detect_preserves_compound_extension_over_last_suffix() -> None:
    # .hwp.zip은 "zip 안이 HWP"라는 정보가 있다 — 마지막 조각(.zip)만 보면 소실된다
    result = detect("특별약관.hwp.zip")
    assert result == FileType(ext=".hwp.zip", content_type="application/zip")


def test_detect_raises_when_file_name_missing() -> None:
    with pytest.raises(CorpusSyncError):
        detect(None)
    with pytest.raises(CorpusSyncError):
        detect("")


def test_detect_raises_on_unknown_extension() -> None:
    with pytest.raises(CorpusSyncError):
        detect("약관.xyz")
    with pytest.raises(CorpusSyncError):
        detect("확장자없음")
