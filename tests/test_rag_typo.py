"""Kiwi 토큰화 테스트 — 내용어 추출(kiwipiepy 실설치)과 프로세스당 한 벌 생성(#113)."""

import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from structlog.testing import capture_logs

from rag import typo
from rag.typo import extract_content_tokens

# 동시에 첫 호출을 하는 스레드 수. 운영은 보장·장해 두 줄기지만, 넉넉히 겹쳐 경합을 확실히 드러낸다.
_THREADS = 8
# 가짜 Kiwi의 생성 시간. 함께 출발한 스레드가 모두 빈 _kiwi를 볼 만큼 길게 잡는다
# (진짜는 1초 안팎, 최고 RSS 약 550MB).
_SLOW_INIT_SECONDS = 0.2
_QUERY = "후유장해 보험금"


def test_extracts_content_nouns_and_drops_particles() -> None:
    # Arrange / Act: 명사는 남고 조사(을)는 빠진다
    tokens = extract_content_tokens("보험금을 청구합니다")

    # Assert
    assert "보험금" in tokens
    assert "을" not in tokens


def test_returns_nonempty_for_domain_query() -> None:
    # Act
    tokens = extract_content_tokens("교통사고 후유장해 보상 기준")

    # Assert: 도메인 내용어가 일부라도 추출된다
    assert len(tokens) >= 2


# ── 프로세스당 한 벌(#113) ─────────────────────────────────────
# 보장·장해 줄기(R6)가 동시에 첫 검색에 들어오면, 락 없는 지연 생성은 두 스레드가 모두 빈 _kiwi를
# 보고 각자 한 벌씩 만든다. 한 벌이 최고 RSS 약 550MB라, 두 벌이 겹치면 report 파드(limit 768Mi)가
# OOMKilled됐다.
class _FakeKiwi:
    """가짜 Kiwi. 분석한 문장과 그때 이미 공유(`typo._kiwi`)된 상태였는지 기록한다. 토큰은 없다."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []

    def tokenize(self, text: str) -> list[object]:
        self.calls.append((text, typo._kiwi is self))
        return []


@pytest.fixture
def created_kiwis(monkeypatch: pytest.MonkeyPatch) -> list[_FakeKiwi]:
    """typo가 생성이 느린 가짜 Kiwi를 만들게 하고, 빈 싱글턴에서 시작한다. 만든 목록을 돌려준다."""
    created: list[_FakeKiwi] = []

    def slow_kiwi() -> _FakeKiwi:
        time.sleep(_SLOW_INIT_SECONDS)  # 만드는 사이 다른 스레드가 들어온다
        kiwi = _FakeKiwi()
        created.append(kiwi)
        return kiwi

    monkeypatch.setattr(typo, "Kiwi", slow_kiwi)
    monkeypatch.setattr(typo, "_kiwi", None)
    return created


def _call_together(fn: Callable[[], object], count: int) -> list[object]:
    """count개 스레드가 배리어에서 함께 출발해 fn을 부른다(첫 호출이 최대한 겹치게)."""
    barrier = threading.Barrier(count, timeout=5)

    def run() -> object:
        barrier.wait()
        return fn()

    with ThreadPoolExecutor(max_workers=count) as pool:
        futures = [pool.submit(run) for _ in range(count)]
        return [future.result() for future in futures]


def test_concurrent_first_calls_create_kiwi_once(created_kiwis: list[_FakeKiwi]) -> None:
    # Arrange: 여러 스레드가 빈 _kiwi로 동시에 토큰화한다(correct_query의 asyncio.to_thread 경로)

    # Act
    _call_together(lambda: extract_content_tokens(_QUERY), _THREADS)

    # Assert: 한 벌만 만들었다. 워밍업은 공유되기 전에 끝났고, 모든 스레드가 공유된 그 한 벌을 썼다
    assert len(created_kiwis) == 1
    assert created_kiwis[0].calls == [(typo._WARMUP_TEXT, False), *[(_QUERY, True)] * _THREADS]


def test_preload_loads_once_and_logs_kiwi_loaded(created_kiwis: list[_FakeKiwi]) -> None:
    # Arrange: 워커 기동 때 미리 올리고(두 번 불려도), 첫 작업의 검색이 그 한 벌을 쓴다

    # Act
    with capture_logs() as logs:
        typo.preload_kiwi()
        typo.preload_kiwi()
        extract_content_tokens(_QUERY)

    # Assert: 생성 1회 = kiwi_loaded 1번(운영에선 파드당 1번인지 본다). 생성 시간과 RSS를 싣는다
    assert len(created_kiwis) == 1
    assert created_kiwis[0].calls == [(typo._WARMUP_TEXT, False), (_QUERY, True)]
    loaded = [entry for entry in logs if entry["event"] == "kiwi_loaded"]
    assert len(loaded) == 1
    assert loaded[0]["duration_ms"] >= _SLOW_INIT_SECONDS * 1000
    assert loaded[0]["peak_rss_mb"] > 0


@pytest.mark.parametrize(
    ("platform", "ru_maxrss", "expected_mb"),
    [
        ("linux", 600 * 1024, 600),  # Linux(컨테이너)는 KiB
        ("darwin", 600 * 1024 * 1024, 600),  # macOS는 바이트
    ],
)
def test_peak_rss_mb_normalizes_ru_maxrss_units(
    monkeypatch: pytest.MonkeyPatch, platform: str, ru_maxrss: int, expected_mb: int
) -> None:
    # Arrange: 단위를 잘못 맞추면 kiwi_loaded의 RSS만 1024배 틀리고 아무것도 실패하지 않는다
    monkeypatch.setattr(typo.sys, "platform", platform)
    monkeypatch.setattr(
        typo.resource, "getrusage", lambda _who: SimpleNamespace(ru_maxrss=ru_maxrss)
    )

    # Act
    peak = typo._peak_rss_mb()

    # Assert
    assert peak == expected_mb
