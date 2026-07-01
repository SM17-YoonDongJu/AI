"""db.py 풀 lifecycle 테스트.

실제 PG 연결 없이(외부 의존 격리) 미초기화 상태의 계약만 검증한다. 실제 풀 생성/쿼리는
docker-compose.dev 기동 후 통합 테스트로 다룬다.
"""

import pytest

from core import db


def test_get_pool_before_init_raises() -> None:
    # Arrange: 모듈 전역 풀이 미초기화 상태인지 보장
    db._pool = None

    # Act / Assert
    with pytest.raises(db.PoolNotInitializedError):
        db.get_pool()


async def test_close_pool_is_noop_when_uninitialized() -> None:
    # Arrange
    db._pool = None

    # Act (예외 없이 무시되어야 함)
    await db.close_pool()

    # Assert
    assert db._pool is None
