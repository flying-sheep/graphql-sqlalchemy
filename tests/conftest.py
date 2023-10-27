from __future__ import annotations

from asyncio import current_task, run
from typing import TYPE_CHECKING, cast

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_scoped_session,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, scoped_session, sessionmaker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator


@pytest.fixture(scope="session", params=[True, False], ids=["async", "sync"])
def is_async(request: pytest.FixtureRequest) -> bool:
    return cast(bool, request.param)


@pytest.fixture(scope="session")
def db_engine(is_async: bool) -> Generator[Engine | AsyncEngine, None, None]:
    engine: Engine | AsyncEngine
    if is_async:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    else:
        engine = create_engine("sqlite+pysqlite:///:memory:", echo=False)

    yield engine

    maybe_coro = engine.dispose()
    if maybe_coro is not None:
        run(maybe_coro)


@pytest.fixture(scope="session")
def db_session_factory(db_engine: Engine | AsyncEngine) -> scoped_session[Session] | async_scoped_session[AsyncSession]:
    """returns a SQLAlchemy scoped session factory"""
    if isinstance(db_engine, AsyncEngine):
        return async_scoped_session(async_sessionmaker(bind=db_engine), current_task)
    return scoped_session(sessionmaker(bind=db_engine))


@pytest.fixture()
async def db_session(
    db_session_factory: scoped_session[Session] | async_scoped_session[AsyncSession],
) -> AsyncGenerator[Session | AsyncSession, None]:
    """yields a SQLAlchemy session which is rollbacked after the test"""
    session = db_session_factory()

    yield session

    if isinstance(session, AsyncSession):
        await session.rollback()
    else:
        session.rollback()
