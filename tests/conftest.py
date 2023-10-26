from __future__ import annotations

from asyncio import run
from collections.abc import Coroutine, Generator
from typing import Any

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker


def maybe_run_coro(coro: Coroutine[Any, Any, None] | None) -> None:
    if coro is not None:
        run(coro)


@pytest.fixture(scope="session", params=[True, False], ids=["async", "sync"])
def is_async(request: pytest.FixtureRequest) -> bool:
    return request.param


@pytest.fixture(scope="session")
def db_engine(is_async: bool) -> Generator[Engine | AsyncEngine, None, None]:
    if is_async:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    else:
        engine = create_engine("sqlite+pysqlite:///:memory:", echo=False)

    yield engine

    maybe_run_coro(engine.dispose())


@pytest.fixture(scope="session")
def db_session_factory(db_engine: Engine | AsyncEngine) -> scoped_session[Session] | async_sessionmaker[AsyncSession]:
    """returns a SQLAlchemy scoped session factory"""
    if isinstance(db_engine, AsyncEngine):
        return async_sessionmaker(bind=db_engine)
    return scoped_session(sessionmaker(bind=db_engine))


@pytest.fixture()
def db_session(
    db_session_factory: scoped_session[Session] | async_sessionmaker[AsyncSession],
) -> Generator[Session | AsyncSession, None, None]:
    """yields a SQLAlchemy session which is rollbacked after the test"""
    session = db_session_factory()

    yield session

    maybe_run_coro(session.rollback())
    maybe_run_coro(session.close())
