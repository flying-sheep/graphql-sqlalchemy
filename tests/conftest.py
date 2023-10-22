from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker


@pytest.fixture(scope="session")
def db_engine() -> Generator[Engine, None, None]:
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=False)

    yield engine

    engine.dispose()


@pytest.fixture(scope="session")
def db_session_factory(db_engine: Engine) -> scoped_session[Session]:
    """returns a SQLAlchemy scoped session factory"""
    return scoped_session(sessionmaker(bind=db_engine))


@pytest.fixture()
def db_session(db_session_factory: scoped_session[Session]) -> Generator[Session, None, None]:
    """yields a SQLAlchemy session which is rollbacked after the test"""
    session = db_session_factory()

    yield session

    session.rollback()
    session.close()
