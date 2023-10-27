from __future__ import annotations

import sys
from asyncio import run
from collections.abc import AsyncGenerator, Callable
from textwrap import indent
from typing import Any

import pytest
from graphql import ExecutionResult, GraphQLSchema, graphql, graphql_sync
from graphql_sqlalchemy.schema import build_schema
from sqlalchemy import Column, Engine, ForeignKey, Table
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, registry, relationship

if sys.version_info < (3, 11):
    from exceptiongroup import ExceptionGroup


class Base(DeclarativeBase):
    registry = registry()


article_tag_association = Table(
    "article_tag",
    Base.metadata,
    Column("article_title", ForeignKey("article.title"), primary_key=True),
    Column("tag_name", ForeignKey("tag.name"), primary_key=True),
)


class Author(Base):
    __tablename__ = "author"
    name: Mapped[str] = mapped_column(primary_key=True)
    articles: Mapped[list[Article]] = relationship(back_populates="author")


class Article(Base):
    __tablename__ = "article"
    title: Mapped[str] = mapped_column(primary_key=True)
    author_id: Mapped[int] = mapped_column(ForeignKey("author.name"))
    author: Mapped[Author] = relationship(back_populates="articles")
    rating: Mapped[int]
    tags: Mapped[list[Tag]] = relationship(back_populates="articles", secondary=article_tag_association)


class Tag(Base):
    __tablename__ = "tag"
    name: Mapped[str] = mapped_column(primary_key=True)
    articles: Mapped[list[Article]] = relationship(back_populates="tags", secondary=article_tag_association)


def add_example_data(db_session: Session | AsyncSession) -> None:
    db_session.add(tag_politics := Tag(name="Politics"))
    db_session.add(tag_sports := Tag(name="Sports"))

    db_session.add(felicias := Author(name="Felicitas"))
    db_session.add_all(
        [
            Article(title="Felicitas good", author=felicias, rating=4),
            Article(title="Felicitas better", author=felicias, rating=5, tags=[tag_politics, tag_sports]),
        ]
    )
    db_session.add(bjork := Author(name="Bjørk"))
    db_session.add_all(
        [
            Article(title="Bjørk bad", author=bjork, rating=2),
            Article(title="Bjørk good", author=bjork, rating=4, tags=[tag_politics]),
        ]
    )
    db_session.add(lundth := Author(name="Lundth"))
    db_session.add_all(
        [
            Article(title="Lundth bad", author=lundth, rating=1, tags=[tag_sports]),
        ]
    )


@pytest.fixture(scope="session")
def gql_schema() -> GraphQLSchema:
    return build_schema(Base)


@pytest.fixture()
async def example_session(
    db_engine: Engine | AsyncEngine, db_session: Session | AsyncSession
) -> AsyncGenerator[Session | AsyncSession, None]:
    if isinstance(db_engine, AsyncEngine):
        async with db_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    else:
        Base.metadata.create_all(bind=db_engine)

    if isinstance(db_session, AsyncSession):
        async with db_session.begin():
            add_example_data(db_session)
            await db_session.commit()
    else:
        with db_session.begin():
            add_example_data(db_session)
            db_session.commit()

    yield db_session

    if isinstance(db_engine, AsyncEngine):
        async with db_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
    else:
        Base.metadata.drop_all(bind=db_engine)


def raise_if_errors(result: ExecutionResult) -> None:
    if result.errors:
        raise result.errors[0] if len(result.errors) == 1 else ExceptionGroup("Invalid Query", result.errors)


@pytest.fixture()
def graphql_example(
    example_session: Session | AsyncSession, gql_schema: GraphQLSchema
) -> Callable[[str], dict[str, Any]]:
    def graphql_(source: str) -> dict[str, Any]:
        async def gql_async(session: AsyncSession) -> ExecutionResult:
            assert isinstance(session, AsyncSession)
            async with session.begin():
                result = await graphql(gql_schema, source, context_value={"session": session})
                raise_if_errors(result)
                return result

        if isinstance(example_session, AsyncSession):
            result = run(gql_async(example_session))
        else:
            with example_session.begin():
                result = graphql_sync(gql_schema, source, context_value={"session": example_session})
                raise_if_errors(result)

        assert not example_session.in_transaction()
        assert result.data is not None
        return result.data

    return graphql_


@pytest.fixture()
def query_example(graphql_example: Callable[[str], dict[str, Any]]) -> Callable[[str], dict[str, Any]]:
    def query(source: str) -> dict[str, Any]:
        return graphql_example(f"query {{\n{indent(source, '    ')}\n}}")

    return query


@pytest.fixture()
def mutation_example(graphql_example: Callable[[str], dict[str, Any]]) -> Callable[[str], dict[str, Any]]:
    def mutation(source: str) -> dict[str, Any]:
        return graphql_example(f"mutation {{\n{indent(source, '    ')}\n}}")

    return mutation
