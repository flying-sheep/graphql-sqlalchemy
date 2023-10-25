from __future__ import annotations

import sys
from collections.abc import Callable, Generator
from typing import Any

import pytest
from graphql import GraphQLSchema, graphql_sync
from graphql_sqlalchemy.schema import build_schema
from sqlalchemy import Column, Engine, ForeignKey, Table
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, registry, relationship

if sys.version_info < (3, 11):
    from exceptiongroup import ExceptionGroup


class Base(DeclarativeBase):
    registry = registry()


article_tag_association = Table(
    "article_tag",
    Base.metadata,
    Column("article_id", ForeignKey("article.id"), primary_key=True),
    Column("tag_id", ForeignKey("tag.id"), primary_key=True),
)


class Author(Base):
    __tablename__ = "author"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    articles: Mapped[list[Article]] = relationship(back_populates="author")


class Article(Base):
    __tablename__ = "article"
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str]
    author_id: Mapped[int] = mapped_column(ForeignKey("author.id"))
    author: Mapped[Author] = relationship(back_populates="articles")
    rating: Mapped[int]
    tags: Mapped[list[Tag]] = relationship(back_populates="articles", secondary=article_tag_association)


class Tag(Base):
    __tablename__ = "tag"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    articles: Mapped[list[Article]] = relationship(back_populates="tags", secondary=article_tag_association)


@pytest.fixture(scope="session")
def gql_schema() -> GraphQLSchema:
    return build_schema(Base)


@pytest.fixture()
def example_session(db_engine: Engine, db_session: Session) -> Generator[Session, None, None]:
    Base.metadata.create_all(bind=db_engine)
    with db_session.begin():
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
        db_session.commit()

    yield db_session

    Base.metadata.drop_all(bind=db_engine)


@pytest.fixture()
def query_example(example_session: Session, gql_schema: GraphQLSchema) -> Callable[[str], Any]:
    def query(q: str) -> Any:
        source = f"query {{ {q} }}"
        result = graphql_sync(gql_schema, source, context_value={"session": example_session})
        if example_session._transaction:
            # TODO: make unnecessary
            example_session._transaction.close()
        if result.errors:
            raise result.errors[0] if len(result.errors) == 1 else ExceptionGroup("Invalid Query", result.errors)
        return result.data

    return query
