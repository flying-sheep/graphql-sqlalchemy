from __future__ import annotations

import sys
from collections.abc import Callable, Generator
from typing import Any, Literal

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


@pytest.mark.parametrize("filt", ["", "(where: { })"])
def test_all(query_example: Callable[[str], Any], filt: str) -> None:
    data = query_example(f"author{filt} {{ name }}")
    author_names = {author["name"] for author in data["author"]}
    assert author_names == {"Felicitas", "Bjørk", "Lundth"}


@pytest.mark.parametrize(
    ("condition", "expected"),
    [
        ("rating: { _gte: 4 }", {"Felicitas good", "Felicitas better", "Bjørk good"}),
        ("_not: { rating: { _gt: 3 } }", {"Bjørk bad", "Lundth bad"}),
    ],
)
def test_simple_filter(query_example: Callable[[str], Any], condition: str, expected: set[str]) -> None:
    data = query_example(f"article(where: {{ {condition} }}) {{ title }}")
    article_titles = {article["title"] for article in data["article"]}
    assert article_titles == expected


@pytest.mark.parametrize("op", [None, "and"])
def test_multi_filter(query_example: Callable[[str], Any], op: Literal[None, "and"]) -> None:
    c1 = "rating: { _gte: 4 }"
    c2 = 'tags: { name: { _eq: "Politics" } }'
    conditions = f"{c1} {c2}" if op is None else f"_{op}: [{{ {c1} }}, {{ {c2} }}]"
    data = query_example(f"article(where: {{ {conditions} }}) {{ title }}")
    article_titles = {article["title"] for article in data["article"]}
    assert article_titles == {"Felicitas better", "Bjørk good"}


@pytest.mark.parametrize(
    ("op", "expected"),
    [
        pytest.param("and", set(), id="and"),
        pytest.param("or", {"Felicitas", "Lundth"}, id="or"),
    ],
)
def test_and_or(query_example: Callable[[str], Any], op: Literal["and", "or"], expected: set[str]) -> None:
    is_feli = '{ name: { _eq: "Felicitas" } }'
    is_lundth = '{ name: { _eq: "Lundth" } }'
    condition = f"{{ _{op}: [{is_feli}, {is_lundth}] }}"
    data = query_example(f"author(where: {condition}) {{ id name }}")
    author_names = {author["name"] for author in data["author"]}
    assert author_names == expected


@pytest.mark.parametrize(
    "filter_author",
    [
        pytest.param("(where: { articles: { rating: { _gte: 4 } } })", id="author_filt"),
        pytest.param("", id="author_all"),
    ],
)
@pytest.mark.parametrize(
    "filter_article",
    [
        pytest.param("(where: { rating: { _gte: 4 } })", id="artcl_filt"),
        pytest.param("", id="artcl_all"),
    ],
)
def test_nested_filter_one2many(
    db_session: Session, query_example: Callable[[str], Any], filter_author: str, filter_article: str
) -> None:
    data = query_example(
        f"""
        author{filter_author} {{
            id name
            articles{filter_article} {{
                id title rating
            }}
        }}
        """
    )
    author_names = {author["name"] for author in data["author"]}
    if filter_author:
        assert author_names == {"Felicitas", "Bjørk"}
    else:
        assert len(author_names) == 3

    articles = [article for author in data["author"] for article in author["articles"]]
    article_titles = {article["title"] for article in articles}
    with db_session.begin():
        all_article_titles = {row[0] for row in db_session.query(Article.title).all()}
    if filter_article:
        assert article_titles == {"Felicitas good", "Felicitas better", "Bjørk good"}
    elif filter_author:
        assert article_titles == all_article_titles - {"Lundth bad"}
    else:
        assert article_titles == all_article_titles


def test_nested_filter_many2one(query_example: Callable[[str], Any]) -> None:
    data = query_example(
        """
        article(where: { author: { name: { _eq: "Lundth" } } }) {
            id title rating
        }
        """
    )
    article_titles = {article["title"] for article in data["article"]}
    assert article_titles == {"Lundth bad"}


def test_nested_filter_many2many(query_example: Callable[[str], Any]) -> None:
    data = query_example(
        """
        article(where: { tags: { name: { _eq: "Politics" } } }) {
            id title rating
        }
        """
    )
    article_titles = {article["title"] for article in data["article"]}
    assert article_titles == {"Felicitas better", "Bjørk good"}


@pytest.mark.parametrize(
    ("op", "expected"),
    [
        pytest.param("and", {"Bjørk"}, id="and"),
        pytest.param("or", {"Felicitas", "Bjørk", "Lundth"}, id="or"),
    ],
)
def test_nested_and_or(query_example: Callable[[str], Any], op: Literal["and", "or"], expected: set[str]) -> None:
    has_good = '{ articles: { title: { _like: "%good" } } }'
    has_bad = '{ articles: { title: { _like: "%bad" } } }'
    condition = f"{{ _{op}: [{has_good}, {has_bad}] }}"
    data = query_example(f"author(where: {condition}) {{ id name }}")
    author_names = {author["name"] for author in data["author"]}
    assert author_names == expected
