from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

import pytest

# In this file, we only test `where` so we always get {typename: list[result]}
QueryCallable = Callable[[str], dict[str, list[dict[str, Any]]]]


def test_all(query_example: QueryCallable) -> None:
    data = query_example("author(where: { }) { name }")
    author_names = {author["name"] for author in data["author"]}
    assert author_names == {"Felicitas", "Bjørk", "Lundth"}


@pytest.mark.parametrize(
    ("condition", "expected"),
    [
        pytest.param("rating: { _gte: 4 }", {"Felicitas good", "Felicitas better", "Bjørk good"}, id="gte"),
        pytest.param("_not: { rating: { _gt: 3 } }", {"Bjørk bad", "Lundth bad"}, id="not_gt"),
    ],
)
def test_simple_filter(query_example: QueryCallable, condition: str, expected: set[str]) -> None:
    data = query_example(f"article(where: {{ {condition} }}) {{ title }}")
    article_titles = {article["title"] for article in data["article"]}
    assert article_titles == expected


@pytest.mark.parametrize("op", [None, "and"])
def test_multi_filter(query_example: QueryCallable, op: Literal[None, "and"]) -> None:
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
def test_and_or(query_example: QueryCallable, op: Literal["and", "or"], expected: set[str]) -> None:
    is_feli = '{ name: { _eq: "Felicitas" } }'
    is_lundth = '{ name: { _eq: "Lundth" } }'
    condition = f"{{ _{op}: [{is_feli}, {is_lundth}] }}"
    data = query_example(f"author(where: {condition}) {{ name }}")
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
def test_nested_filter_one2many(query_example: QueryCallable, filter_author: str, filter_article: str) -> None:
    data = query_example(
        f"""
        author{filter_author} {{
            name
            articles{filter_article} {{
                title rating
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
    all_article_titles = {article["title"] for article in query_example("article { title }")["article"]}
    if filter_article:
        assert article_titles == {"Felicitas good", "Felicitas better", "Bjørk good"}
    elif filter_author:
        assert article_titles == all_article_titles - {"Lundth bad"}
    else:
        assert article_titles == all_article_titles


def test_nested_filter_many2one(query_example: QueryCallable) -> None:
    data = query_example(
        """
        article(where: { author: { name: { _in: ["Lundth"] } } }) {
            title rating
        }
        """
    )
    article_titles = {article["title"] for article in data["article"]}
    assert article_titles == {"Lundth bad"}


def test_nested_filter_many2many(query_example: QueryCallable) -> None:
    data = query_example(
        """
        article(where: { tags: { name: { _eq: "Politics" } } }) {
            title rating
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
def test_nested_and_or(query_example: QueryCallable, op: Literal["and", "or"], expected: set[str]) -> None:
    has_good = '{ articles: { title: { _like: "%good" } } }'
    has_bad = '{ articles: { title: { _like: "%bad" } } }'
    condition = f"{{ _{op}: [{has_good}, {has_bad}] }}"
    data = query_example(f"author(where: {condition}) {{ name }}")
    author_names = {author["name"] for author in data["author"]}
    assert author_names == expected
