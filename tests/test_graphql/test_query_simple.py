from __future__ import annotations

from collections.abc import Callable
from typing import Any

# Returns either a single object or a dict of object name to object
QueryCallable = Callable[[str], dict[str, Any]]


def test_all(query_example: QueryCallable) -> None:
    data = query_example("author { name }")
    author_names = {author["name"] for author in data["author"]}
    assert author_names == {"Felicitas", "Bjørk", "Lundth"}


def test_get_by_pk(query_example: QueryCallable) -> None:
    data = query_example('author_by_pk(name: "Felicitas") { name }')
    assert data["author_by_pk"] == {"name": "Felicitas"}
