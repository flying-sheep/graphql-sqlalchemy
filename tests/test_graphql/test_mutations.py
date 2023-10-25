from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest
from graphql import GraphQLError

MutationCallable = Callable[[str], dict[str, Any]]
QueryCallable = Callable[[str], dict[str, Any]]


def test_insert_one(mutation_example: MutationCallable, query_example: QueryCallable) -> None:
    mut_data = mutation_example(
        """
        insert_author_one(object: { name: "Lisa" }) {
            name
        }
        """
    )
    assert mut_data["insert_author_one"] == {"name": "Lisa"}

    q_data = query_example("author { name }")
    author_names = {author["name"] for author in q_data["author"]}
    assert author_names == {"Felicitas", "Bjørk", "Lundth", "Lisa"}


@pytest.mark.parametrize("merge", [True, False], ids=["merge", "no_merge"])
def test_insert_many(mutation_example: MutationCallable, query_example: QueryCallable, merge: bool) -> None:
    mut = f"""
        insert_author(
            objects: [{{ name: "Lisa" }}, {{ name: "Bjørk" }}]
            on_conflict: {{ merge: {json.dumps(merge)} }}
        ) {{
            returning {{ name }}
            affected_rows
        }}
        """
    if not merge:
        with pytest.raises(GraphQLError, match=r"New instance <Author.*conflicts"):
            mutation_example(mut)
        return

    data = mutation_example(mut)
    expected = {
        "returning": [{"name": "Lisa"}, {"name": "Bjørk"}],
        "affected_rows": 2,
    }
    assert data["insert_author"] == expected

    q_data = query_example("author { name }")
    author_names = sorted(author["name"] for author in q_data["author"])
    assert author_names == ["Bjørk", "Felicitas", "Lisa", "Lundth"]
