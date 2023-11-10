from __future__ import annotations

from typing import Any

from graphql import GraphQLEnumType, GraphQLEnumValueMap, GraphQLNonNull, is_equal_type


def assert_equal_gql_type(a: Any, b: Any) -> None:
    if is_equal_type(a, b):
        return
    if isinstance(a, GraphQLNonNull) and isinstance(b, GraphQLNonNull):
        assert_equal_gql_type(a.of_type, b.of_type)
        return
    assert type(a) is type(b)
    assert isinstance(a, GraphQLEnumType)
    assert isinstance(b, GraphQLEnumType)
    assert a.name == b.name
    assert mk_comparable_values(a.values) == mk_comparable_values(b.values)


def mk_comparable_values(values: GraphQLEnumValueMap) -> Any:
    return {k: v.value for k, v in values.items()}
