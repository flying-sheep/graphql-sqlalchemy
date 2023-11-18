from __future__ import annotations

from typing import Any

from graphql import GraphQLEnumType, GraphQLEnumValueMap, GraphQLList, GraphQLNonNull, GraphQLType, is_equal_type


def assert_equal_gql_type(a: GraphQLType, b: GraphQLType) -> None:
    if is_equal_type(a, b):
        return
    if (isinstance(a, GraphQLNonNull) and isinstance(b, GraphQLNonNull)) or (
        isinstance(a, GraphQLList) and isinstance(b, GraphQLList)
    ):
        assert_equal_gql_type(a.of_type, b.of_type)
        return
    if isinstance(a, GraphQLEnumType) and isinstance(b, GraphQLEnumType):
        assert a.name == b.name
        assert mk_comparable_values(a.values) == mk_comparable_values(b.values)
        return
    raise NotImplementedError(f"Cannot compare {a!r} and {b!r}")


def mk_comparable_values(values: GraphQLEnumValueMap) -> dict[str, Any]:
    return {k: v.value for k, v in values.items()}
