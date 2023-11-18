from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, TypeVar

from graphql import GraphQLEnumType, GraphQLEnumValueMap, GraphQLList, GraphQLNonNull, GraphQLType, is_equal_type
from sqlalchemy import JSON, Dialect, TypeDecorator

if TYPE_CHECKING:
    from sqlalchemy.types import TypeEngine


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
    msg = f"{a} != {b}"
    raise AssertionError(msg)


def mk_comparable_values(values: GraphQLEnumValueMap) -> dict[str, Any]:
    return {k: v.value for k, v in values.items()}


T = TypeVar("T")


class JsonArray(TypeDecorator[Sequence[T]]):
    impl = JSON
    cache_ok = True

    item_type: TypeEngine[T]

    def __init__(self, item_type: TypeEngine[T] | type[TypeEngine[T]], none_as_null: bool = False):
        super().__init__(none_as_null=none_as_null)
        self.item_type = item_type() if isinstance(item_type, type) else item_type

    def process_bind_param(self, value: Sequence[T] | None, dialect: Dialect) -> Sequence[T] | None:
        if value is None:
            return None
        if not isinstance(value, Sequence):
            raise ValueError("value must be a sequence")
        if not all(isinstance(v, self.item_type.python_type) for v in value):
            raise ValueError(f"all values must be of type {self.item_type.python_type}")
        return value

    def process_result_value(self, value: Any | None, dialect: Dialect) -> list[T]:
        if not isinstance(value, list):
            raise ValueError("value must be a list")
        return value

    @property
    def python_type(self) -> type[list[T]]:
        return list
