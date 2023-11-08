from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Literal

import pytest
from graphql import (
    GraphQLBoolean,
    GraphQLEnumType,
    GraphQLFloat,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLString,
    is_equal_type,
)
from graphql_sqlalchemy.graphql_types import get_graphql_type_from_column, get_graphql_type_from_python
from sqlalchemy import ARRAY, Boolean, Column, Enum, Float, Integer, String

if sys.version_info >= (3, 10):
    str_or_none = str | None
else:
    from typing import Union as _U

    str_or_none = _U[str, None]

if TYPE_CHECKING:
    from types import UnionType

    from sqlalchemy.sql.type_api import TypeEngine


@pytest.mark.parametrize(
    ("sqla_type", "expected"),
    [
        pytest.param(Integer, GraphQLInt, id="int"),
        pytest.param(Float, GraphQLFloat, id="float"),
        pytest.param(Boolean, GraphQLBoolean, id="bool"),
        pytest.param(String, GraphQLString, id="str"),
        pytest.param(
            Enum("a", "b", name="e"), GraphQLEnumType("e", dict.fromkeys(["a", "b"]), names_as_values=True), id="enum"
        ),
        pytest.param(ARRAY(String), GraphQLList(GraphQLNonNull(GraphQLString)), id="arr"),
    ],
)
def test_get_graphql_type_from_column(
    sqla_type: TypeEngine[Any], expected: GraphQLScalarType | GraphQLList[Any]
) -> None:
    column = Column("name", sqla_type)
    converted = get_graphql_type_from_column(column.type, {})
    assert is_equal_type(converted, expected) or is_equal_enum(converted, expected)


@pytest.mark.parametrize(
    ("py_type", "expected"),
    [
        pytest.param(int, GraphQLNonNull(GraphQLInt), id="int"),
        pytest.param(float, GraphQLNonNull(GraphQLFloat), id="float"),
        pytest.param(bool, GraphQLNonNull(GraphQLBoolean), id="bool"),
        pytest.param(str, GraphQLNonNull(GraphQLString), id="str"),
        pytest.param(
            Literal["a", "b"],
            GraphQLNonNull(GraphQLEnumType("_", dict.fromkeys(["a", "b"]), names_as_values=True)),
            id="enum",
        ),
        pytest.param(str_or_none, GraphQLString, id="str|None"),
        pytest.param(list[str], GraphQLNonNull(GraphQLList(GraphQLNonNull(GraphQLString))), id="arr"),
    ],
)
def test_get_graphql_type_from_python(
    py_type: type[Any] | UnionType, expected: GraphQLScalarType | GraphQLObjectType | GraphQLList[Any]
) -> None:
    converted = get_graphql_type_from_python(py_type, {})
    assert is_equal_type(converted, expected) or is_equal_enum(converted, expected)


def is_equal_enum(a: Any, b: Any) -> bool:
    if isinstance(a, GraphQLNonNull) and isinstance(b, GraphQLNonNull):
        return is_equal_enum(a.of_type, b.of_type)
    if not isinstance(a, GraphQLEnumType) or not isinstance(b, GraphQLEnumType):
        return False
    return a.name == b.name and a.values == b.values
