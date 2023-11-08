from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

import pytest
from graphql import (
    GraphQLBoolean,
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
from sqlalchemy import ARRAY, Boolean, Column, Float, Integer, String

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
        pytest.param(ARRAY(String), GraphQLList(GraphQLNonNull(GraphQLString)), id="arr"),
    ],
)
def test_get_graphql_type_from_column(
    sqla_type: TypeEngine[Any], expected: GraphQLScalarType | GraphQLList[Any]
) -> None:
    column = Column("name", sqla_type)
    assert is_equal_type(get_graphql_type_from_column(column.type), expected)


@pytest.mark.parametrize(
    ("py_type", "expected"),
    [
        pytest.param(int, GraphQLNonNull(GraphQLInt), id="int"),
        pytest.param(float, GraphQLNonNull(GraphQLFloat), id="float"),
        pytest.param(bool, GraphQLNonNull(GraphQLBoolean), id="bool"),
        pytest.param(str, GraphQLNonNull(GraphQLString), id="str"),
        pytest.param(str_or_none, GraphQLString, id="str|None"),
        pytest.param(list[str], GraphQLNonNull(GraphQLList(GraphQLNonNull(GraphQLString))), id="arr"),
    ],
)
def test_get_graphql_type_from_python(
    py_type: type[Any] | UnionType, expected: GraphQLScalarType | GraphQLObjectType | GraphQLList[Any]
) -> None:
    assert is_equal_type(get_graphql_type_from_python(py_type, {}), expected)
