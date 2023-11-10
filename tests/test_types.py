from __future__ import annotations

import sys
from enum import Enum
from typing import TYPE_CHECKING, Any

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
)
from graphql_sqlalchemy.graphql_types import get_graphql_type_from_column, get_graphql_type_from_python
from graphql_sqlalchemy.testing import assert_equal_gql_type
from sqlalchemy import ARRAY, Boolean, Column, Float, Integer, String
from sqlalchemy import Enum as SqlaEnum

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
            SqlaEnum(Enum("e", {"a": 1, "b": 2})),
            GraphQLEnumType("e", {"a": 1, "b": 2}, names_as_values=True),
            id="enum",
        ),
        pytest.param(ARRAY(String), GraphQLList(GraphQLNonNull(GraphQLString)), id="arr"),
    ],
)
def test_get_graphql_type_from_column(
    sqla_type: TypeEngine[Any], expected: GraphQLScalarType | GraphQLList[Any]
) -> None:
    column = Column("name", sqla_type)
    converted = get_graphql_type_from_column(column.type, {})
    assert_equal_gql_type(converted, expected)


@pytest.mark.parametrize(
    ("py_type", "expected"),
    [
        pytest.param(int, GraphQLNonNull(GraphQLInt), id="int"),
        pytest.param(float, GraphQLNonNull(GraphQLFloat), id="float"),
        pytest.param(bool, GraphQLNonNull(GraphQLBoolean), id="bool"),
        pytest.param(str, GraphQLNonNull(GraphQLString), id="str"),
        pytest.param(
            Enum("E1", {"a": 1, "b": 2}),
            GraphQLNonNull(GraphQLEnumType("e1", {"a": 1, "b": 2}, names_as_values=True)),
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
    assert_equal_gql_type(converted, expected)
