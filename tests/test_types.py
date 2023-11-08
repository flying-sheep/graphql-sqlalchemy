from __future__ import annotations

from typing import Any

import pytest
from graphql import (
    GraphQLBoolean,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLScalarType,
    GraphQLString,
    is_equal_type,
)
from graphql_sqlalchemy.graphql_types import get_graphql_type_from_column
from sqlalchemy import ARRAY, Boolean, Column, Integer, String


@pytest.mark.parametrize(
    ("column", "expected"),
    [
        pytest.param(Column("int", Integer), GraphQLInt, id="int"),
        pytest.param(Column("float", Integer), GraphQLInt, id="float"),
        pytest.param(Column("bool", Boolean), GraphQLBoolean, id="bool"),
        pytest.param(Column("str", String), GraphQLString, id="str"),
        pytest.param(Column("arr", ARRAY(String)), GraphQLList(GraphQLNonNull(GraphQLString)), id="arr"),
    ],
)
def test_get_graphql_type_from_column(column: Column[Any], expected: GraphQLScalarType | GraphQLList[Any]) -> None:
    assert is_equal_type(get_graphql_type_from_column(column.type), expected)
