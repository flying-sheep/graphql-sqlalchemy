from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from graphql import (
    GraphQLBoolean,
    GraphQLFloat,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLScalarType,
    GraphQLString,
    is_equal_type,
)
from graphql_sqlalchemy.graphql_types import get_graphql_type_from_column
from sqlalchemy import ARRAY, Boolean, Column, Float, Integer, String

if TYPE_CHECKING:
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
