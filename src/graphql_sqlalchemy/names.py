from __future__ import annotations

from functools import singledispatch
from typing import TYPE_CHECKING, Any, Literal

from graphql import GraphQLEnumType, GraphQLList, GraphQLNonNull, GraphQLScalarType
from sqlalchemy import Column

from .helpers import get_table

if TYPE_CHECKING:
    from sqlalchemy.orm import DeclarativeBase

FIELD_NAMES = {
    "by_pk": "{}_by_pk",
    "order_by": "{}_order_by",
    "where": "{}_bool_exp",
    "insert": "insert_{}",
    "insert_one": "insert_{}_one",
    "insert_input": "{}_insert_input",
    "mutation_response": "{}_mutation_response",
    "update": "update_{}",
    "update_by_pk": "update_{}_by_pk",
    "delete": "delete_{}",
    "delete_by_pk": "delete_{}_by_pk",
    "inc_input": "{}_inc_input",
    "set_input": "{}_set_input",
    "comparison": "{}_comparison_exp",
    "arr_comparison": "arr_{}_comparison_exp",
    "constraint": "{}_constraint",
    "update_column": "{}_update_column",
    "on_conflict": "{}_on_conflict",
    "pkey": "{}_pkey",
    "key": "{}_{}_key",
}


def get_table_name(model: type[DeclarativeBase]) -> str:
    return get_table(model).name


@singledispatch
def get_field_name(
    model: type[DeclarativeBase] | GraphQLScalarType | GraphQLEnumType | GraphQLList[Any],
    field_name: str,
    column: Column[Any] | GraphQLScalarType | GraphQLList[Any] | None = None,
) -> str:
    raise NotImplementedError


@get_field_name.register(type)
def _(model: type[DeclarativeBase], field_name: str, column: Column[Any] | None = None) -> str:
    name = get_table_name(model)
    if isinstance(column, Column) and field_name == "key":
        return FIELD_NAMES[field_name].format(name, column.name)
    return FIELD_NAMES[field_name].format(name)


@get_field_name.register(GraphQLScalarType)
@get_field_name.register(GraphQLEnumType)
def _(model: GraphQLScalarType | GraphQLEnumType, field_name: Literal["comparison"]) -> str:
    return FIELD_NAMES[field_name].format(model.name).lower()


@get_field_name.register(GraphQLList)
def _(model: GraphQLList[Any], field_name: Literal["comparison"]) -> str:
    item_model = model.of_type.of_type if isinstance(model.of_type, GraphQLNonNull) else model.of_type
    return FIELD_NAMES[f"arr_{field_name}"].format(item_model.name.lower())
