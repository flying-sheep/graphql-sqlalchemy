from __future__ import annotations

from functools import singledispatch
from typing import Literal

from graphql import GraphQLList, GraphQLScalarType
from sqlalchemy import Column
from sqlalchemy.orm import DeclarativeBase

from .helpers import get_table

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
    model: type[DeclarativeBase] | GraphQLScalarType | GraphQLList,
    field_name: str,
    column: Column | GraphQLScalarType | GraphQLList | None = None,
) -> str:
    raise NotImplementedError


@get_field_name.register(type)
def _(model: type[DeclarativeBase], field_name: str, column: Column | None = None) -> str:
    name = get_table_name(model)
    if isinstance(column, Column) and field_name == "key":
        return FIELD_NAMES[field_name].format(name, column.name)
    return FIELD_NAMES[field_name].format(name)


@get_field_name.register
def _(model: GraphQLScalarType | GraphQLList, field_name: Literal["comparison"]) -> str:
    if isinstance(model, GraphQLList):
        return FIELD_NAMES["arr_comparison"].format(model.of_type.name.lower())
    return FIELD_NAMES[field_name].format(getattr(model, "name").lower())
