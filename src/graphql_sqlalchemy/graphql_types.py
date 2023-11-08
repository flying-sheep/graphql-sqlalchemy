from __future__ import annotations

import sys
from collections.abc import Collection
from functools import singledispatch
from typing import TYPE_CHECKING, Any, get_args, get_origin

from graphql import (
    GraphQLBoolean,
    GraphQLFloat,
    GraphQLInputField,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLOutputType,
    GraphQLScalarType,
    GraphQLString,
)
from sqlalchemy import ARRAY, Boolean, Float, Integer
from sqlalchemy.dialects.postgresql import ARRAY as PGARRAY
from sqlalchemy.orm import DeclarativeBase

from graphql_sqlalchemy.names import get_table_name

if sys.version_info >= (3, 10):
    from types import UnionType
else:
    from typing import Union as _U

    UnionType = type(_U[int, str])

if TYPE_CHECKING:
    from sqlalchemy.types import TypeEngine

    from .types import Objects


@singledispatch
def get_graphql_type_from_python(
    typ: type | UnionType, objects: Objects
) -> GraphQLOutputType | GraphQLList[GraphQLNonNull[Any]]:
    raise NotImplementedError(f"Unsupported type: {typ} of type {type(typ)}")


@get_graphql_type_from_python.register(UnionType)
def _(typ: UnionType, objects: Objects) -> GraphQLScalarType | GraphQLObjectType | GraphQLList[GraphQLNonNull[Any]]:
    types = set(get_args(typ)) - {type(None)}
    if len(types) != 1:
        raise NotImplementedError(f"Unsupported union type: {typ} with args {types}")
    return get_graphql_type_from_python_inner(types.pop(), objects)


@get_graphql_type_from_python.register(type)
@get_graphql_type_from_python.register(type(list[str]))  # _GenericAlias
def _(
    typ: type[str | int | float | bool | DeclarativeBase], objects: Objects
) -> GraphQLNonNull[GraphQLScalarType | GraphQLObjectType | GraphQLList[GraphQLNonNull[Any]]]:
    inner = get_graphql_type_from_python_inner(typ, objects)
    return GraphQLNonNull(inner)


def get_graphql_type_from_python_inner(
    typ: type[str | int | float | bool | DeclarativeBase], objects: Objects
) -> GraphQLScalarType | GraphQLObjectType | GraphQLList[GraphQLNonNull[Any]]:
    if issubclass(typ, bool):
        return GraphQLBoolean
    if issubclass(typ, int):
        return GraphQLInt
    if issubclass(typ, float):
        return GraphQLFloat
    if issubclass(typ, str):
        return GraphQLString
    if issubclass(typ, DeclarativeBase):
        return objects[get_table_name(typ)]
    if issubclass(get_origin(typ), Collection):
        [typ_inner] = get_args(typ)
        inner_type_gql = get_graphql_type_from_python(typ_inner, objects)
        assert isinstance(inner_type_gql, GraphQLNonNull)
        return GraphQLList(inner_type_gql)
    raise TypeError(f"Unsupported type: {typ} of type {type(typ)}")


def get_graphql_type_from_column(column_type: TypeEngine[Any]) -> GraphQLScalarType | GraphQLList[GraphQLNonNull[Any]]:
    if isinstance(column_type, Boolean):
        return GraphQLBoolean
    if isinstance(column_type, Integer):
        return GraphQLInt
    if isinstance(column_type, Float):
        return GraphQLFloat
    if isinstance(column_type, (ARRAY, PGARRAY)):
        inner_type_gql = get_graphql_type_from_column(column_type.item_type)
        return GraphQLList(GraphQLNonNull(inner_type_gql))
    return GraphQLString


def get_base_comparison_fields(graphql_type: GraphQLScalarType | GraphQLList[Any]) -> dict[str, GraphQLInputField]:
    return {
        "_eq": GraphQLInputField(graphql_type),
        "_neq": GraphQLInputField(graphql_type),
        "_in": GraphQLInputField(GraphQLList(GraphQLNonNull(graphql_type))),
        "_nin": GraphQLInputField(GraphQLList(GraphQLNonNull(graphql_type))),
        "_lt": GraphQLInputField(graphql_type),
        "_gt": GraphQLInputField(graphql_type),
        "_gte": GraphQLInputField(graphql_type),
        "_lte": GraphQLInputField(graphql_type),
        "_is_null": GraphQLInputField(GraphQLBoolean),
    }


def get_string_comparison_fields() -> dict[str, GraphQLInputField]:
    return {"_like": GraphQLInputField(GraphQLString), "_nlike": GraphQLInputField(GraphQLString)}
