from __future__ import annotations

import sys
from collections.abc import Collection
from functools import singledispatch
from typing import TYPE_CHECKING, Any, Literal, get_args, get_origin

from graphql import (
    GraphQLBoolean,
    GraphQLEnumType,
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
from sqlalchemy import ARRAY, Boolean, Enum, Float, Integer
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
def _(
    typ: UnionType, objects: Objects
) -> GraphQLScalarType | GraphQLObjectType | GraphQLEnumType | GraphQLList[GraphQLNonNull[Any]]:
    types = set(get_args(typ)) - {type(None)}
    if len(types) != 1:
        raise NotImplementedError(f"Unsupported union type: {typ} with args {types}")
    return get_graphql_type_from_python_inner(types.pop(), objects)


@get_graphql_type_from_python.register(type)
@get_graphql_type_from_python.register(type(list[str]))  # _GenericAlias
@get_graphql_type_from_python.register(type(Literal[1]))  # _LiteralGenericAlias
def _(
    typ: type[str | int | float | bool | DeclarativeBase], objects: Objects
) -> GraphQLNonNull[GraphQLScalarType | GraphQLObjectType | GraphQLEnumType | GraphQLList[GraphQLNonNull[Any]]]:
    inner = get_graphql_type_from_python_inner(typ, objects)
    return GraphQLNonNull(inner)


def get_graphql_type_from_python_inner(
    typ: type[str | int | float | bool | DeclarativeBase], objects: Objects
) -> GraphQLScalarType | GraphQLObjectType | GraphQLEnumType | GraphQLList[GraphQLNonNull[Any]]:
    # doesn’t support issubclass
    if get_origin(typ) is Literal:
        name = "_"  # TODO: add support for more than one enum
        if (enum := objects.get(name)) is None:
            enum = GraphQLEnumType(name, dict.fromkeys(get_args(typ)), names_as_values=True)
            objects[name] = enum
        if not isinstance(enum, GraphQLEnumType):
            raise RuntimeError(f"Object type {name} already exists and is not an enum: {enum}")
        return enum
    # all these do
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


def get_graphql_type_from_column(
    column_type: TypeEngine[Any], objects: Objects
) -> GraphQLScalarType | GraphQLEnumType | GraphQLList[GraphQLNonNull[Any]]:
    if isinstance(column_type, Boolean):
        return GraphQLBoolean
    if isinstance(column_type, Integer):
        return GraphQLInt
    if isinstance(column_type, Float):
        return GraphQLFloat
    if isinstance(column_type, (ARRAY, PGARRAY)):
        inner_type_gql = get_graphql_type_from_column(column_type.item_type, objects)
        return GraphQLList(GraphQLNonNull(inner_type_gql))
    if isinstance(column_type, Enum):
        if not column_type.name:
            raise ValueError(f"Enum for {column_type} must have a name")
        name = column_type.name
        if (enum := objects.get(name)) is None:
            if column_type.enum_class:
                enum = GraphQLEnumType(name, column_type.enum_class)
            else:
                enum = GraphQLEnumType(name, dict.fromkeys(column_type.enums), names_as_values=True)
            objects[name] = enum
        if not isinstance(enum, GraphQLEnumType):
            raise RuntimeError(f"Object type {name} already exists and is not an enum: {enum}")
        return enum
    return GraphQLString


def get_base_comparison_fields(
    graphql_type: GraphQLScalarType | GraphQLEnumType | GraphQLList[Any]
) -> dict[str, GraphQLInputField]:
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
