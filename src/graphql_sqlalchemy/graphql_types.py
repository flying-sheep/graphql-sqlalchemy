from __future__ import annotations

import sys
from collections.abc import Collection
from enum import Enum
from functools import singledispatch
from typing import TYPE_CHECKING, Any, cast, get_args, get_origin

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
from sqlalchemy import ARRAY, Boolean, Float, Integer, TypeDecorator
from sqlalchemy import Enum as SqlaEnum
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeEngine

from graphql_sqlalchemy.names import get_table_name

if sys.version_info >= (3, 10):
    from types import UnionType
else:
    from typing import Union as _U

    UnionType = type(_U[int, str])

if TYPE_CHECKING:
    from .types import Objects


class NativeGraphQLEnumType(GraphQLEnumType):
    """A GraphQL enum type mapping to Python Enum members, not their `.value` attributes."""

    def __init__(self, name: str, enum: type[Enum], *args: Any, values: None = None, **kwargs: Any) -> None:
        if values is not None:
            raise TypeError("Specify native `enum` type, not `values`.")
        super().__init__(name, enum.__members__, *args, **kwargs)


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
def _(
    typ: type[str | int | float | bool | DeclarativeBase], objects: Objects
) -> GraphQLNonNull[GraphQLScalarType | GraphQLObjectType | GraphQLEnumType | GraphQLList[GraphQLNonNull[Any]]]:
    inner = get_graphql_type_from_python_inner(typ, objects)
    return GraphQLNonNull(inner)


def get_graphql_type_from_python_inner(
    typ: type[str | int | float | bool | DeclarativeBase], objects: Objects
) -> GraphQLScalarType | GraphQLObjectType | GraphQLEnumType | GraphQLList[GraphQLNonNull[Any]]:
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
    if isinstance((origin := get_origin(typ)), type) and issubclass(origin, Collection):
        [typ_inner] = get_args(typ)
        inner_type_gql = get_graphql_type_from_python(typ_inner, objects)
        assert isinstance(inner_type_gql, GraphQLNonNull)
        return GraphQLList(inner_type_gql)
    if issubclass(typ, Enum):
        name = typ.__name__.lower()
        if (enum := objects.get(name)) is None:
            objects[name] = enum = NativeGraphQLEnumType(name, typ)
        if not isinstance(enum, GraphQLEnumType):
            raise RuntimeError(f"Object type {name} already exists and is not an enum: {enum}")
        return enum
    raise TypeError(f"Unsupported type: {typ} of type {type(typ)}")


def _get_array_item_type(column_type: TypeEngine[Any]) -> TypeEngine[Any] | None:
    if isinstance(column_type, ARRAY):
        return column_type.item_type
    if not (
        isinstance(column_type, TypeDecorator)
        and issubclass(column_type.python_type, (list, tuple))
        and hasattr(column_type, "item_type")
    ):
        return None
    return cast(TypeEngine[Any], column_type.item_type)


def get_graphql_type_from_column(
    column_type: TypeEngine[Any], objects: Objects
) -> GraphQLScalarType | GraphQLEnumType | GraphQLList[GraphQLNonNull[Any]]:
    if isinstance(column_type, Boolean):
        return GraphQLBoolean
    if isinstance(column_type, Integer):
        return GraphQLInt
    if isinstance(column_type, Float):
        return GraphQLFloat
    if item_type := _get_array_item_type(column_type):
        inner_type_gql = get_graphql_type_from_column(item_type, objects)
        return GraphQLList(GraphQLNonNull(inner_type_gql))
    if isinstance(column_type, SqlaEnum):
        if not column_type.name:
            raise ValueError(f"Enum for {column_type} must have a name")
        name = column_type.name
        if (enum := objects.get(name)) is None:
            if not column_type.enum_class:
                return GraphQLString
            objects[name] = enum = NativeGraphQLEnumType(name, column_type.enum_class)
        if not isinstance(enum, GraphQLEnumType):
            raise RuntimeError(f"Object type {name} already exists and is not an enum: {enum}")
        return enum
    return GraphQLString


def get_base_comparison_fields(
    graphql_type: GraphQLScalarType | GraphQLEnumType | GraphQLList[Any],
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
