from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any, get_args

from graphql import (
    GraphQLBoolean,
    GraphQLFloat,
    GraphQLInputField,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLScalarType,
    GraphQLString,
)
from sqlalchemy import ARRAY, Boolean, Float, Integer
from sqlalchemy.dialects.postgresql import ARRAY as PGARRAY

if TYPE_CHECKING:
    from sqlalchemy.types import TypeEngine


def is_gql_collection_type(typ: type[Any]) -> bool:
    return issubclass(typ, Collection) and not isinstance(typ, (str, bytes, Mapping))


def get_graphql_type_from_python(
    typ: type[str | int | float | bool]
) -> GraphQLNonNull[GraphQLScalarType] | GraphQLNonNull[GraphQLList[GraphQLNonNull[Any]]]:
    if issubclass(typ, int):
        return GraphQLNonNull(GraphQLInt)

    if issubclass(typ, float):
        return GraphQLNonNull(GraphQLFloat)

    if issubclass(typ, bool):
        return GraphQLNonNull(GraphQLBoolean)

    if issubclass(typ, str):
        return GraphQLNonNull(GraphQLString)

    if is_gql_collection_type(typ):
        [typ_inner] = get_args(typ)
        inner_type_gql = get_graphql_type_from_python(typ_inner)
        return GraphQLNonNull(GraphQLList(GraphQLNonNull(inner_type_gql)))

    raise TypeError(f"Unsupported type: {typ}")


def get_graphql_type_from_column(column_type: TypeEngine[Any]) -> GraphQLScalarType | GraphQLList[GraphQLNonNull[Any]]:
    if isinstance(column_type, Integer):
        return GraphQLInt

    if isinstance(column_type, Float):
        return GraphQLFloat

    if isinstance(column_type, Boolean):
        return GraphQLBoolean

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
