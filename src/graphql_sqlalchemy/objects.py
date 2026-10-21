from __future__ import annotations

from inspect import get_annotations
from typing import TYPE_CHECKING

from graphql import (
    GraphQLField,
    GraphQLFieldMap,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLNullableOutputType,
    GraphQLObjectType,
)
from sqlalchemy.orm import DeclarativeBase, interfaces

from .args import make_args
from .graphql_types import get_graphql_type_from_column, get_graphql_type_from_python
from .helpers import get_hybrid_properties, get_relationships, get_table
from .names import get_field_name, get_table_name
from .resolvers import make_field_resolver, make_many_resolver

if TYPE_CHECKING:
    from .types import Inputs, Objects


def build_object_type(model: type[DeclarativeBase], objects: Objects, inputs: Inputs) -> GraphQLObjectType:
    def get_fields() -> GraphQLFieldMap:
        fields = {}

        for column in get_table(model).columns:
            column_type = get_graphql_type_from_column(column.type, objects)
            fields[column.name] = GraphQLField(
                column_type if column.nullable else GraphQLNonNull(column_type),
                resolve=make_field_resolver(column.name),
            )

        for name, prop in get_hybrid_properties(model).items():
            typ = get_annotations(prop.fget, eval_str=True)["return"]
            graphql_type = get_graphql_type_from_python(typ, objects)
            fields[name] = GraphQLField(graphql_type, resolve=make_field_resolver(name))

        for name, relationship in get_relationships(model):
            [column_elem] = relationship.local_columns
            related_model: type[DeclarativeBase] = relationship.mapper.entity
            object_type: GraphQLNullableOutputType = objects[get_table_name(related_model)]
            is_filterable = relationship.direction in (interfaces.ONETOMANY, interfaces.MANYTOMANY)
            if is_filterable:
                object_type = GraphQLList(GraphQLNonNull(object_type))
                make_resolver = make_many_resolver
            else:
                make_resolver = make_field_resolver

            fields[name] = GraphQLField(
                object_type if column_elem.nullable else GraphQLNonNull(object_type),
                args=make_args(related_model, inputs, objects),
                resolve=make_resolver(name),
            )

        return fields

    return GraphQLObjectType(get_table_name(model), get_fields)


def build_mutation_response_type(model: type[DeclarativeBase], objects: Objects) -> GraphQLObjectType:
    type_name = get_field_name(model, "mutation_response")

    object_type = objects[get_table_name(model)]
    fields = {
        "affected_rows": GraphQLField(GraphQLNonNull(GraphQLInt)),
        "returning": GraphQLField(GraphQLNonNull(GraphQLList(GraphQLNonNull(object_type)))),
    }

    return GraphQLObjectType(type_name, fields)
