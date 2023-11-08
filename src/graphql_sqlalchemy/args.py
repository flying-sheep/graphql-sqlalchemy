from __future__ import annotations

from typing import TYPE_CHECKING

from graphql import GraphQLArgument, GraphQLArgumentMap, GraphQLInt, GraphQLList, GraphQLNonNull

from .graphql_types import get_graphql_type_from_column
from .helpers import get_table, has_int
from .inputs import (
    ON_CONFLICT_INPUT,
    get_inc_input_type,
    get_insert_input_type,
    get_order_input_type,
    get_set_input_type,
    get_where_input_type,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import DeclarativeBase

    from .types import Inputs, Objects

PAGINATION_ARGS = {"limit": GraphQLInt, "offset": GraphQLInt}


def make_args(model: type[DeclarativeBase], inputs: Inputs, objects: Objects) -> GraphQLArgumentMap:
    args = {
        "order": GraphQLArgument(GraphQLList(GraphQLNonNull(get_order_input_type(model, inputs)))),
        "where": GraphQLArgument(get_where_input_type(model, inputs, objects)),
    }

    for name, field in PAGINATION_ARGS.items():
        args[name] = GraphQLArgument(field)

    return args


def make_pk_args(model: type[DeclarativeBase], objects: Objects) -> GraphQLArgumentMap:
    primary_key = get_table(model).primary_key

    args = {}
    for column in primary_key.columns:
        graphql_type = get_graphql_type_from_column(column.type, objects)
        args[column.name] = GraphQLArgument(GraphQLNonNull(graphql_type))

    return args


def make_insert_args(model: type[DeclarativeBase], inputs: Inputs, objects: Objects) -> GraphQLArgumentMap:
    return {
        "objects": GraphQLArgument(
            GraphQLNonNull(GraphQLList(GraphQLNonNull(get_insert_input_type(model, inputs, objects))))
        ),
        "on_conflict": GraphQLArgument(ON_CONFLICT_INPUT),
    }


def make_insert_one_args(model: type[DeclarativeBase], inputs: Inputs, objects: Objects) -> GraphQLArgumentMap:
    return {
        "object": GraphQLArgument(get_insert_input_type(model, inputs, objects)),
        "on_conflict": GraphQLArgument(ON_CONFLICT_INPUT),
    }


def make_delete_args(model: type[DeclarativeBase], inputs: Inputs, objects: Objects) -> GraphQLArgumentMap:
    return {"where": GraphQLArgument(get_where_input_type(model, inputs, objects))}


def make_update_args(model: type[DeclarativeBase], inputs: Inputs, objects: Objects) -> GraphQLArgumentMap:
    return {
        **({"_inc": GraphQLArgument(get_inc_input_type(model, inputs, objects))} if has_int(model) else {}),
        "_set": GraphQLArgument(get_set_input_type(model, inputs, objects)),
        "where": GraphQLArgument(get_where_input_type(model, inputs, objects)),
    }


def make_update_by_pk_args(model: type[DeclarativeBase], inputs: Inputs, objects: Objects) -> GraphQLArgumentMap:
    return {
        **({"_inc": GraphQLArgument(get_inc_input_type(model, inputs, objects))} if has_int(model) else {}),
        "_set": GraphQLArgument(get_set_input_type(model, inputs, objects)),
        **make_pk_args(model, objects),
    }
