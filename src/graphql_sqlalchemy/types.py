from __future__ import annotations

from typing import TypedDict

from graphql import GraphQLInputObjectType, GraphQLObjectType, GraphQLResolveInfo
from sqlalchemy.orm import Session

Objects = dict[str, GraphQLObjectType]
Inputs = dict[str, GraphQLInputObjectType]


class Context(TypedDict):
    session: Session


class ResolveInfo(GraphQLResolveInfo):
    context: Context
