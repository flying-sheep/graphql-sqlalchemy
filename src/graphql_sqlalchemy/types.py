from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from graphql import GraphQLEnumType, GraphQLInputObjectType, GraphQLObjectType, GraphQLResolveInfo

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session

Objects = dict[str, GraphQLObjectType | GraphQLEnumType]
Inputs = dict[str, GraphQLInputObjectType]


class Context(TypedDict):
    session: Session | AsyncSession


class ResolveInfo(GraphQLResolveInfo):
    context: Context
