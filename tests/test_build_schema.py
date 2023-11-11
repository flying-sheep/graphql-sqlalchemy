from __future__ import annotations

from enum import Enum
from typing import Union, cast

import pytest
from graphql import (
    GraphQLBoolean,
    GraphQLEnumType,
    GraphQLField,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLString,
)
from graphql_sqlalchemy import build_schema
from graphql_sqlalchemy.testing import assert_equal_gql_type
from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, registry, relationship


class Base(DeclarativeBase):
    registry = registry()


user_project_association = Table(
    "user_project",
    Base.metadata,
    Column("user_id", ForeignKey("user.some_id"), primary_key=True),
    Column("project_id", ForeignKey("project.some_id"), primary_key=True),
)


class SomeEnum(Enum):
    a = 1
    b = 1


class User(Base):
    __tablename__ = "user"

    some_id: Mapped[int] = mapped_column(primary_key=True)
    some_string: Mapped[str] = mapped_column(unique=True, index=True, nullable=False)
    some_bool: Mapped[bool] = mapped_column(nullable=False)
    some_enum: Mapped[SomeEnum] = mapped_column(nullable=False)

    projects: Mapped[list[Project]] = relationship(back_populates="users", secondary=user_project_association)


class Project(Base):
    """Multiple projects per user, multiple users per project"""

    __tablename__ = "project"

    some_id: Mapped[int] = mapped_column(primary_key=True)

    users: Mapped[list[User]] = relationship(back_populates="projects", secondary=user_project_association)


@pytest.mark.parametrize(
    ("field", "gql_type"),
    [
        ("some_id", GraphQLInt),
        ("some_string", GraphQLString),
        ("some_bool", GraphQLBoolean),
        ("some_enum", GraphQLEnumType("someenum", SomeEnum.__members__)),
    ],
)
def test_build_schema_simple(field: str, gql_type: GraphQLScalarType) -> None:
    schema = build_schema(Base)
    user = cast(Union[GraphQLObjectType, None], schema.get_type("user"))
    assert user
    f: GraphQLField = user.fields[field]
    assert_equal_gql_type(f.type, GraphQLNonNull(gql_type))


def test_build_schema_rel() -> None:
    schema = build_schema(Base)
    user = cast(Union[GraphQLObjectType, None], schema.get_type("user"))
    assert user
    f: GraphQLField = user.fields["projects"]
    assert isinstance(f.type, GraphQLNonNull)
    assert isinstance(f.type.of_type, GraphQLList)
    obj_type = f.type.of_type.of_type
    assert isinstance(obj_type, GraphQLNonNull)
    assert isinstance(obj_type.of_type, GraphQLObjectType)
