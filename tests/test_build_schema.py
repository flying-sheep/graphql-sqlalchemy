from __future__ import annotations

from collections.abc import Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

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
from sqlalchemy import JSON, Column, Dialect, ForeignKey, Integer, Table, TypeDecorator
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, registry, relationship

if TYPE_CHECKING:
    from sqlalchemy.types import TypeEngine

# Tested types


class SomeEnum(Enum):
    a = 1
    b = 1


T = TypeVar("T")


class JsonArray(TypeDecorator[Sequence[T]]):
    impl = JSON
    cache_ok = True

    item_type: TypeEngine[T]

    def __init__(self, item_type: TypeEngine[T], none_as_null: bool = False):
        super().__init__(none_as_null=none_as_null)
        self.item_type = item_type

    def process_bind_param(self, value: Sequence[T] | None, dialect: Dialect) -> Sequence[T] | None:
        if value is None:
            return None
        if not isinstance(value, Sequence):
            raise ValueError("value must be a sequence")
        if not all(isinstance(v, self.item_type.python_type) for v in value):
            raise ValueError(f"all values must be of type {self.item_type.python_type}")
        return value

    def process_result_value(self, value: Any | None, dialect: Dialect) -> list[T]:
        if not isinstance(value, list):
            raise ValueError("value must be a list")
        return value

    @property
    def python_type(self) -> type[list[T]]:
        return list


# SQLAlchemy models


class Base(DeclarativeBase):
    registry = registry()


user_project_association = Table(
    "user_project",
    Base.metadata,
    Column("user_id", ForeignKey("user.some_id"), primary_key=True),
    Column("project_id", ForeignKey("project.some_id"), primary_key=True),
)


class User(Base):
    __tablename__ = "user"

    some_id: Mapped[int] = mapped_column(primary_key=True)
    some_string: Mapped[str] = mapped_column(unique=True, index=True, nullable=False)
    some_bool: Mapped[bool] = mapped_column(nullable=False)
    some_enum: Mapped[SomeEnum] = mapped_column(nullable=False)
    some_custom: Mapped[list[int]] = mapped_column(JsonArray(Integer()), nullable=False)

    projects: Mapped[list[Project]] = relationship(back_populates="users", secondary=user_project_association)


class Project(Base):
    """Multiple projects per user, multiple users per project"""

    __tablename__ = "project"

    some_id: Mapped[int] = mapped_column(primary_key=True)

    users: Mapped[list[User]] = relationship(back_populates="projects", secondary=user_project_association)


@pytest.mark.parametrize(
    ("field", "gql_type"),
    [
        pytest.param("some_id", GraphQLInt, id="int"),
        pytest.param("some_string", GraphQLString, id="str"),
        pytest.param("some_bool", GraphQLBoolean, id="bool"),
        pytest.param("some_enum", GraphQLEnumType("someenum", SomeEnum.__members__), id="enum"),
        pytest.param("some_custom", GraphQLList(GraphQLNonNull(GraphQLString)), id="arr"),
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
