from __future__ import annotations

from typing import cast

from graphql import GraphQLField, GraphQLInt, GraphQLNonNull, GraphQLObjectType
from graphql_sqlalchemy import build_schema
from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class User(Base):  # type: ignore
    __tablename__ = "test"

    some_id = Column(Integer, primary_key=True)
    some_string = Column(String(length=320), unique=True, index=True, nullable=False)
    some_bool = Column(Boolean, nullable=False)
    some_int = Column(Integer, nullable=False)


def test_build_schema() -> None:
    schema = build_schema(Base)
    user = cast(GraphQLObjectType | None, schema.get_type("test"))
    assert user
    f: GraphQLField = user.fields["some_id"]
    assert isinstance(f.type, GraphQLNonNull)
    assert f.type.of_type is GraphQLInt
