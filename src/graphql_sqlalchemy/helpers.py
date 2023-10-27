from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import Float, Integer, Table

if TYPE_CHECKING:
    from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute, Mapper, RelationshipProperty


def get_table(model: type[DeclarativeBase]) -> Table:
    if not isinstance(model.__table__, Table):
        raise TypeError(f"{model.__tablename__!r} is not a Table, it’s a {type(model.__table__)}")
    return model.__table__


def get_mapper(model: type[DeclarativeBase] | InstrumentedAttribute[Any]) -> Mapper[Any]:
    return model.__mapper__


def get_relationships(model: type[DeclarativeBase]) -> list[tuple[str, RelationshipProperty[Any]]]:
    return get_mapper(model).relationships.items()


def has_int(model: type[DeclarativeBase]) -> bool:
    return any(isinstance(i.type, (Integer, Float)) for i in get_table(model).columns)
