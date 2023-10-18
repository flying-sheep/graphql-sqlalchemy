from __future__ import annotations

from collections.abc import Generator
from functools import partial
from itertools import starmap
from typing import Any, Callable, TypedDict

from graphql import GraphQLResolveInfo
from sqlalchemy import ColumnExpressionArgument, and_, not_, or_, true
from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute, Query, Session, interfaces

from .helpers import get_mapper


class InsDel(TypedDict):
    affected_rows: int
    returning: list[DeclarativeBase]


def get_bool_operation(
    model_property: InstrumentedAttribute, operator: str, value: Any
) -> ColumnExpressionArgument[bool]:
    if operator == "_eq":
        return model_property == value

    if operator == "_in":
        return model_property.in_(value)

    if operator == "_is_null":
        return model_property.is_(None)

    if operator == "_like":
        return model_property.like(value)

    if operator == "_neq":
        return model_property != value

    if operator == "_nin":
        return model_property.notin_(value)

    if operator == "_nlike":
        return model_property.notlike(value)

    if operator == "_lt":
        return model_property < value

    if operator == "_gt":
        return model_property > value

    if operator == "_lte":
        return model_property <= value

    if operator == "_gte":
        return model_property >= value

    raise Exception("Invalid operator")


def get_filter_operation(model: type[DeclarativeBase], where: dict[str, Any]) -> ColumnExpressionArgument[bool]:
    partial_filter = partial(get_filter_operation, model)

    for name, exprs in where.items():
        if name == "_or":
            return or_(*map(partial_filter, exprs))

        if name == "_not":
            return not_(partial_filter(exprs))

        if name == "_and":
            return and_(*map(partial_filter, exprs))

        model_property: InstrumentedAttribute = getattr(model, name)

        # relationships
        if relationship := get_mapper(model).relationships.get(name):
            related_model = relationship.entity.class_
            if relationship.direction in (interfaces.ONETOMANY, interfaces.MANYTOMANY):
                elem_filter = get_filter_operation(related_model, exprs)
                return model_property.any(elem_filter)
            # TODO: join
            return get_filter_operation(related_model, exprs)

        # fields
        partial_bool = partial(get_bool_operation, model_property)
        return and_(*starmap(partial_bool, exprs.items()))

    return true()


def filter_query(
    model: type[DeclarativeBase] | InstrumentedAttribute, query: Query, where: dict[str, Any] | None = None
) -> Query:
    if not where:
        return query

    query_filter = query.filter
    for name, exprs in where.items():
        query = query_filter(get_filter_operation(model, {name: exprs}))

    return query


def order_query(
    model: type[DeclarativeBase] | InstrumentedAttribute,
    query: Query,
    order: list[dict[str, Any]] | None = None,
) -> Query:
    if not order:
        return query

    for expr in order:
        for name, direction in expr.items():
            model_property = getattr(model, name)
            model_order = getattr(model_property, direction)
            query = query.order_by(model_order())

    return query


def resolve_filtered(
    model: type[DeclarativeBase] | InstrumentedAttribute,
    query: Query[DeclarativeBase],
    *,
    where: dict[str, Any] | None = None,
    order: list[dict[str, Any]] | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> list[DeclarativeBase]:
    query = filter_query(model, query, where)
    query = order_query(model, query, order)

    if limit:
        query = query.limit(limit)

    if offset:
        query = query.offset(offset)

    return query.all()


def make_field_resolver(field_name: str) -> Callable[..., Any]:
    def field_resolver(root: DeclarativeBase, _info: GraphQLResolveInfo) -> Any:
        return getattr(root, field_name)

    return field_resolver


def pk_filter(instance: DeclarativeBase) -> Generator[ColumnExpressionArgument, None, None]:
    model = instance.__class__
    for column in model.__table__.primary_key:
        yield getattr(model, column.name) == getattr(instance, column.name)


def make_many_resolver(field_name: str) -> Callable[..., list[DeclarativeBase]]:
    def resolver(
        root: DeclarativeBase,
        info: GraphQLResolveInfo,
        *,
        where: dict[str, Any] | None = None,
        order: list[dict[str, Any]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[DeclarativeBase]:
        if all(f is None for f in [where, order, limit, offset]):
            return getattr(root, field_name)
        session: Session = info.context["session"]
        relationship: InstrumentedAttribute = getattr(root.__class__, field_name)
        field_model = relationship.prop.entity.class_
        query = session.query(field_model).select_from(root.__class__).join(relationship).filter(*pk_filter(root))
        return resolve_filtered(field_model, query, where=where, order=order, limit=limit, offset=offset)

    return resolver


def make_object_resolver(model: type[DeclarativeBase]) -> Callable[..., list[DeclarativeBase]]:
    def resolver(
        _root: None,
        info: GraphQLResolveInfo,
        *,
        where: dict[str, Any] | None = None,
        order: list[dict[str, Any]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[DeclarativeBase]:
        session: Session = info.context["session"]
        query = session.query(model)
        return resolve_filtered(model, query, where=where, order=order, limit=limit, offset=offset)

    return resolver


def make_pk_resolver(model: type[DeclarativeBase]) -> Callable[..., DeclarativeBase | None]:
    def resolver(_root: None, info: GraphQLResolveInfo, **kwargs: dict[str, Any]) -> DeclarativeBase:
        session = info.context["session"]
        return session.query(model).get(kwargs)

    return resolver


def session_add_object(
    obj: dict[str, Any], model: type[DeclarativeBase], session: Session, on_conflict: dict[str, Any] | None = None
) -> DeclarativeBase:
    instance = model()
    for key, value in obj.items():
        setattr(instance, key, value)

    if on_conflict and on_conflict["merge"]:
        session.merge(instance)
    else:
        session.add(instance)
    return instance


def session_commit(session: Session) -> None:
    try:
        session.commit()
    except Exception:
        session.rollback()
        raise


def make_insert_resolver(model: type[DeclarativeBase]) -> Callable[..., InsDel]:
    def resolver(
        _root: None, info: GraphQLResolveInfo, objects: list[dict[str, Any]], on_conflict: dict[str, Any] | None = None
    ) -> InsDel:
        session = info.context["session"]
        models = []

        with session.no_autoflush:
            for obj in objects:
                instance = session_add_object(obj, model, session, on_conflict)
                models.append(instance)

        session_commit(session)
        return InsDel(affected_rows=len(models), returning=models)

    return resolver


def make_insert_one_resolver(model: type[DeclarativeBase]) -> Callable[..., DeclarativeBase]:
    def resolver(
        _root: None, info: GraphQLResolveInfo, object: dict[str, Any], on_conflict: dict[str, Any] | None = None
    ) -> DeclarativeBase:
        session = info.context["session"]

        instance = session_add_object(object, model, session, on_conflict)
        session_commit(session)
        return instance

    return resolver


def make_delete_resolver(model: type[DeclarativeBase]) -> Callable[..., InsDel]:
    def resolver(_root: None, info: GraphQLResolveInfo, where: dict[str, Any] | None = None) -> InsDel:
        session = info.context["session"]
        query = session.query(model)
        query = filter_query(model, query, where)

        rows = query.all()
        affected = query.delete()
        session_commit(session)

        return InsDel(affected_rows=affected, returning=rows)

    return resolver


def make_delete_by_pk_resolver(model: type[DeclarativeBase]) -> Callable[..., DeclarativeBase | None]:
    def resolver(_root: None, info: GraphQLResolveInfo, **kwargs: dict[str, Any]) -> DeclarativeBase | None:
        session: Session = info.context["session"]

        row: DeclarativeBase | None = session.query(model).get(kwargs)
        if row:
            session.delete(row)
            session_commit(session)
            return row

        return None

    return resolver


def update_query(
    query: Query,
    model: type[DeclarativeBase],
    _set: dict[str, Any] | None = None,
    _inc: dict[str, Any] | None = None,
) -> int:
    affected = 0
    if _inc:
        to_increment = {}
        for column_name, increment in _inc.items():
            to_increment[column_name] = getattr(model, column_name) + increment

        affected += query.update(to_increment)

    if _set:
        affected += query.update(_set)

    return affected


def make_update_resolver(model: type[DeclarativeBase]) -> Callable[..., InsDel]:
    def resolver(
        _root: None,
        info: GraphQLResolveInfo,
        where: dict[str, Any],
        _set: dict[str, Any] | None,
        _inc: dict[str, Any] | None,
    ) -> InsDel:
        session = info.context["session"]
        query = session.query(model)
        query = filter_query(model, query, where)
        affected = update_query(query, model, _set, _inc)
        session_commit(session)

        return InsDel(affected_rows=affected, returning=query.all())

    return resolver


def make_update_by_pk_resolver(model: type[DeclarativeBase]) -> Callable[..., DeclarativeBase | None]:
    def resolver(
        _root: None,
        info: GraphQLResolveInfo,
        _set: dict[str, Any] | None,
        _inc: dict[str, Any] | None,
        **pk_columns: dict[str, Any],
    ) -> DeclarativeBase | None:
        session = info.context["session"]
        query = session.query(model).filter_by(**pk_columns)

        if update_query(query, model, _set, _inc):
            session_commit(session)
            return query.one()

        return None

    return resolver
