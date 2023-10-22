from __future__ import annotations

from collections.abc import Generator, Sequence
from functools import partial
from itertools import starmap
from typing import Any, Callable, TypedDict, TypeVar

from sqlalchemy import ColumnExpressionArgument, Select, and_, delete, not_, or_, select, true, update
from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute, Session, interfaces
from sqlalchemy.sql.dml import ReturningDelete, ReturningUpdate

from .helpers import get_mapper
from .types import ResolveInfo


class InsDel(TypedDict):
    affected_rows: int
    returning: Sequence[DeclarativeBase]


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
            # else *TOONE:
            return model_property.and_(get_filter_operation(related_model, exprs))

        # fields
        partial_bool = partial(get_bool_operation, model_property)
        return and_(*starmap(partial_bool, exprs.items()))

    return true()


W = TypeVar(
    "W",
    Select[tuple[DeclarativeBase]],
    ReturningUpdate[tuple[DeclarativeBase]],
    ReturningDelete[tuple[DeclarativeBase]],
)


def filter_selection(
    model: type[DeclarativeBase],
    selection: W,
    *,
    where: dict[str, Any] | None = None,
) -> W:
    if not where:
        return selection

    for name, exprs in where.items():
        selection = selection.filter(get_filter_operation(model, {name: exprs}))

    return selection


def order_selection(
    model: type[DeclarativeBase] | InstrumentedAttribute,
    selection: Select[tuple[DeclarativeBase]] | None = None,
    order: list[dict[str, Any]] | None = None,
) -> Select[tuple[DeclarativeBase]]:
    if selection is None:
        selection = select(model)
    if not order:
        return selection

    for expr in order:
        for name, direction in expr.items():
            model_property = getattr(model, name)
            model_order = getattr(model_property, direction)
            selection = selection.order_by(model_order())

    return selection


def resolve_filtered(
    model: type[DeclarativeBase],
    selection: Select[tuple[DeclarativeBase]] | None = None,
    *,
    where: dict[str, Any] | None = None,
    order: list[dict[str, Any]] | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> Select[tuple[DeclarativeBase]]:
    if selection is None:
        selection = select(model)
    selection = filter_selection(model, selection, where=where)
    selection = order_selection(model, selection, order)

    if limit:
        selection = selection.limit(limit)

    if offset:
        selection = selection.offset(offset)

    return selection


def make_field_resolver(field_name: str) -> Callable[..., Any]:
    def field_resolver(root: DeclarativeBase, _info: ResolveInfo) -> Any:
        return getattr(root, field_name)

    return field_resolver


def pk_filter(instance: DeclarativeBase) -> Generator[ColumnExpressionArgument, None, None]:
    model = instance.__class__
    for column in model.__table__.primary_key:
        yield getattr(model, column.name) == getattr(instance, column.name)


def make_many_resolver(field_name: str) -> Callable[..., Sequence[DeclarativeBase]]:
    def resolver(
        root: DeclarativeBase,
        info: ResolveInfo,
        *,
        where: dict[str, Any] | None = None,
        order: list[dict[str, Any]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Sequence[DeclarativeBase]:
        if all(f is None for f in [where, order, limit, offset]):
            return getattr(root, field_name)
        session = info.context["session"]
        relationship: InstrumentedAttribute = getattr(root.__class__, field_name)
        field_model = relationship.prop.entity.class_
        selection = select(field_model).select_from(root.__class__).join(relationship).filter(*pk_filter(root))
        selection = resolve_filtered(field_model, selection, where=where, order=order, limit=limit, offset=offset)
        return session.execute(selection).scalars().all()

    return resolver


def make_object_resolver(model: type[DeclarativeBase]) -> Callable[..., Sequence[DeclarativeBase]]:
    def resolver(
        _root: None,
        info: ResolveInfo,
        *,
        where: dict[str, Any] | None = None,
        order: list[dict[str, Any]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Sequence[DeclarativeBase]:
        session = info.context["session"]
        selection = resolve_filtered(model, where=where, order=order, limit=limit, offset=offset)
        return session.execute(selection).scalars().all()

    return resolver


def make_pk_resolver(model: type[DeclarativeBase]) -> Callable[..., DeclarativeBase | None]:
    def resolver(_root: None, info: ResolveInfo, **kwargs: dict[str, Any]) -> DeclarativeBase:
        session = info.context["session"]
        return session.get_one(model, kwargs)

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
        _root: None, info: ResolveInfo, objects: list[dict[str, Any]], on_conflict: dict[str, Any] | None = None
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
        _root: None, info: ResolveInfo, object: dict[str, Any], on_conflict: dict[str, Any] | None = None
    ) -> DeclarativeBase:
        session = info.context["session"]

        instance = session_add_object(object, model, session, on_conflict)
        session_commit(session)
        return instance

    return resolver


def make_delete_resolver(model: type[DeclarativeBase]) -> Callable[..., InsDel]:
    def resolver(_root: None, info: ResolveInfo, where: dict[str, Any] | None = None) -> InsDel:
        session = info.context["session"]
        deletion = filter_selection(model, delete(model).returning(model), where=where)
        result = session.execute(deletion)
        rows = result.scalars().all()
        session_commit(session)
        # TODO: is len(rows) correct?
        return InsDel(affected_rows=len(rows), returning=rows)

    return resolver


def make_delete_by_pk_resolver(model: type[DeclarativeBase]) -> Callable[..., DeclarativeBase | None]:
    def resolver(_root: None, info: ResolveInfo, **kwargs: dict[str, Any]) -> DeclarativeBase | None:
        session = info.context["session"]

        row = session.get(model, kwargs)
        if row is None:
            return row

        session.delete(row)
        session_commit(session)
        return row

    return resolver


def update_selection(
    model: type[DeclarativeBase],
    selection: ReturningUpdate[tuple[DeclarativeBase]],
    _set: dict[str, Any] | None = None,
    _inc: dict[str, Any] | None = None,
) -> ReturningUpdate[tuple[DeclarativeBase]]:
    if _inc:
        to_increment = {}
        for column_name, increment in _inc.items():
            to_increment[column_name] = getattr(model, column_name) + increment

        selection = selection.values(**to_increment)

    if _set:
        selection = selection.values(**_set)

    return selection


def make_update_resolver(model: type[DeclarativeBase]) -> Callable[..., InsDel]:
    def resolver(
        _root: None,
        info: ResolveInfo,
        where: dict[str, Any],
        _set: dict[str, Any] | None,
        _inc: dict[str, Any] | None,
    ) -> InsDel:
        session = info.context["session"]
        selection = filter_selection(model, update(model).returning(model), where=where)
        selection = update_selection(model, selection, _set, _inc)
        result = session.execute(selection).scalars().all()
        session_commit(session)
        # TODO: is len(result) correct?
        return InsDel(affected_rows=len(result), returning=result)

    return resolver


def make_update_by_pk_resolver(model: type[DeclarativeBase]) -> Callable[..., DeclarativeBase | None]:
    def resolver(
        _root: None,
        info: ResolveInfo,
        _set: dict[str, Any] | None,
        _inc: dict[str, Any] | None,
        **pk_columns: dict[str, Any],
    ) -> DeclarativeBase | None:
        session = info.context["session"]
        selection = update(model).returning(model).filter_by(**pk_columns)
        selection = update_selection(model, selection, _set, _inc)
        result = session.execute(selection).scalar_one_or_none()
        if result is not None:
            session_commit(session)
        return result

    return resolver
