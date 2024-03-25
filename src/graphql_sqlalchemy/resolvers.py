from __future__ import annotations

from asyncio import gather
from collections.abc import Sequence
from functools import partial
from itertools import starmap
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Callable, TypedDict, TypeVar, cast, overload

from sqlalchemy import ColumnExpressionArgument, ScalarResult, Select, and_, delete, not_, or_, select, true, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute, Session, interfaces
from sqlalchemy.sql.dml import ReturningDelete, ReturningUpdate

from .helpers import get_mapper

if TYPE_CHECKING:
    from collections.abc import Awaitable, Generator, Mapping

    from graphql.pyutils import AwaitableOrValue
    from sqlalchemy.orm._typing import OrmExecuteOptionsParameter

    from .types import ResolveInfo


class InsDel(TypedDict):
    affected_rows: int
    returning: Sequence[DeclarativeBase]


T = TypeVar("T")


W = TypeVar(
    "W",
    Select[tuple[DeclarativeBase]],
    ReturningUpdate[tuple[DeclarativeBase]],
    ReturningDelete[tuple[DeclarativeBase]],
)


# Async helpers


@overload
def all_scalars(
    session: Session,
    selection: W,
    *,
    execution_options: OrmExecuteOptionsParameter = MappingProxyType({}),
) -> Sequence[DeclarativeBase]: ...


@overload
def all_scalars(
    session: AsyncSession,
    selection: W,
    *,
    execution_options: OrmExecuteOptionsParameter = MappingProxyType({}),
) -> Awaitable[Sequence[DeclarativeBase]]: ...


def all_scalars(
    session: Session | AsyncSession,
    selection: W,
    *,
    execution_options: OrmExecuteOptionsParameter = MappingProxyType({}),
) -> AwaitableOrValue[Sequence[DeclarativeBase]]:
    result = session.scalars(selection, execution_options=execution_options)
    if isinstance(result, ScalarResult):
        return result.all()

    async def get_all() -> Sequence[DeclarativeBase]:
        return (await result).all()

    return get_all()


# Start


def get_bool_operation(
    model_property: InstrumentedAttribute[Any], operator: str, value: Any
) -> ColumnExpressionArgument[bool]:
    if operator == "_eq":
        return model_property == value  # type: ignore[no-any-return]

    if operator == "_in":
        return model_property.in_(value)

    if operator == "_is_null":
        return model_property.is_(None)

    if operator == "_like":
        return model_property.like(value)

    if operator == "_neq":
        return model_property != value  # type: ignore[no-any-return]

    if operator == "_nin":
        return model_property.notin_(value)

    if operator == "_nlike":
        return model_property.notlike(value)

    if operator == "_lt":
        return model_property < value  # type: ignore[no-any-return]

    if operator == "_gt":
        return model_property > value  # type: ignore[no-any-return]

    if operator == "_lte":
        return model_property <= value  # type: ignore[no-any-return]

    if operator == "_gte":
        return model_property >= value  # type: ignore[no-any-return]

    raise Exception("Invalid operator")


def get_filter_operation(model: type[DeclarativeBase], where: Mapping[str, Any]) -> ColumnExpressionArgument[bool]:
    partial_filter = partial(get_filter_operation, model)

    if len(where) == 0:
        return true()

    if len(where) > 1:
        return and_(*(partial_filter({name: expr}) for name, expr in where.items()))

    # Single filter
    [(name, exprs)] = where.items()
    if name == "_or":
        return or_(*map(partial_filter, exprs))

    if name == "_not":
        return not_(partial_filter(exprs))

    if name == "_and":
        return and_(*map(partial_filter, exprs))

    model_property: InstrumentedAttribute[Any] = getattr(model, name)

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


def filter_selection(
    model: type[DeclarativeBase],
    selection: W,
    *,
    where: Mapping[str, Any] = MappingProxyType({}),
) -> W:
    return selection.where(get_filter_operation(model, where))


def order_selection(
    model: type[DeclarativeBase] | InstrumentedAttribute[Any],
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
    where: Mapping[str, Any] = MappingProxyType({}),
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


def make_field_resolver(field_name: str) -> Callable[..., AwaitableOrValue[Any]]:
    def field_resolver(root: DeclarativeBase, info: ResolveInfo) -> AwaitableOrValue[Any]:
        session = info.context["session"]

        if isinstance(session, Session):
            return getattr(root, field_name)

        async def get_field() -> Any:
            await session.refresh(root, attribute_names=[field_name])
            return getattr(root, field_name)

        return get_field()

    return field_resolver


def pk_filter(instance: DeclarativeBase) -> Generator[ColumnExpressionArgument[Any], None, None]:
    model = instance.__class__
    for column in model.__table__.primary_key:
        yield getattr(model, column.name) == getattr(instance, column.name)


def make_many_resolver(
    field_name: str,
) -> Callable[..., AwaitableOrValue[Sequence[DeclarativeBase]]]:
    def resolver(
        root: DeclarativeBase,
        info: ResolveInfo,
        *,
        where: dict[str, Any] | None = None,
        order: list[dict[str, Any]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> AwaitableOrValue[Sequence[DeclarativeBase]]:
        if all(f is None for f in [where, order, limit, offset]):
            return cast(Sequence[Any], getattr(root, field_name))
        session = info.context["session"]
        relationship: InstrumentedAttribute[Any] = getattr(root.__class__, field_name)
        field_model = relationship.prop.entity.class_
        selection = select(field_model).select_from(root.__class__).join(relationship).filter(*pk_filter(root))
        selection = resolve_filtered(field_model, selection, where=where or {}, order=order, limit=limit, offset=offset)
        return all_scalars(session, selection)

    return resolver


def make_object_resolver(model: type[DeclarativeBase]) -> Callable[..., AwaitableOrValue[Sequence[DeclarativeBase]]]:
    def resolver(
        _root: None,
        info: ResolveInfo,
        *,
        where: Mapping[str, Any] = MappingProxyType({}),
        order: list[dict[str, Any]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> AwaitableOrValue[Sequence[DeclarativeBase]]:
        session = info.context["session"]
        selection = resolve_filtered(model, where=where, order=order, limit=limit, offset=offset)
        return all_scalars(session, selection)

    return resolver


def make_pk_resolver(model: type[DeclarativeBase]) -> Callable[..., AwaitableOrValue[DeclarativeBase | None]]:
    def resolver(_root: None, info: ResolveInfo, **kwargs: dict[str, Any]) -> AwaitableOrValue[DeclarativeBase | None]:
        session = info.context["session"]
        return session.get_one(model, kwargs)

    return resolver


@overload
def session_add_object(
    obj: dict[str, Any], model: type[DeclarativeBase], session: Session, *, on_conflict: dict[str, Any] | None
) -> DeclarativeBase: ...


@overload
def session_add_object(
    obj: dict[str, Any], model: type[DeclarativeBase], session: AsyncSession, *, on_conflict: dict[str, Any] | None
) -> Awaitable[DeclarativeBase]: ...


def session_add_object(
    obj: dict[str, Any],
    model: type[DeclarativeBase],
    session: Session | AsyncSession,
    *,
    on_conflict: dict[str, Any] | None,
) -> AwaitableOrValue[DeclarativeBase]:
    merge = bool(on_conflict and on_conflict["merge"])
    instance = model()
    for key, value in obj.items():
        setattr(instance, key, value)

    if isinstance(session, Session):
        if merge:
            session.merge(instance)
        else:
            session.add(instance)
        return instance

    return _session_add_object_async(session, instance, merge=merge)


async def _session_add_object_async(session: AsyncSession, instance: DeclarativeBase, merge: bool) -> DeclarativeBase:
    if merge:
        await session.merge(instance)
    else:
        session.add(instance)
    return instance


def make_insert_resolver(model: type[DeclarativeBase]) -> Callable[..., AwaitableOrValue[InsDel]]:
    def resolver(
        _root: None,
        info: ResolveInfo,
        *,
        objects: list[dict[str, Any]],
        on_conflict: dict[str, Any] | None = None,
    ) -> AwaitableOrValue[InsDel]:
        session = info.context["session"]

        if isinstance(session, Session):
            with session.no_autoflush:
                models = [session_add_object(obj, model, session, on_conflict=on_conflict) for obj in objects]

            rv = InsDel(affected_rows=len(models), returning=models)
            session.flush()
            return rv

        async def insert_many() -> InsDel:
            assert isinstance(session, AsyncSession)
            with session.no_autoflush:
                models = await gather(
                    *[session_add_object(obj, model, session, on_conflict=on_conflict) for obj in objects]
                )

            rv = InsDel(affected_rows=len(models), returning=models)
            await session.flush()
            return rv

        return insert_many()

    return resolver


def make_insert_one_resolver(model: type[DeclarativeBase]) -> Callable[..., AwaitableOrValue[DeclarativeBase]]:
    def resolver(
        _root: None,
        info: ResolveInfo,
        *,
        object: dict[str, Any],
        on_conflict: dict[str, Any] | None = None,
    ) -> AwaitableOrValue[DeclarativeBase]:
        session = info.context["session"]

        if isinstance(session, Session):
            instance = session_add_object(object, model, session, on_conflict=on_conflict)
            session.flush()
            return instance

        async def insert_one() -> DeclarativeBase:
            assert isinstance(session, AsyncSession)
            instance = await session_add_object(object, model, session, on_conflict=on_conflict)
            await session.flush()
            return instance

        return insert_one()

    return resolver


def make_delete_resolver(model: type[DeclarativeBase]) -> Callable[..., AwaitableOrValue[InsDel]]:
    def resolver(
        _root: None,
        info: ResolveInfo,
        *,
        where: Mapping[str, Any] = MappingProxyType({}),
    ) -> AwaitableOrValue[InsDel]:
        session = info.context["session"]
        deletion = filter_selection(model, delete(model).returning(model), where=where)

        if isinstance(session, Session):
            objs = all_scalars(session, deletion, execution_options=dict(is_delete_using=True))
            rv = InsDel(affected_rows=len(objs), returning=objs)
            session.flush()
            return rv

        async def delete_many() -> InsDel:
            assert isinstance(session, AsyncSession)
            objs = await all_scalars(session, deletion, execution_options=dict(is_delete_using=True))
            rv = InsDel(affected_rows=len(objs), returning=objs)
            await session.flush()
            return rv

        return delete_many()

    return resolver


def make_delete_by_pk_resolver(model: type[DeclarativeBase]) -> Callable[..., AwaitableOrValue[DeclarativeBase | None]]:
    def resolver(_root: None, info: ResolveInfo, **kwargs: dict[str, Any]) -> AwaitableOrValue[DeclarativeBase | None]:
        session = info.context["session"]

        if isinstance(session, Session):
            row = session.get(model, kwargs)
            if row is None:
                return row

            session.delete(row)
            session.flush()
            return row

        async def delete_row() -> DeclarativeBase | None:
            row = await session.get(model, kwargs)
            if row is None:
                return row
            await session.delete(row)
            await session.flush()
            return row

        return delete_row()

    return resolver


def update_selection(
    model: type[DeclarativeBase],
    selection: ReturningUpdate[tuple[DeclarativeBase]],
    *,
    _set: dict[str, Any] | None,
    _inc: dict[str, Any] | None,
) -> ReturningUpdate[tuple[DeclarativeBase]]:
    if _inc:
        to_increment = {}
        for column_name, increment in _inc.items():
            to_increment[column_name] = getattr(model, column_name) + increment

        selection = selection.values(**to_increment)

    if _set:
        selection = selection.values(**_set)

    return selection


def make_update_resolver(model: type[DeclarativeBase]) -> Callable[..., AwaitableOrValue[InsDel]]:
    def resolver(
        _root: None,
        info: ResolveInfo,
        *,
        where: dict[str, Any],
        _set: dict[str, Any] | None = None,
        _inc: dict[str, Any] | None = None,
    ) -> AwaitableOrValue[InsDel]:
        session = info.context["session"]
        selection = filter_selection(model, update(model).returning(model), where=where)
        selection = update_selection(model, selection, _set=_set, _inc=_inc)
        if isinstance(session, Session):
            result = all_scalars(session, selection)
            session.flush()
            return InsDel(affected_rows=len(result), returning=result)

        async def update_many() -> InsDel:
            result = await all_scalars(session, selection)
            await session.flush()
            return InsDel(affected_rows=len(result), returning=result)

        return update_many()

    return resolver


def make_update_by_pk_resolver(model: type[DeclarativeBase]) -> Callable[..., AwaitableOrValue[DeclarativeBase | None]]:
    def resolver(
        _root: None,
        info: ResolveInfo,
        *,
        _set: dict[str, Any] | None = None,
        _inc: dict[str, Any] | None = None,
        **pk_columns: dict[str, Any],
    ) -> AwaitableOrValue[DeclarativeBase | None]:
        session = info.context["session"]
        selection = update(model).returning(model).filter_by(**pk_columns)
        selection = update_selection(model, selection, _set=_set, _inc=_inc)
        if isinstance(session, Session):
            result = session.execute(selection).scalar_one_or_none()
            if result is None:
                return None
            session.flush()
            return result

        async def update_one() -> DeclarativeBase | None:
            assert isinstance(session, AsyncSession)
            result = (await session.execute(selection)).scalar_one_or_none()
            if result is None:
                return None
            await session.flush()
            return result

        return update_one()

    return resolver
