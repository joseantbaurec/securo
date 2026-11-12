"""Category history: one event per real change to a transaction's category.

Recorded on its own at commit, comparing each touched transaction with how it
was when first touched. The actor is read from `acting_as` when the category
is set, so a rule running inside someone's request is credited to the rule.
Bulk UPDATEs bypass the ORM and call `record_bulk` themselves.
"""

import uuid
from collections.abc import Generator, Iterable, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import event, inspect, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, SessionTransaction, UOWTransaction

from app.models.transaction import Transaction
from app.models.transaction_category_event import TransactionCategoryEvent
from app.models.transaction_category_line import TransactionCategoryLine


@dataclass(frozen=True)
class Actor:
    kind: str
    user_id: Optional[uuid.UUID] = None
    rule_id: Optional[uuid.UUID] = None
    agent_id: Optional[uuid.UUID] = None


SYSTEM = Actor("system")

_current: ContextVar[Optional[Actor]] = ContextVar("category_actor", default=None)


@contextmanager
def acting_as(actor: Actor) -> Generator[None]:
    """Credit the category changes made inside the block to `actor`."""
    token = _current.set(actor)
    try:
        yield
    finally:
        _current.reset(token)


def current_actor() -> Actor:
    """The actor of the running operation, or the system when none was set."""
    return _current.get() or SYSTEM


def record_bulk(
    session: AsyncSession,
    workspace_id: uuid.UUID,
    transaction_ids: Iterable[uuid.UUID],
    category_id: Optional[uuid.UUID],
) -> None:
    """Record one event per row a bulk UPDATE changed. Does not commit."""
    actor = current_actor()
    session.add_all(
        _event(workspace_id, transaction_id, (category_id, None), actor)
        for transaction_id in transaction_ids
    )


# A category, or the ordered line categories while split.
_State = tuple[Optional[uuid.UUID], Optional[tuple[uuid.UUID, ...]]]
_UNCATEGORIZED: _State = (None, None)
_TRACKED = "category_history"
_ACTOR = "category_actor"


@dataclass
class _Tracked:
    before: _State
    actor: Actor


def _event(
    workspace_id: uuid.UUID, transaction_id: uuid.UUID, state: _State, actor: Actor
) -> TransactionCategoryEvent:
    category_id, split = state
    return TransactionCategoryEvent(
        workspace_id=workspace_id,
        transaction_id=transaction_id,
        category_id=category_id,
        split_category_ids=[str(c) for c in split] if split is not None else None,
        actor=actor.kind,
        user_id=actor.user_id,
        rule_id=actor.rule_id,
        agent_id=actor.agent_id,
    )


def _load_states(session: Session, ids: list[uuid.UUID]) -> dict[uuid.UUID, tuple[uuid.UUID, _State]]:
    """Read each transaction's workspace and category state from the database.

    A split transaction's state is its line categories in order; otherwise
    it is its category. Missing ids are left out.
    """
    states: dict[uuid.UUID, tuple[uuid.UUID, _State]] = {}
    splits: dict[uuid.UUID, list[uuid.UUID]] = {}
    # Called from flush and commit hooks, so reading must not flush again.
    with session.no_autoflush:
        for tx_id, category_id in session.execute(
            select(TransactionCategoryLine.transaction_id, TransactionCategoryLine.category_id)
            .where(TransactionCategoryLine.transaction_id.in_(ids))
            .order_by(TransactionCategoryLine.transaction_id, TransactionCategoryLine.position)
        ):
            splits.setdefault(tx_id, []).append(category_id)
        for tx_id, workspace_id, category_id, is_split in session.execute(
            select(
                Transaction.id,
                Transaction.workspace_id,
                Transaction.category_id,
                Transaction.has_category_lines,
            ).where(Transaction.id.in_(ids))
        ):
            state = (None, tuple(splits.get(tx_id, ()))) if is_split else (category_id, None)
            states[tx_id] = (workspace_id, state)
    return states


def _remember_actor(target: Transaction | TransactionCategoryLine, *args: object) -> None:
    """Pin the current actor to the object whose category was just set."""
    inspect(target).info[_ACTOR] = current_actor()


def _changed(obj: Transaction | TransactionCategoryLine, *fields: str) -> bool:
    attrs = inspect(obj).attrs
    return any(attrs[field].history.has_changes() for field in fields)


def _transaction_of(line: TransactionCategoryLine) -> Optional[Transaction]:
    # Read without loading: a pending line only has its parent in memory.
    parent = inspect(line).attrs.transaction.loaded_value
    return parent if isinstance(parent, Transaction) else None


def _before_flush(
    session: Session, flush_context: UOWTransaction, instances: Optional[Sequence[object]]
) -> None:
    """Note which transactions this flush changes, their state before it, and who changed them."""
    touched: dict[uuid.UUID, Optional[Actor]] = {}
    new_ids: set[uuid.UUID] = set()

    def touch(tx_id: uuid.UUID, obj: Transaction | TransactionCategoryLine) -> None:
        actor = inspect(obj).info.pop(_ACTOR, None)
        if actor is not None or tx_id not in touched:
            touched[tx_id] = actor

    for obj in (*session.new, *session.dirty, *session.deleted):
        if isinstance(obj, Transaction):
            if obj in session.deleted:
                continue
            is_new = obj in session.new
            if not is_new and not _changed(obj, "category_id", "has_category_lines"):
                continue
            # The id default only applies on insert, and it keys the tracking.
            if obj.id is None:
                obj.id = uuid.uuid4()
            if is_new:
                new_ids.add(obj.id)
            touch(obj.id, obj)
        elif isinstance(obj, TransactionCategoryLine):
            if obj in session.dirty and not _changed(obj, "category_id"):
                continue
            tx_id = obj.transaction_id
            if tx_id is None and (parent := _transaction_of(obj)) is not None:
                if parent.id is None:
                    parent.id = uuid.uuid4()
                tx_id = parent.id
            if tx_id is not None:
                touch(tx_id, obj)

    if not touched:
        return
    tracked: dict[uuid.UUID, _Tracked] = session.info.setdefault(_TRACKED, {})
    unseen = [tx_id for tx_id in touched if tx_id not in tracked and tx_id not in new_ids]
    # Read before this flush writes: the database still holds the original state.
    before = _load_states(session, unseen) if unseen else {}
    for tx_id, actor in touched.items():
        if tx_id in tracked:
            if actor is not None:
                tracked[tx_id].actor = actor
            continue
        state = before[tx_id][1] if tx_id in before else _UNCATEGORIZED
        tracked[tx_id] = _Tracked(before=state, actor=actor or current_actor())


def _before_commit(session: Session) -> None:
    """Add an event for each tracked transaction whose state differs from before."""
    # Pending changes are only tracked once flushed.
    session.flush()
    tracked: dict[uuid.UUID, _Tracked] = session.info.pop(_TRACKED, {})
    if not tracked:
        return
    after = _load_states(session, list(tracked))
    for tx_id, entry in tracked.items():
        if tx_id not in after:
            continue
        workspace_id, state = after[tx_id]
        if state != entry.before:
            session.add(_event(workspace_id, tx_id, state, entry.actor))


def _after_transaction_end(session: Session, transaction: SessionTransaction) -> None:
    """Drop the tracking on rollback, so it never leaks into the next transaction."""
    if transaction.parent is None:
        session.info.pop(_TRACKED, None)


event.listen(Transaction.category_id, "set", _remember_actor)
event.listen(Transaction.has_category_lines, "set", _remember_actor)
event.listen(Transaction.category_lines, "append", _remember_actor)
event.listen(Transaction.category_lines, "remove", _remember_actor)
event.listen(TransactionCategoryLine.category_id, "set", _remember_actor)
event.listen(Session, "before_flush", _before_flush)
event.listen(Session, "before_commit", _before_commit)
event.listen(Session, "after_transaction_end", _after_transaction_end)
