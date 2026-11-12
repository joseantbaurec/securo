"""The category history writes one event per real change, with its actor."""
import uuid
from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

import mcp_server.tools  # noqa: F401 -- registers the MCP handlers
from app.core.category_history import Actor, acting_as
from app.models.transaction import Transaction
from app.models.transaction_category_event import TransactionCategoryEvent
from app.schemas.transaction_category_line import (
    TransactionCategoryLineInput,
    TransactionCategoryLinesInput,
)
from app.services import category_line_service, rule_service
from mcp_server.auth import CallContext
from mcp_server.registry import call_tool

pytestmark = pytest.mark.asyncio


async def _events(session, tx_id) -> list[TransactionCategoryEvent]:
    rows = await session.execute(
        select(TransactionCategoryEvent)
        .where(TransactionCategoryEvent.transaction_id == tx_id)
        .order_by(TransactionCategoryEvent.at)
    )
    return list(rows.scalars().all())


def _split(*parts) -> TransactionCategoryLinesInput:
    return TransactionCategoryLinesInput(
        share_type="exact",
        lines=[TransactionCategoryLineInput(category_id=c.id, amount=Decimal(a)) for c, a in parts],
    )


# --- What gets written --------------------------------------------------------


async def test_a_change_is_recorded_with_its_actor(session, test_user, test_transactions, test_categories):
    tx = test_transactions[3]  # uncategorized

    with acting_as(Actor("user", user_id=test_user.id)):
        tx.category_id = test_categories[0].id
    await session.commit()

    (event,) = await _events(session, tx.id)
    assert event.category_id == test_categories[0].id
    assert event.split_category_ids is None
    assert event.actor == "user"
    assert event.user_id == test_user.id
    assert event.workspace_id == tx.workspace_id


async def test_setting_the_same_category_leaves_no_trace(session, test_transactions):
    tx = test_transactions[0]
    before = await _events(session, tx.id)

    tx.category_id = tx.category_id
    await session.commit()

    assert await _events(session, tx.id) == before


async def test_a_new_transaction_is_recorded_only_with_a_category(session, test_user, test_account, test_categories):
    def new(category_id) -> Transaction:
        return Transaction(
            user_id=test_user.id, account_id=test_account.id, category_id=category_id,
            description="MERCADONA", amount=Decimal("12.30"), date=date.today(),
            type="debit", source="manual",
        )

    categorized, uncategorized = new(test_categories[0].id), new(None)
    session.add_all([categorized, uncategorized])
    await session.commit()

    (event,) = await _events(session, categorized.id)
    assert event.category_id == test_categories[0].id
    assert await _events(session, uncategorized.id) == []


async def test_without_a_context_the_system_did_it(session, test_transactions, test_categories):
    tx = test_transactions[3]

    tx.category_id = test_categories[2].id
    await session.commit()

    (event,) = await _events(session, tx.id)
    assert event.actor == "system"
    assert event.user_id is None


async def test_the_actor_is_read_when_the_category_is_set(session, test_user, test_transactions, test_categories):
    tx = test_transactions[3]

    with acting_as(Actor("user", user_id=test_user.id)):
        tx.category_id = test_categories[0].id
    # Committed outside the context: the change still belongs to the user.
    await session.commit()

    (event,) = await _events(session, tx.id)
    assert event.actor == "user"


# --- Splits -------------------------------------------------------------------


async def test_a_split_is_one_event_with_its_categories_in_order(session, test_transactions, test_categories):
    food, transport, _ = test_categories
    tx = test_transactions[1]  # 45.00, in food
    before = await _events(session, tx.id)

    await category_line_service.replace_lines(session, tx, _split((transport, "15"), (food, "30")))
    await session.commit()

    events = await _events(session, tx.id)
    # Several flushes on the way, one event in the end.
    assert len(events) == len(before) + 1
    assert events[-1].category_id is None
    assert events[-1].split_category_ids == [str(transport.id), str(food.id)]


async def test_reweighting_a_split_is_not_a_category_change(session, test_transactions, test_categories):
    food, transport, _ = test_categories
    tx = test_transactions[1]  # 45.00
    await category_line_service.replace_lines(session, tx, _split((food, "30"), (transport, "15")))
    await session.commit()
    before = await _events(session, tx.id)

    await category_line_service.replace_lines(session, tx, _split((food, "20"), (transport, "25")))
    await session.commit()

    assert await _events(session, tx.id) == before


# --- Rules --------------------------------------------------------------------


async def test_a_rule_inside_a_users_request_is_credited_to_the_rule(
    session, test_user, test_account, test_categories, test_rules
):
    uber_rule = test_rules[0]
    tx = Transaction(
        user_id=test_user.id, account_id=test_account.id, description="UBER TRIP",
        amount=Decimal("9.50"), date=date.today(), type="debit", source="manual",
    )
    session.add(tx)
    await session.flush()

    with acting_as(Actor("user", user_id=test_user.id)):
        await rule_service.apply_rules_to_transaction(session, test_user.id, tx)
    await session.commit()

    (event,) = await _events(session, tx.id)
    assert event.actor == "rule"
    assert event.rule_id == uber_rule.id
    assert event.category_id == test_categories[1].id


async def test_a_rule_preview_writes_nothing(session, test_user, test_transactions, test_rules):
    tx = test_transactions[0]
    before = await _events(session, tx.id)

    await rule_service.preview_rules_for_transaction(session, test_user.id, tx)
    await session.commit()

    assert await _events(session, tx.id) == before


# --- Entry points -------------------------------------------------------------


async def test_an_api_edit_is_credited_to_the_user(client, auth_headers, session, test_user, test_transactions, test_categories):
    tx = test_transactions[3]

    response = await client.patch(
        f"/api/transactions/{tx.id}", headers=auth_headers,
        json={"category_id": str(test_categories[0].id)},
    )

    assert response.status_code == 200, response.text
    (event,) = await _events(session, tx.id)
    assert event.actor == "user"
    assert event.user_id == test_user.id


async def test_bulk_categorize_records_only_rows_that_changed(client, auth_headers, session, test_transactions, test_categories):
    already, uncategorized = test_transactions[0], test_transactions[3]
    target = test_categories[1]  # `already` is in it
    before = await _events(session, already.id)

    response = await client.patch(
        "/api/transactions/bulk-categorize", headers=auth_headers,
        json={"transaction_ids": [str(already.id), str(uncategorized.id)], "category_id": str(target.id)},
    )

    assert response.status_code == 200, response.text
    assert await _events(session, already.id) == before
    (event,) = await _events(session, uncategorized.id)
    assert event.actor == "user"
    assert event.category_id == target.id


async def test_an_external_mcp_client_is_recorded_as_mcp(session, test_user, test_transactions, test_categories):
    tx = test_transactions[3]

    await call_tool(
        session,
        CallContext(user_id=test_user.id, external=True),
        "propose_categorize",
        {"transaction_ids": [str(tx.id)], "category_id": str(test_categories[0].id), "apply": True},
    )

    (event,) = await _events(session, tx.id)
    assert event.actor == "mcp"
    assert event.user_id == test_user.id


# --- Constraints --------------------------------------------------------------


@pytest.mark.parametrize(
    "actor",
    [
        Actor("agent", agent_id=uuid.uuid4()),  # nobody approved it
        Actor("rule", rule_id=uuid.uuid4(), agent_id=uuid.uuid4()),
        Actor("user"),
    ],
)
async def test_an_inconsistent_actor_is_refused(session, test_transactions, test_categories, actor):
    tx = test_transactions[3]

    with acting_as(actor):
        tx.category_id = test_categories[2].id
    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()
