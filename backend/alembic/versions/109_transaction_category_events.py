"""who categorized each transaction, and when

One row per change to a transaction's category, with the actor behind it.
Ids of users, rules, agents and categories have no foreign key, so history
outlives deletions. Existing transactions start with no history.

Revision ID: 109
Revises: 108
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "109"
down_revision = "108"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "transaction_category_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("workspace_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("transaction_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("category_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("split_category_ids", sa.JSON(), nullable=True),
        sa.Column("actor", sa.String(length=16), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("rule_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.ForeignKeyConstraint(["workspace_id"], ["workspaces.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["transaction_id"], ["transactions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "actor IN ('user', 'rule', 'agent', 'mcp', 'system')",
            name="ck_transaction_category_event_actor",
        ),
        sa.CheckConstraint(
            "(actor = 'user' AND user_id IS NOT NULL AND rule_id IS NULL AND agent_id IS NULL)"
            " OR (actor = 'rule' AND rule_id IS NOT NULL AND agent_id IS NULL)"
            " OR (actor = 'agent' AND agent_id IS NOT NULL AND user_id IS NOT NULL AND rule_id IS NULL)"
            " OR (actor = 'mcp' AND user_id IS NOT NULL AND rule_id IS NULL AND agent_id IS NULL)"
            " OR (actor = 'system' AND rule_id IS NULL AND agent_id IS NULL)",
            name="ck_transaction_category_event_actor_ids",
        ),
        sa.CheckConstraint(
            "split_category_ids IS NULL OR category_id IS NULL",
            name="ck_transaction_category_event_split",
        ),
    )
    op.create_index(
        "ix_transaction_category_events_transaction",
        "transaction_category_events",
        ["transaction_id", "at"],
    )


def downgrade() -> None:
    op.drop_table("transaction_category_events")
