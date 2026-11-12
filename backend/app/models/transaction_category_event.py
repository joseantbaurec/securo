import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import JSON, CheckConstraint, DateTime, ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class TransactionCategoryEvent(Base):
    """A change to a transaction's category, and who made it.

    Each row holds the resulting state; the previous state is the row before.
    Ids of users, rules, agents and categories have no foreign key, so history
    outlives deletions.
    """

    __tablename__ = "transaction_category_events"
    __table_args__ = (
        CheckConstraint(
            "actor IN ('user', 'rule', 'agent', 'mcp', 'system')",
            name="ck_transaction_category_event_actor",
        ),
        CheckConstraint(
            "(actor = 'user' AND user_id IS NOT NULL AND rule_id IS NULL AND agent_id IS NULL)"
            " OR (actor = 'rule' AND rule_id IS NOT NULL AND agent_id IS NULL)"
            " OR (actor = 'agent' AND agent_id IS NOT NULL AND user_id IS NOT NULL AND rule_id IS NULL)"
            " OR (actor = 'mcp' AND user_id IS NOT NULL AND rule_id IS NULL AND agent_id IS NULL)"
            " OR (actor = 'system' AND rule_id IS NULL AND agent_id IS NULL)",
            name="ck_transaction_category_event_actor_ids",
        ),
        CheckConstraint(
            "split_category_ids IS NULL OR category_id IS NULL",
            name="ck_transaction_category_event_split",
        ),
        Index("ix_transaction_category_events_transaction", "transaction_id", "at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workspaces.id", ondelete="CASCADE")
    )
    transaction_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("transactions.id", ondelete="CASCADE")
    )
    # Set in Python: now() is frozen per database transaction, so events could tie.
    at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    category_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    # Line categories in order while split. none_as_null stores SQL NULL, not JSON null.
    split_category_ids: Mapped[Optional[list[str]]] = mapped_column(
        JSON(none_as_null=True), nullable=True
    )
    actor: Mapped[str] = mapped_column(String(16))
    user_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    rule_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    agent_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
