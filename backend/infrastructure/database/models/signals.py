from datetime import datetime

from sqlalchemy import BIGINT, Index, String, Float, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB, UUID as PUUID
from sqlalchemy.orm import Mapped, mapped_column

from infrastructure.database.models.base import Base


class SignalHistory(Base):
    __tablename__ = "signal_history"

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)

    # Core signal data
    global_token_id: Mapped[str] = mapped_column(PUUID(), nullable=False)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    buy_from: Mapped[str] = mapped_column(String(100), nullable=False)
    sell_to: Mapped[str] = mapped_column(String(100), nullable=False)
    buy_base_asset: Mapped[str | None] = mapped_column(String(50), nullable=True)
    sell_base_asset: Mapped[str | None] = mapped_column(String(50), nullable=True)
    telegram_link: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Prices & profit
    buy_price: Mapped[float] = mapped_column(Float, nullable=False)
    sell_price: Mapped[float] = mapped_column(Float, nullable=False)
    spread_percent: Mapped[float] = mapped_column(Float, nullable=False)
    profit: Mapped[float] = mapped_column(Float, nullable=False)
    profit_before_fee: Mapped[float] = mapped_column(Float, nullable=True)
    applied_fee: Mapped[float | None] = mapped_column(Float, default=0, nullable=True)
    volume: Mapped[float] = mapped_column(Float, nullable=True)

    # Signal metadata
    level: Mapped[str] = mapped_column(String(20), nullable=False)

    # Timestamps
    analyzed_at: Mapped[int] = mapped_column(BIGINT, nullable=False)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now(), nullable=True)

    # Extra details (chains, sources, timestamps)
    details: Mapped[dict] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        Index("ix_signal_history_analyzed_at", "analyzed_at", postgresql_using="btree"),
        Index("ix_signal_history_symbol", "symbol"),
        Index("ix_signal_history_level", "level"),
        Index("ix_signal_history_created_at", "created_at"),
    )
