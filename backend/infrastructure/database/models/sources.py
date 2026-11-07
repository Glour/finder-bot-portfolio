from datetime import datetime
from uuid import UUID

from sqlalchemy import String, Enum, UniqueConstraint, ForeignKey, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin, TableNameMixin, UUIDMixin, SourceType, MarketType, MuteType


class GlobalTokenSource(Base, TimestampMixin, UUIDMixin):
    __tablename__ = "global_token_sources"

    global_token_id: Mapped[UUID] = mapped_column(ForeignKey("global_tokens.id", ondelete="CASCADE"), index=True)
    source_id: Mapped[UUID] = mapped_column(ForeignKey("sources.id", ondelete="CASCADE"), index=True)

    global_token: Mapped["GlobalToken"] = relationship("GlobalToken", back_populates="sources_assoc")
    source: Mapped["Source"] = relationship("Source", back_populates="global_tokens_assoc")
    prices: Mapped[list["TokenPrice"]] = relationship(
        "TokenPrice",
        back_populates="global_token_source"
    )

    __table_args__ = (
        UniqueConstraint("global_token_id", "source_id", name="uq_global_token_source"),
    )


class Source(Base, TableNameMixin, UUIDMixin, TimestampMixin):
    name: Mapped[str] = mapped_column(String(64), unique=True)
    type: Mapped[SourceType] = mapped_column(Enum(SourceType, name="source_type_enum"))

    tokens: Mapped[list["Token"]] = relationship(back_populates="source")
    global_tokens_assoc: Mapped[list["GlobalTokenSource"]] = relationship("GlobalTokenSource", back_populates="source")
    global_tokens: Mapped[list["GlobalToken"]] = relationship(
        "GlobalToken",
        secondary="global_token_sources",
        back_populates="sources",
        viewonly=True
    )

    __table_args__ = (
        UniqueConstraint("name", "type", name="uq_source_name_type"),
    )


class ExchangeBlacklist(Base, TimestampMixin, UUIDMixin):
    __tablename__ = "exchange_blacklist"

    source_id: Mapped[UUID] = mapped_column(
        ForeignKey("sources.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    market_type: Mapped[MarketType | None] = mapped_column(
        Enum(MarketType, name="market_type_enum"),
        nullable=True  # NULL = all markets
    )
    mute_type: Mapped[MuteType] = mapped_column(
        Enum(MuteType, name="mute_type_enum"),
        nullable=False
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True
    )

    source: Mapped["Source"] = relationship("Source")

    __table_args__ = (
        UniqueConstraint("source_id", "market_type", name="uq_exchange_blacklist"),
    )
