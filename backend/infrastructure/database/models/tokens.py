from datetime import datetime
from uuid import UUID

from sqlalchemy import String, Boolean, Enum, UniqueConstraint, ForeignKey, Float, Integer, Index, desc, DateTime
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin, TableNameMixin, UUIDMixin, MarketType, MuteType, MergeStatusType


class GlobalToken(Base, TimestampMixin, UUIDMixin):
    __tablename__ = "global_tokens"

    symbol: Mapped[str] = mapped_column(String(64), index=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, server_default="false")

    token_on_chains: Mapped[list["TokenOnChain"]] = relationship("TokenOnChain", back_populates="global_token")
    prices: Mapped[list["TokenPrice"]] = relationship(
        "TokenPrice",
        back_populates="global_token",
        foreign_keys="[TokenPrice.global_token_id]"
    )
    sources_assoc: Mapped[list["GlobalTokenSource"]] = relationship(
        "GlobalTokenSource",
        back_populates="global_token",
        cascade="all, delete-orphan"
    )
    sources: Mapped[list["Source"]] = relationship(
        "Source",
        secondary="global_token_sources",
        back_populates="global_tokens",
        viewonly=True
    )


class Token(Base, TableNameMixin, TimestampMixin, UUIDMixin):
    symbol: Mapped[str] = mapped_column(String(64), index=True)
    base_asset: Mapped[str] = mapped_column(String(64))
    quote_asset: Mapped[str] = mapped_column(String(64))
    contract_size: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_type: Mapped[MarketType] = mapped_column(Enum(MarketType, name="market_type_enum"))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    price: Mapped["TokenPrice"] = relationship(back_populates="token")
    token_on_chains: Mapped[list["TokenOnChain"]] = relationship(back_populates="token")
    source_id: Mapped[UUID] = mapped_column(ForeignKey("sources.id"), nullable=False)
    source: Mapped["Source"] = relationship("Source", back_populates="tokens")

    __table_args__ = (
        UniqueConstraint("symbol", "market_type", "source_id", name="uq_token_symbol_type"),
    )

    @property
    def name(self):
        return f"{self.base_asset.upper()}{self.quote_asset.upper()}"


class TokenOnChain(Base, TimestampMixin, UUIDMixin):
    __tablename__ = "token_chains"

    token_id: Mapped[str] = mapped_column(ForeignKey("tokens.id", ondelete="CASCADE"), index=True)
    chain_id: Mapped[str] = mapped_column(ForeignKey("chains.id", ondelete="CASCADE"), index=True)
    contract_address: Mapped[str] = mapped_column(String(128), nullable=False)

    deposit_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    min_deposit_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    deposit_confirm: Mapped[int | None] = mapped_column(Integer, nullable=True)
    deposit_tax: Mapped[float | None] = mapped_column(Float, nullable=True)

    withdraw_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    min_withdraw_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    withdrawal_fee: Mapped[float | None] = mapped_column(Float, nullable=True)
    withdraw_tax: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Timestamps when deposit/withdraw status last changed
    deposit_enabled_changed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    withdraw_enabled_changed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    token: Mapped["Token"] = relationship(back_populates="token_on_chains")
    chain: Mapped["Chain"] = relationship(back_populates="token_bindings")
    global_token_id: Mapped[UUID] = mapped_column(
        ForeignKey("global_tokens.id", ondelete="CASCADE"),
        index=True
    )
    global_token: Mapped["GlobalToken"] = relationship("GlobalToken", back_populates="token_on_chains")

    __table_args__ = (
        UniqueConstraint("token_id", "chain_id", name="uq_token_chain"),
        Index("idx_token_chains_gtid", "global_token_id"),
    )


class TokenPrice(Base, TimestampMixin, UUIDMixin):
    __tablename__ = "token_prices"

    price_usd: Mapped[float] = mapped_column(Float, nullable=True)
    orderbook: Mapped[dict] = mapped_column(JSONB, default=dict)
    market_type: Mapped[MarketType] = mapped_column(Enum(MarketType, name="market_type_enum"), server_default="SPOT")
    get_at: Mapped[datetime] = mapped_column(nullable=True)

    token_id: Mapped[UUID] = mapped_column(ForeignKey("tokens.id", ondelete="CASCADE"), index=True)
    global_token_id: Mapped[UUID] = mapped_column(ForeignKey("global_tokens.id", ondelete="CASCADE"), index=True)
    global_token_source_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("global_token_sources.id", ondelete="CASCADE"), nullable=True
    )

    token: Mapped["Token"] = relationship("Token", back_populates="price")
    global_token: Mapped["GlobalToken"] = relationship("GlobalToken", back_populates="prices")
    global_token_source: Mapped["GlobalTokenSource"] = relationship(
        "GlobalTokenSource",
        back_populates="prices",
        foreign_keys=[global_token_source_id]
    )

    __table_args__ = (
        UniqueConstraint("global_token_id", "global_token_source_id", "market_type", name="uq_token_price"),
        Index("idx_token_prices_gtid_upd", "global_token_id", desc("updated_at")),
        Index("idx_token_prices_gtid", "global_token_id"),
    )


class GlobalMergeCandidate(Base, TimestampMixin, UUIDMixin):
    __tablename__ = "global_merge_candidates"

    symbol: Mapped[str] = mapped_column(String(64), nullable=True)
    exchanges_a: Mapped[list[str] | None] = mapped_column(ARRAY(String), nullable=True)
    exchanges_b: Mapped[list[str] | None] = mapped_column(ARRAY(String), nullable=True)

    global_token_a_id: Mapped[UUID] = mapped_column(ForeignKey("global_tokens.id", ondelete="CASCADE"))
    global_token_b_id: Mapped[UUID] = mapped_column(ForeignKey("global_tokens.id", ondelete="CASCADE"))

    price_a: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_b: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_diff_pct: Mapped[float | None] = mapped_column(Float, nullable=True)

    network_overlap: Mapped[list[str] | None] = mapped_column(ARRAY(String), nullable=True)
    reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
    status: Mapped[str] = mapped_column(Enum(MergeStatusType, name="merge_status_type_enum"),
                                        server_default=MergeStatusType.PENDING.value)

    global_token_a: Mapped["GlobalToken"] = relationship(foreign_keys=[global_token_a_id])
    global_token_b: Mapped["GlobalToken"] = relationship(foreign_keys=[global_token_b_id])

    __table_args__ = (
        UniqueConstraint("global_token_a_id", "global_token_b_id", name="uq_merge_candidates_pair"),
    )


class TokenBlacklist(Base, TimestampMixin, UUIDMixin):
    __tablename__ = "token_blacklist"

    chain_name: Mapped[str | None] = mapped_column(String(32), nullable=True)
    contract_address: Mapped[str | None] = mapped_column(String(128), nullable=True)
    global_token_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("global_tokens.id", ondelete="CASCADE"),
        nullable=True,
        index=True
    )
    mute_type: Mapped[MuteType] = mapped_column(Enum(MuteType, name="mute_type_enum"), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    global_token: Mapped["GlobalToken | None"] = relationship("GlobalToken")


class DexPair(Base, TableNameMixin, UUIDMixin, TimestampMixin):
    __tablename__ = "dex_pairs"

    price_usd: Mapped[float | None] = mapped_column(Float, nullable=True)

    txns_m5: Mapped[int | None]
    txns_h1: Mapped[int | None]
    txns_h6: Mapped[int | None]
    txns_h24: Mapped[int | None]

    volume_m5: Mapped[float | None]
    volume_h1: Mapped[float | None]
    volume_h6: Mapped[float | None]
    volume_h24: Mapped[float | None]

    price_change_m5: Mapped[float | None]
    price_change_h1: Mapped[float | None]
    price_change_h6: Mapped[float | None]
    price_change_h24: Mapped[float | None]

    liquidity_usd: Mapped[float | None]
    liquidity_base: Mapped[float | None]
    liquidity_quote: Mapped[float | None]

    fdv: Mapped[float | None]
    market_cap: Mapped[float | None]
    pair_created_at: Mapped[datetime | None]
    chain_name: Mapped[str | None]
    base_address: Mapped[str | None]
    quote_address: Mapped[str | None]
    pair_address: Mapped[str] = mapped_column(String(256))
    url: Mapped[str | None]

    chain_id: Mapped[UUID] = mapped_column(ForeignKey("chains.id", ondelete="CASCADE"))
    source_id: Mapped[UUID] = mapped_column(ForeignKey("sources.id", ondelete="CASCADE"))
    token_base_id: Mapped[UUID] = mapped_column(ForeignKey("tokens.id", ondelete="CASCADE"))
    token_quote_id: Mapped[UUID] = mapped_column(ForeignKey("tokens.id", ondelete="CASCADE"))

    __table_args__ = (
        UniqueConstraint("chain_id", "pair_address", name="uq_dexpair_chain_pair"),
    )


class GlobalTokenPairBlacklist(Base, TimestampMixin, UUIDMixin):
    """
    Blacklist for preventing specific global_token pairs from merging.
    
    Use cases:
    - SPOT vs FUTURES with same symbol but different assets (e.g., FUN spot vs FUN futures)
    - Same symbol but different tokens on same exchange (e.g., XT KEKE futures vs spot)
    - Same symbol but different chains with no contract overlap
    
    Example:
        # Prevent merging XT KEKE futures with XT KEKE spot
        GlobalTokenPairBlacklist(
            global_token_a_id=<futures_gt_id>,
            global_token_b_id=<spot_gt_id>,
            reason="XT KEKE: FUTURES (contract_size=1000) vs SPOT - different assets"
        )
    """
    __tablename__ = "global_token_pair_blacklist"

    global_token_a_id: Mapped[UUID] = mapped_column(
        ForeignKey("global_tokens.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    global_token_b_id: Mapped[UUID] = mapped_column(
        ForeignKey("global_tokens.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    reason: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    
    global_token_a: Mapped["GlobalToken"] = relationship("GlobalToken", foreign_keys=[global_token_a_id])
    global_token_b: Mapped["GlobalToken"] = relationship("GlobalToken", foreign_keys=[global_token_b_id])
    
    __table_args__ = (
        UniqueConstraint("global_token_a_id", "global_token_b_id", name="uq_pair_blacklist"),
    )
