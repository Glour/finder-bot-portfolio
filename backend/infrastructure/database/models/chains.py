from sqlalchemy import String, Float
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin, TableNameMixin, UUIDMixin


class Chain(Base, TableNameMixin, TimestampMixin, UUIDMixin):
    name: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    block_time: Mapped[float | None] = mapped_column(Float, nullable=True)

    token_bindings: Mapped[list["TokenOnChain"]] = relationship(
        back_populates="chain",
        cascade="all, delete-orphan"
    )
