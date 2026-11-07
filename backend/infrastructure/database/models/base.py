import enum
import uuid
from datetime import datetime
from typing import Annotated

from sqlalchemy import BIGINT
from sqlalchemy.dialects.postgresql import UUID as PUUID
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql.functions import func

int_pk = Annotated[BIGINT, mapped_column(primary_key=True)]


class Base(DeclarativeBase):
    repr_cols_num = 6
    repr_cols = tuple()

    def __repr__(self):
        cols = []
        for idx, col in enumerate(self.__table__.columns.keys()):
            if col in self.repr_cols or idx < self.repr_cols_num:
                cols.append(f"{col}={getattr(self, col)}")

        return f"<{self.__class__.__name__} {', '.join(cols)}>"


class TableNameMixin:
    @declared_attr.directive
    def __tablename__(cls) -> str:  # noqa
        return cls.__name__.lower() + "s"


class UUIDMixin:
    id: Mapped[uuid.UUID] = mapped_column(PUUID(), primary_key=True, default=uuid.uuid4)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(default=func.now())
    updated_at: Mapped[datetime] = mapped_column(default=func.now(), onupdate=func.now())


class MarketType(str, enum.Enum):
    SPOT = "SPOT"
    FUTURES = "FUTURES"


class SourceType(str, enum.Enum):
    CEX = "CEX"  # Uppercase to match DB enum
    DEX = "DEX"


class MuteType(str, enum.Enum):
    PERMANENT = "PERMANENT"
    TEMPORARY = "TEMPORARY"


class MergeStatusType(str, enum.Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
