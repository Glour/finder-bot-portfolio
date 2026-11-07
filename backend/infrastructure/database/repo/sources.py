from datetime import datetime, UTC
from uuid import UUID

from sqlalchemy import select, tuple_, delete, or_, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker

from infrastructure.database.models import Source
from infrastructure.database.models.base import SourceType, MarketType, MuteType
from infrastructure.database.models.sources import GlobalTokenSource, ExchangeBlacklist
from infrastructure.database.repo.base import BaseRepo


class SourceRepo(BaseRepo):
    def __init__(self, session: async_sessionmaker):
        super().__init__(session, Source)  # noqa

    async def get_or_create(self, name: str, source_type: SourceType) -> Source:
        async with self.sessionmaker() as session:
            insert_stmt = (
                insert(self.model)
                .values(name=name, type=source_type)
                .on_conflict_do_nothing(
                    index_elements=["name", "type"]
                )
                .returning(self.model)
            )

            result = await session.execute(insert_stmt)
            source = result.scalar_one_or_none()

            if source:
                await session.commit()
                return source

            stmt = select(self.model).filter_by(name=name, type=source_type)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def get_by_name_and_type(self, name: str, source_type: SourceType) -> Source | None:
        async with self.sessionmaker() as session:
            stmt = (
                select(Source)
                .where(
                    func.lower(Source.name) == name.lower(),
                    Source.type == source_type
                )
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def get_all_cex(self) -> list[Source]:
        async with self.sessionmaker() as session:
            stmt = (
                select(Source)
                .filter_by(type=SourceType.CEX)
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())


class GlobalTokenSourceRepo(BaseRepo):
    def __init__(self, session: async_sessionmaker):
        super().__init__(session, GlobalTokenSource)  # noqa

    async def bulk_create(self, data: list[dict]) -> list[GlobalTokenSource] | None:
        if not data:
            return None

        async with self.sessionmaker() as session:
            stmt = (
                insert(GlobalTokenSource)
                .values(data)
                .on_conflict_do_nothing(
                    index_elements=["global_token_id", "source_id"]
                )
                .returning(GlobalTokenSource)
            )

            result = await session.execute(stmt)
            await session.commit()
            return list(result.scalars().all())

    async def get_all(
            self,
            limit: int | None = None,
            offset: int | None = None,
            source_id: UUID | None = None
    ) -> list[GlobalTokenSource]:
        """Override base get_all to support source_id filtering"""
        async with self.sessionmaker() as session:
            stmt = select(GlobalTokenSource)

            if source_id:
                stmt = stmt.where(GlobalTokenSource.source_id == source_id)
            if limit:
                stmt = stmt.limit(limit)
            if offset:
                stmt = stmt.offset(offset)

            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_all_with_global_tokens(
            self,
            source_id: UUID | None = None
    ) -> list[GlobalTokenSource]:
        """Get all global_token_sources with their global_tokens eagerly loaded."""
        from sqlalchemy.orm import selectinload
        from infrastructure.database.models.tokens import GlobalToken

        async with self.sessionmaker() as session:
            stmt = select(GlobalTokenSource).options(
                selectinload(GlobalTokenSource.global_token)
            )

            if source_id:
                stmt = stmt.where(GlobalTokenSource.source_id == source_id)

            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_by_global_token_source_id(self, global_token_id: UUID, source_id: UUID) -> GlobalTokenSource:
        async with self.sessionmaker() as session:
            stmt = (
                select(GlobalTokenSource)
                .filter_by(global_token_id=global_token_id, source_id=source_id)
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def get_or_create(self, global_token_id: UUID, source_id: UUID) -> GlobalTokenSource:
        source = await self.get_by_global_token_source_id(global_token_id, source_id)
        if not source:
            async with self.sessionmaker() as session:
                stmt = (
                    insert(GlobalTokenSource)
                    .values(global_token_id=global_token_id, source_id=source_id)
                    .on_conflict_do_nothing(
                        index_elements=["global_token_id", "source_id"]
                    )
                    .returning(GlobalTokenSource)
                )

                result = await session.execute(stmt)
                await session.commit()
                source = result.scalar_one_or_none()

        return source

    async def get_by_pairs(self, pairs: list[tuple[UUID, UUID]]) -> list[GlobalTokenSource]:
        async with self.sessionmaker() as session:
            stmt = select(GlobalTokenSource).where(
                tuple_(
                    GlobalTokenSource.global_token_id,
                    GlobalTokenSource.source_id
                ).in_(pairs)
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())


class ExchangeBlacklistRepo(BaseRepo):
    def __init__(self, session: async_sessionmaker):
        super().__init__(session, ExchangeBlacklist)  # noqa

    async def get_all(self) -> list[ExchangeBlacklist]:
        """Get all exchange blacklist entries with source loaded"""
        from sqlalchemy.orm import selectinload
        async with self.sessionmaker() as session:
            stmt = select(ExchangeBlacklist).options(selectinload(ExchangeBlacklist.source))
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_active(self) -> list[ExchangeBlacklist]:
        """Get active exchange blacklist entries (filter expired)"""
        async with self.sessionmaker() as session:
            now = datetime.now(UTC).replace(tzinfo=None)
            stmt = (
                select(ExchangeBlacklist)
                .where(
                    or_(
                        ExchangeBlacklist.expires_at.is_(None),
                        ExchangeBlacklist.expires_at > now
                    )
                )
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_or_create(
        self,
        source_id: UUID,
        market_type: MarketType | None,
        mute_type: MuteType,
        expires_at: datetime | None
    ) -> ExchangeBlacklist:
        """Get or create exchange blacklist entry"""
        async with self.sessionmaker() as session:
            # Check if already exists
            stmt = select(ExchangeBlacklist).filter_by(
                source_id=source_id,
                market_type=market_type
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()

            if existing:
                return existing

            # Create new entry
            insert_stmt = (
                insert(ExchangeBlacklist)
                .values(
                    source_id=source_id,
                    market_type=market_type,
                    mute_type=mute_type,
                    expires_at=expires_at
                )
                .returning(ExchangeBlacklist)
            )

            result = await session.execute(insert_stmt)
            await session.commit()
            return result.scalar_one_or_none()

    async def delete(self, id: UUID) -> None:
        """Delete exchange blacklist entry by ID"""
        async with self.sessionmaker() as session:
            stmt = delete(ExchangeBlacklist).filter_by(id=id)
            await session.execute(stmt)
            await session.commit()

    async def is_muted(self, source_id: UUID, market_type: MarketType) -> bool:
        """Check if a source+market is muted (including global mutes)"""
        async with self.sessionmaker() as session:
            now = datetime.now(UTC).replace(tzinfo=None)

            # Check for:
            # 1. Specific market_type mute for this source
            # 2. Global mute (market_type is NULL) for this source
            stmt = (
                select(ExchangeBlacklist)
                .where(
                    ExchangeBlacklist.source_id == source_id,
                    or_(
                        ExchangeBlacklist.market_type == market_type,
                        ExchangeBlacklist.market_type.is_(None)
                    ),
                    or_(
                        ExchangeBlacklist.expires_at.is_(None),
                        ExchangeBlacklist.expires_at > now
                    )
                )
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none() is not None
