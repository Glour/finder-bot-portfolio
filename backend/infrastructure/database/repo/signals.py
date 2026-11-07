from typing import Optional, List

from sqlalchemy import func, text
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.future import select

from infrastructure.database.models.signals import SignalHistory
from infrastructure.database.repo.base import BaseRepo


class SignalHistoryRepo(BaseRepo):
    def __init__(self, sessionmaker: async_sessionmaker):
        super().__init__(sessionmaker, SignalHistory)

    async def get_recent(
        self,
        limit: int = 100,
        symbol: Optional[str] = None,
        level: Optional[str] = None,
        buy_from: Optional[str] = None,
        sell_to: Optional[str] = None,
    ) -> List[SignalHistory]:
        async with self.sessionmaker() as session:
            stmt = select(SignalHistory).order_by(SignalHistory.analyzed_at.desc())

            if symbol:
                stmt = stmt.where(SignalHistory.symbol == symbol)
            if level:
                stmt = stmt.where(SignalHistory.level == level)
            if buy_from:
                stmt = stmt.where(SignalHistory.buy_from.ilike(f"%{buy_from}%"))
            if sell_to:
                stmt = stmt.where(SignalHistory.sell_to.ilike(f"%{sell_to}%"))

            stmt = stmt.limit(limit)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_stats(self, hours: int = 24) -> dict:
        async with self.sessionmaker() as session:
            cutoff = func.now() - text(f"INTERVAL '{hours} hours'")

            # Count by level
            stmt = (
                select(SignalHistory.level, func.count().label("cnt"))
                .where(SignalHistory.created_at >= cutoff)
                .group_by(SignalHistory.level)
            )
            result = await session.execute(stmt)
            by_level = {row.level: row.cnt for row in result}

            # Aggregate stats
            stmt = (
                select(
                    func.count().label("total"),
                    func.avg(SignalHistory.spread_percent).label("avg_spread"),
                    func.avg(SignalHistory.profit).label("avg_profit"),
                    func.max(SignalHistory.profit).label("max_profit"),
                )
                .where(SignalHistory.created_at >= cutoff)
            )
            result = await session.execute(stmt)
            row = result.one()

            return {
                "hours": hours,
                "total": row.total or 0,
                "avg_spread": round(float(row.avg_spread or 0), 2),
                "avg_profit": round(float(row.avg_profit or 0), 2),
                "max_profit": round(float(row.max_profit or 0), 2),
                "by_level": by_level,
            }
