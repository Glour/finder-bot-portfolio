"""
Database setup and session management with proper singleton pattern.

This module provides database engine and session factory creation,
eliminating global state issues and implementing proper dependency injection.
"""
from typing import AsyncGenerator, Optional
from contextlib import asynccontextmanager

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession, AsyncEngine

from settings.app_settings import AppSettings
from settings.db_settings import DatabaseSettings


class DatabaseManager:
    """
    Singleton database manager with proper lifecycle management.

    Manages database engine, session pool, and Redis connections
    with proper initialization and cleanup.
    """

    _instance: Optional['DatabaseManager'] = None
    _initialized: bool = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # Only initialize once
        if DatabaseManager._initialized:
            return

        self._engine: Optional[AsyncEngine] = None
        self._async_session: Optional[async_sessionmaker] = None
        self._redis: Optional[Redis] = None

        DatabaseManager._initialized = True

    @property
    def async_session(self) -> async_sessionmaker:
        """Get the session maker."""
        if self._async_session is None:
            raise RuntimeError("Database not initialized. Call init_database() first.")
        return self._async_session

    @property
    def redis(self) -> Redis:
        """Get Redis client."""
        if self._redis is None:
            raise RuntimeError("Redis not initialized. Set redis property first.")
        return self._redis

    @redis.setter
    def redis(self, value: Redis) -> None:
        """Set Redis client."""
        self._redis = value

    @staticmethod
    def create_engine(db: DatabaseSettings, echo: bool = False) -> AsyncEngine:
        """
        Create SQLAlchemy async engine with optimized settings.

        Args:
            db: Database settings
            echo: Enable SQL query logging

        Returns:
            Configured async engine
        """
        engine = create_async_engine(
            db.construct_sqlalchemy_url,
            query_cache_size=1200,
            pool_size=20,
            max_overflow=200,
            pool_pre_ping=True,  # Verify connections before using
            pool_recycle=3600,   # Recycle connections after 1 hour
            future=True,
            echo=echo,
        )
        return engine

    @staticmethod
    def create_session_pool(engine: AsyncEngine) -> async_sessionmaker:
        """
        Create session pool bound to engine.

        Args:
            engine: Database engine

        Returns:
            Session maker factory
        """
        session_pool = async_sessionmaker(
            bind=engine,
            expire_on_commit=False,
            class_=AsyncSession
        )
        return session_pool

    def init_database(self, settings: AppSettings, echo: bool = False) -> None:
        """
        Initialize database engine and session pool.

        Args:
            settings: Application settings
            echo: Enable SQL query logging
        """
        if self._engine is not None:
            raise RuntimeError("Database already initialized")

        self._engine = self.create_engine(settings.postgres, echo=echo)
        self._async_session = self.create_session_pool(self._engine)

    async def close_database(self) -> None:
        """Close database engine and cleanup resources."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._async_session = None

        if self._redis:
            await self._redis.close()
            self._redis = None

    @asynccontextmanager
    async def lifespan(self):
        """Context manager for proper lifecycle management."""
        try:
            yield self
        finally:
            await self.close_database()


# Singleton instance
_db_manager = DatabaseManager()


# Legacy global variables for backward compatibility
# These maintain the original module-level variable pattern for assignment compatibility
async_session: Optional[async_sessionmaker] = None
redis: Optional[Redis] = None


# Legacy functions for backward compatibility
def create_engine(db: DatabaseSettings, echo: bool = False) -> AsyncEngine:
    """Legacy function - delegates to DatabaseManager."""
    return DatabaseManager.create_engine(db, echo)


def create_session_pool(engine: AsyncEngine) -> async_sessionmaker:
    """Legacy function - delegates to DatabaseManager."""
    return DatabaseManager.create_session_pool(engine)


def init_database(settings: AppSettings, echo: bool = False) -> None:
    """
    Legacy function - initializes singleton DatabaseManager.

    Args:
        settings: Application settings
        echo: Enable SQL query logging
    """
    global async_session

    _db_manager.init_database(settings, echo=echo)

    # Update module-level variable for backward compatibility
    async_session = _db_manager._async_session


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency injection function for FastAPI routes.

    Yields:
        Database session

    Example:
        @app.get("/items")
        async def get_items(db: AsyncSession = Depends(get_db)):
            ...
    """
    session_maker = _db_manager.async_session

    async with session_maker() as session:
        try:
            yield session
        finally:
            await session.close()


# Export singleton for modern usage
db_manager = _db_manager
