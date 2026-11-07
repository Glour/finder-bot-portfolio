from typing import Any, Optional, Type, TypeVar, Union, List

from sqlalchemy import update, delete, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.future import select
from sqlalchemy.orm import DeclarativeMeta

T = TypeVar('T', bound=DeclarativeMeta)


class BaseRepo:
    def __init__(self, sessionmaker: async_sessionmaker, model: Type[T]):
        self.sessionmaker = sessionmaker
        self.model = model

    async def get(self, obj_id: Any) -> Optional[T]:
        async with self.sessionmaker() as session:
            stmt = select(self.model).filter_by(id=obj_id)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def create(self, **kwargs) -> Optional[T]:
        async with self.sessionmaker() as session:
            insert_stmt = (
                insert(self.model)
                .values(**kwargs)
                .returning(self.model)
            )

            try:
                result = await session.execute(insert_stmt)
                await session.commit()
                return result.scalar_one_or_none()

            except IntegrityError:
                await session.rollback()

    async def create_on_conflict_do_nothing(self, **kwargs) -> Optional[T]:
        async with self.sessionmaker() as session:
            if not kwargs:
                kwargs = {}

            insert_stmt = (
                insert(self.model)
                .values(**kwargs)
                .on_conflict_do_nothing(index_elements=[self.model.id])
                .returning(self.model)
            )
            result = await session.execute(insert_stmt)
            await session.commit()
            return result.scalar_one_or_none()

    async def update(self, obj_id: Union[Any, List[Any]], **kwargs) -> Union[T, List[T], None]:
        if not obj_id:
            return None

        async with self.sessionmaker() as session:
            if isinstance(obj_id, list):
                stmt = (
                    update(self.model)
                    .values(**kwargs)
                    .where(self.model.id.in_(obj_id))
                    .execution_options(synchronize_session="fetch")
                    .returning(self.model)
                )
            else:
                stmt = (
                    update(self.model)
                    .values(**kwargs)
                    .filter_by(id=obj_id)
                    .returning(self.model)
                )
            result = await session.execute(stmt)
            await session.commit()

            if isinstance(obj_id, list):
                return list(result.scalars().all())
            return result.scalar_one_or_none()

    async def delete(self, obj_id: Union[Any, List[Any]]) -> Union[T, List[T], None]:
        async with self.sessionmaker() as session:
            if isinstance(obj_id, list):
                stmt = (
                    delete(self.model)
                    .filter(self.model.id.in_(obj_id))
                    .returning(self.model)
                )
            else:
                stmt = (
                    delete(self.model)
                    .filter_by(id=obj_id)
                    .returning(self.model)
                )

            result = await session.execute(stmt)
            await session.commit()

            if isinstance(obj_id, list):
                return list(result.scalars().all())
            else:
                return result.scalar_one_or_none()

    async def delete_all(self):
        async with self.sessionmaker() as session:
            stmt = delete(self.model)
            await session.execute(stmt)
            await session.commit()

    async def get_all(
            self,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
    ) -> List[T]:
        async with self.sessionmaker() as session:
            stmt = select(self.model).order_by(self.model.created_at.desc())

            if limit:
                stmt = stmt.limit(limit)
            if offset:
                stmt = stmt.offset(offset)

            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def count_all(self) -> int:
        async with self.sessionmaker() as session:
            stmt = select(func.count()).select_from(self.model)
            result = await session.execute(stmt)
            return result.scalar()
