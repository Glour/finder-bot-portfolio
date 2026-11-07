from typing import Callable, Dict, Any, Awaitable

from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated

from infrastructure.database.requests import RequestsRepo
from infrastructure.services.common.cache import RedisCache


class DatabaseMiddleware(BaseMiddleware):
    def __init__(self, repo: RequestsRepo, redis: RedisCache) -> None:
        self.repo = repo
        self.redis = redis

    async def __call__(
            self,
            handler: Callable[
                [Message | CallbackQuery | ChatMemberUpdated, Dict[str, Any]],
                Awaitable[Any],
            ],
            event: Message | CallbackQuery | ChatMemberUpdated,
            data: Dict[str, Any],
    ) -> Any:
        data["repo"] = self.repo
        data["cache"] = self.redis

        result = await handler(event, data)

        return result
