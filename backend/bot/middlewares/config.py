from typing import Callable, Dict, Any, Awaitable

from aiogram import BaseMiddleware
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from settings.app_settings import AppSettings


class ConfigMiddleware(BaseMiddleware):
    def __init__(
            self,
            settings: AppSettings,
            scheduler: AsyncIOScheduler,
    ) -> None:
        self.settings = settings
        self.scheduler = scheduler

    async def __call__(
            self,
            handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
            event: Message,
            data: Dict[str, Any],
    ) -> Any:
        data["settings"] = self.settings
        data["scheduler"] = self.scheduler

        return await handler(event, data)
