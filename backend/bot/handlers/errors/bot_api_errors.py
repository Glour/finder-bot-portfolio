import asyncio
import logging

from aiogram import Router
from aiogram.dispatcher.event.bases import CancelHandler
from aiogram.exceptions import TelegramRetryAfter, TelegramUnauthorizedError, TelegramForbiddenError, \
    TelegramBadRequest, TelegramNotFound, TelegramConflictError, TelegramNetworkError, \
    TelegramEntityTooLarge
from aiogram.types import ErrorEvent
from aiohttp import ServerDisconnectedError, ServerTimeoutError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

router = Router()


@router.error()
async def error_handler(event: ErrorEvent):
    if isinstance(event.exception, TelegramRetryAfter):
        logging.warning(f"Флуд атака началась, сплю {event.exception.retry_after} сек.")
        await asyncio.sleep(int(event.exception.retry_after) + 2)
        return True  # Слишком много запросов

    if isinstance(event, CancelHandler):

        logger.info(f"CancelHandler: {event}")
        return True  # Отмена обработчика

    if isinstance(event.exception, TelegramForbiddenError):
        logger.debug(f'Unauthorized: {event}')
        return True  # Неавторизованный, заблокированный

    if isinstance(event.exception, TelegramNotFound):
        return True  # Сообщение для удаления не найдено.

    if isinstance(event.exception, TelegramEntityTooLarge):
        logger.exception(f"TelegramEntityTooLarge: {event.exception}")
        return True  # Слишком большой файл

    if isinstance(event.exception, (TelegramConflictError, TelegramUnauthorizedError)):
        logger.error("Токен неверный или уже используется другим ботом.")
        return True  # Проблемы с токеном.

    if isinstance(event.exception, (ServerDisconnectedError, ServerTimeoutError)):
        return True  # Ошибки сети

    if isinstance(event.exception, TelegramBadRequest):
        logger.exception(f"BadRequest: {event.exception}")
        return True  # Неверный запрос

    if isinstance(event.exception, TelegramNetworkError):
        logger.debug(f"TelegramNetworkError: {event.exception}")
        return True  # Ошибка API Telegram

    logger.error("Critical error caused by %s", event.exception, exc_info=True)
    return None
