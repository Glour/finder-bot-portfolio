import asyncio
import logging
import time
from typing import Union

from aiogram import Bot
from aiogram import exceptions
from aiogram.types import InlineKeyboardMarkup


# Global rate limiter: max 20 messages per second per bot (Telegram limit is 30)
_bot_rate_limiters: dict[int, asyncio.Semaphore] = {}
_bot_last_send: dict[int, float] = {}
_MIN_INTERVAL = 0.12  # 120ms between messages = ~8 msg/sec per process (3 processes * 8 = 24 < 30 TG limit)

# Max total retry time before giving up
_MAX_RETRY_SECONDS = 30


MAX_MESSAGE_LENGTH = 4096


def _get_rate_limiter(bot_id: int) -> asyncio.Semaphore:
    """Get or create rate limiter semaphore for a bot."""
    if bot_id not in _bot_rate_limiters:
        _bot_rate_limiters[bot_id] = asyncio.Semaphore(1)
    return _bot_rate_limiters[bot_id]


def _split_text(text: str) -> list[str]:
    """Split text into chunks fitting within Telegram's 4096 char limit.
    Prefers double-newline boundaries to keep logical groups together.
    """
    if len(text) <= MAX_MESSAGE_LENGTH:
        return [text]
    parts = []
    while len(text) > MAX_MESSAGE_LENGTH:
        split_pos = text.rfind("\n\n", 0, MAX_MESSAGE_LENGTH)
        if split_pos <= 0:
            split_pos = text.rfind("\n", 0, MAX_MESSAGE_LENGTH)
        if split_pos <= 0:
            split_pos = MAX_MESSAGE_LENGTH
        parts.append(text[:split_pos])
        text = text[split_pos:].lstrip("\n")
    if text:
        parts.append(text)
    return parts


async def _send_single(
        bot: Bot,
        text: str,
        chat_id: int | str,
        tread_id: int | str | None = None,
        disable_notification: bool = False,
        reply_markup: InlineKeyboardMarkup = None,
) -> bool:
    """Send a single message chunk with rate limiting and retry."""
    bot_id = bot.id or id(bot)
    limiter = _get_rate_limiter(bot_id)
    start_time = time.monotonic()

    async with limiter:
        # Enforce minimum interval between sends
        now = time.monotonic()
        last = _bot_last_send.get(bot_id, 0)
        wait = _MIN_INTERVAL - (now - last)
        if wait > 0:
            await asyncio.sleep(wait)

        while True:
            elapsed = time.monotonic() - start_time
            if elapsed > _MAX_RETRY_SECONDS:
                logging.warning(
                    f"Target [ID:{chat_id}]: giving up after {elapsed:.1f}s of retries"
                )
                return False

            try:
                _bot_last_send[bot_id] = time.monotonic()
                await bot.send_message(
                    chat_id,
                    text,
                    message_thread_id=tread_id,
                    disable_notification=disable_notification,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                )
                return True
            except exceptions.TelegramBadRequest:
                logging.error(f"Telegram server says - Bad Request: chat {chat_id} not found for bot: {bot_id}")
                return False
            except exceptions.TelegramForbiddenError:
                logging.error(f"Target [ID:{chat_id}]: got TelegramForbiddenError")
                return False
            except exceptions.TelegramRetryAfter as e:
                remaining = _MAX_RETRY_SECONDS - elapsed
                sleep_time = min(e.retry_after, remaining)
                if sleep_time <= 0:
                    logging.warning(
                        f"Target [ID:{chat_id}]: flood limit {e.retry_after}s but no time left, giving up"
                    )
                    return False
                logging.warning(
                    f"Target [ID:{chat_id}]: flood limit, sleeping {sleep_time:.1f}s "
                    f"(asked {e.retry_after}s, budget {remaining:.1f}s)"
                )
                await asyncio.sleep(sleep_time)
                # Loop back to retry
            except (exceptions.TelegramAPIError, exceptions.TelegramNetworkError):
                logging.warning(f"Target [ID:{chat_id}]: failed")
                return False


async def send_message(
        bot: Bot,
        text: str,
        chat_id: int | str,
        tread_id: int | str | None = None,
        disable_notification: bool = False,
        reply_markup: InlineKeyboardMarkup = None,
) -> bool:
    """
    Safe messages sender with rate limiting and automatic text splitting.

    Splits messages exceeding Telegram's 4096 char limit.
    Uses global per-bot rate limiter to prevent Telegram flood cascading.
    Retries on TelegramRetryAfter but gives up after _MAX_RETRY_SECONDS total.
    """
    parts = _split_text(text)
    success = True
    for i, part in enumerate(parts):
        is_last = (i == len(parts) - 1)
        ok = await _send_single(
            bot=bot,
            text=part,
            chat_id=chat_id,
            tread_id=tread_id,
            disable_notification=disable_notification,
            reply_markup=reply_markup if is_last else None,
        )
        if not ok:
            success = False
    return success


async def broadcast(
        bot: Bot,
        users: list[Union[str, int]],
        text: str,
        disable_notification: bool = False,
        reply_markup: InlineKeyboardMarkup = None,
        delay: float = 0.05
) -> int:
    """
    Simple broadcaster.
    :param delay: Delay between messages, default is 0.05 sec. 20 messages per second (Limit: 30 messages per second).
    :param bot: Bot instance.
    :param users: List of users.
    :param text: Text of the message.
    :param disable_notification: Disable notification or not.
    :param reply_markup: Reply markup.
    :return: Count of messages.
    """
    count = 0
    try:
        for user_id in users:
            if await send_message(
                    bot=bot,
                    text=text,
                    chat_id=user_id,
                    tread_id=None,
                    disable_notification=disable_notification,
                    reply_markup=reply_markup
            ):
                count += 1
            await asyncio.sleep(delay)
        return count

    finally:
        logging.debug(f"{count} messages successful sent.")
