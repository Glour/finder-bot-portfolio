import logging

from aiogram.exceptions import TelegramAPIError
from aiogram.types import Message, FSInputFile

MAX_MESSAGE_LENGTH = 4096


def split_text(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> list[str]:
    """Split text into chunks that fit within Telegram's message length limit.
    Prefers splitting at double-newline boundaries (between groups) to avoid
    cutting between a group header and its items.
    """
    if len(text) <= max_length:
        return [text]

    parts = []
    while len(text) > max_length:
        # Prefer double-newline split (between groups)
        split_pos = text.rfind("\n\n", 0, max_length)
        if split_pos <= 0:
            # Fall back to single newline
            split_pos = text.rfind("\n", 0, max_length)
        if split_pos <= 0:
            split_pos = max_length
        parts.append(text[:split_pos])
        text = text[split_pos:].lstrip("\n")

    if text:
        parts.append(text)

    return parts


async def edit_message(message: Message, text: str, reply_markup=None, photo_path: str = None):
    parts = split_text(text)

    # Edit the first part in place, send the rest as new messages
    first_part = parts[0]
    rest_parts = parts[1:]

    try:
        if not photo_path:
            msg = await message.edit_text(first_part, reply_markup=reply_markup if not rest_parts else None,
                                          disable_web_page_preview=True)
        else:
            msg = await message.answer_photo(photo=FSInputFile(photo_path), caption=first_part,
                                             reply_markup=reply_markup if not rest_parts else None)
            await message.delete()
    except TelegramAPIError:
        try:
            if not photo_path:
                msg = await message.answer(first_part, reply_markup=reply_markup if not rest_parts else None,
                                           disable_web_page_preview=True)
            else:
                msg = await message.answer_photo(photo=FSInputFile(photo_path), caption=first_part,
                                                 reply_markup=reply_markup if not rest_parts else None)
        except Exception as e:
            logging.error(f"edit_message fallback failed: {e}")
            return None
    except Exception as e:
        logging.error(f"edit_message failed: {e}")
        return None

    # Send remaining parts
    for i, part in enumerate(rest_parts):
        is_last = (i == len(rest_parts) - 1)
        try:
            msg = await message.answer(part,
                                       reply_markup=reply_markup if is_last else None,
                                       disable_web_page_preview=True)
        except Exception as e:
            logging.error(f"edit_message part {i + 2} failed: {e}")

    return msg
