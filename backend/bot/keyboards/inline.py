from aiogram.types import (
    InlineKeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

admin_keyboard = InlineKeyboardBuilder()

# Первая строка - Показать
admin_keyboard.row(
    InlineKeyboardButton(text="📋 Токены", callback_data="show_token_blacklist"),
    InlineKeyboardButton(text="🏢 Биржи", callback_data="show_exchange_blacklist"),
)

# Вторая строка - Добавить
admin_keyboard.row(
    InlineKeyboardButton(text="➕ Токен", callback_data="add_token_to_blacklist"),
    InlineKeyboardButton(text="➕ Биржу", callback_data="add_exchange_to_blacklist"),
    InlineKeyboardButton(text="➕ Пару GT", callback_data="add_pair_to_blacklist"),
)

# Третья строка - Удалить
admin_keyboard.row(
    InlineKeyboardButton(text="➖ Токен", callback_data="remove_token_from_blacklist"),
    InlineKeyboardButton(text="➖ Биржу", callback_data="remove_exchange_from_blacklist"),
    InlineKeyboardButton(text="➖ Пару GT", callback_data="remove_pair_from_blacklist"),
)

admin_keyboard = admin_keyboard.as_markup()

mute_token_kb = InlineKeyboardBuilder().add(
    InlineKeyboardButton(text="Навсегда", callback_data="add_bl:PERMANENT"),
    InlineKeyboardButton(text="На 24ч", callback_data="add_bl:TEMPORARY"),
).adjust(1).as_markup()

exchange_mute_kb = InlineKeyboardBuilder().add(
    InlineKeyboardButton(text="Навсегда", callback_data="add_ex_bl:PERMANENT"),
    InlineKeyboardButton(text="На 24ч", callback_data="add_ex_bl:TEMPORARY"),
).adjust(1).as_markup()


async def global_candidate_approve_kb(candidate_hash: str):
    return InlineKeyboardBuilder().add(
        InlineKeyboardButton(
            text="Одобрить",
            callback_data=f"global_approve:{candidate_hash}"
        ),
        InlineKeyboardButton(
            text="Отклонить",
            callback_data=f"global_decline:{candidate_hash}"
        ),
    ).adjust(1).as_markup()
