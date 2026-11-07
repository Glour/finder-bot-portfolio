import logging
from gettext import gettext as _

from aiogram import Bot, exceptions
from aiogram.types import BotCommand, BotCommandScopeAllPrivateChats, BotCommandScopeChat


async def set_default_commands(bot: Bot):
    await bot.delete_my_commands()
    commands = [
        BotCommand(command="start", description=_("Start the bot")),
    ]
    return await bot.set_my_commands(commands=commands, scope=BotCommandScopeAllPrivateChats())


async def set_admin_commands(bot: Bot, admin_ids: list[int]):
    for admin_id in admin_ids:
        try:
            await bot.set_my_commands(
                [
                    BotCommand(command="start", description=_("Start the bot")),
                    BotCommand(command="admin", description=_("Admin panel")),
                ], scope=BotCommandScopeChat(chat_id=admin_id)
            )
        except exceptions.TelegramBadRequest:
            logging.info(f"Can't set admin commands: chat {admin_id} not found.")
    return
