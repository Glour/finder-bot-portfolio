from aiogram import Router, F

from handlers.channel.new_channel_member import router
from settings import settings

channel_router = Router()
channel_router.chat_member.filter(F.chat.id == settings.bot.dev_channel_id)

channel_router.include_routers(*[
    router,
])
