from aiogram import Router, F

from handlers.user import start

user_router = Router()
user_router.message.filter(F.chat.func(lambda chat: chat.type == "private"))
user_router.include_routers(*[
    start.router,
])
