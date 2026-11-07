from aiogram import Router

from filters.admin import AdminFilter
from handlers.admin import admin

admin_router = Router()
admin_router.message.filter(AdminFilter())

admin_router.include_routers(*[
    admin.router,
])
