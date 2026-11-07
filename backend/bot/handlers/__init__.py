"""Import all routers and add them to routers_list."""

from handlers.admin import admin_router
from handlers.channel import channel_router
from handlers.errors import bot_api_errors, error_router
from handlers.user import start, user_router

routers_list = [
    user_router,
    admin_router,
    channel_router,
    error_router,
]

__all__ = [
    "routers_list",
]
