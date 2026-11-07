from aiogram import Bot
from aiogram.client.default import DefaultBotProperties

from settings import settings

test_bot = Bot(token=settings.bot.test_token, default=DefaultBotProperties(parse_mode="HTML"))
dev_bot = Bot(token=settings.bot.dev_token, default=DefaultBotProperties(parse_mode="HTML"))
prod_bot = Bot(token=settings.bot.prod_token, default=DefaultBotProperties(parse_mode="HTML"))
en_prod_bot = Bot(token=settings.bot.en_prod_token, default=DefaultBotProperties(parse_mode="HTML"))
tech_prod_bot = Bot(token=settings.bot.tech_prod_token, default=DefaultBotProperties(parse_mode="HTML"))
