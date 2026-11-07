from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

router = Router()


@router.message(CommandStart())
async def user_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Hello")
