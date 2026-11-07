from aiogram import Router, F
from aiogram.types import ChatMemberUpdated

router = Router()


@router.chat_member(F.new_chat_member.status == "member")
async def new_channel_member(event: ChatMemberUpdated):
    if event.new_chat_member.status == "member" and not event.from_user.is_bot:
        pass
