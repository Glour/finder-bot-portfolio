from aiogram.fsm.state import StatesGroup, State


class TokenBlacklistState(StatesGroup):
    get_new_token = State()


class ExchangeBlacklistState(StatesGroup):
    get_new_exchange = State()


class PairBlacklistState(StatesGroup):
    get_pair = State()
