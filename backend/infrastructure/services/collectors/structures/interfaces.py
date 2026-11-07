from typing import Protocol
from uuid import UUID

from infrastructure.database.models import Token
from infrastructure.services.collectors.structures.schemas import CachedResult


class SupportsParser(Protocol):
    def source_id(self) -> UUID:
        ...

    def source_appellation(self) -> str:
        ...

    async def setup(self) -> None:
        ...

    async def save_to_db(self, data: list[dict] | tuple[str, dict]) -> list[dict] | None:
        ...


class SupportsTokenParsing(SupportsParser, Protocol):

    async def fetch_tokens(self) -> list[dict] | tuple[list[Token], list[str]]:
        ...

    async def fetch_and_check(self) -> CachedResult[list[dict]]:
        ...


class SupportsPriceParsing(SupportsParser, Protocol):

    async def collect_tokens(self) -> list[dict] | tuple[list[Token], list[str]]:
        ...

    async def start(self) -> None:
        ...
