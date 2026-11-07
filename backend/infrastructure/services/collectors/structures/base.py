import hashlib
import json
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from infrastructure.database.models.base import SourceType
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.collectors.structures.interfaces import SupportsParser
from infrastructure.services.collectors.structures.schemas import CachedResult
from infrastructure.services.common.cache import RedisCache


class BaseParser(SupportsParser):
    def __init__(
            self,
            cache,
            repo,
            *,
            source_name: str,
            source_type,
    ):
        self.cache: RedisCache = cache
        self.repo: RequestsRepo = repo
        self.source_name: str = source_name
        self.source_type: SourceType = source_type
        self.source = None

    def source_id(self) -> UUID:
        return self.source.id

    def source_appellation(self) -> str:
        return self.source_name

    async def setup(self):
        self.source = await self.repo.sources.get_by_name_and_type(self.source_name, self.source_type)
        if not self.source:
            self.source = await self.repo.sources.get_or_create(name=self.source_name, source_type=self.source_type)

    def deep_sort(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self.deep_sort(v) for k, v in sorted(obj.items())}
        elif isinstance(obj, list):
            return sorted([self.deep_sort(i) for i in obj], key=lambda x: json.dumps(x, sort_keys=True))
        elif isinstance(obj, Enum):
            return obj.value
        elif isinstance(obj, UUID):
            return str(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return obj

    async def fetch_and_check_internal(self, cache_key: str, data: list[dict]) -> "CachedResult[list[dict]]":
        normalized = self.deep_sort(data)
        serialized = json.dumps(normalized, separators=(",", ":"), ensure_ascii=False)
        current_hash = hashlib.sha256(serialized.encode()).hexdigest()
        cached_hash = await self.cache.get(f"{cache_key}_hash")

        if current_hash == cached_hash:
            return CachedResult(data=None, updated=False)

        await self.cache.set(cache_key, serialized, timeout_secs=300)
        await self.cache.set(f"{cache_key}_hash", current_hash, timeout_secs=300)

        return CachedResult(data=data, updated=True)
