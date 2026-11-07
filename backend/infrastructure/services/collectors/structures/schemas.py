from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass
class CachedResult(Generic[T]):
    data: T | None
    updated: bool
