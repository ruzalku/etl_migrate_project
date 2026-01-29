from abc import ABC, abstractmethod
from typing import Any

from schema.mapping import Map
from .db import AbstractExtractor


class AbstractCRUD(ABC):
    def __init__(self, extractor: AbstractExtractor):
        self.extractor = extractor

    @abstractmethod
    async def get_obj(self, index: str) -> Any:
        pass

    @abstractmethod
    async def save_obj(self, index: str, obj: Any) -> bool:
        pass
