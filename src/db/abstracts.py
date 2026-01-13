from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Sequence

TClient = TypeVar("TClient")
TObj = TypeVar("TObj")


class AbstractStorage(ABC, Generic[TClient, TObj]):
    def __init__(self, client: TClient):
        self.client = client

    @abstractmethod
    async def get_mapping(self, index: str) -> None:
        """Получение mapping-а"""
        pass

    @abstractmethod
    async def save_objs(self, objs: Sequence[TObj]) -> None:
        """Сохранение объектов в storage"""
        pass

    @abstractmethod
    async def get_objs(self) -> Sequence[TObj]:
        """Получние объектов из storage"""
        pass
