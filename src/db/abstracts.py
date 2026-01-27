from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Any

from schema.obj import ObjList

TClient = TypeVar("TClient")


class AbstractStorage(ABC, Generic[TClient]):
    def __init__(self, client: TClient):
        self.client = client

    @abstractmethod
    async def get_mapping(self, index: str) -> Any:
        """Получение mapping-а"""
        pass

    @abstractmethod
    async def save_objs(self, objs: ObjList) -> None:
        """Сохранение объектов в storage"""
        pass

    @abstractmethod
    async def get_objs(self) -> ObjList:
        """Получние объектов из storage"""
        pass
