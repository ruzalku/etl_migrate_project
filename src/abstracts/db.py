from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Any

from src.schema.obj import ObjList
from src.schema.mapping import Map
from src.schema.enums import Mode


TClient = TypeVar("TClient")

class BaseStorage(Generic[TClient]):
    def __init__(
        self,
        config: dict,
        cdc: bool = False,
        update_row: str = '',
        pk_col: str = '',
        cdc_mode: Mode = Mode.TIMESTAMP,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.config = config
        self.client: TClient | None = None
        self.cdc = cdc
        self.update_row = update_row
        self.mode = cdc_mode


class AsyncAbstractExtractor(ABC, BaseStorage[TClient]):
    """Абстрактный класс для выгрузки данных для асинхронных клиентов"""
    @abstractmethod
    async def start(self):
        """Загрузка конфига в клиента"""
        pass
    
    @abstractmethod
    async def stop(self):
        """Остановка клиента"""
        pass

    @abstractmethod
    async def get_mapping(self) -> Map:
        """Получение mapping-а"""
        pass

    @abstractmethod
    async def get_objs(self) -> ObjList:
        """Получние объектов из storage"""
        pass
    

class AsyncAbstractLoader(ABC, BaseStorage[TClient]):
    """Абстрактный класс для загрузки данных для асинхронных клиентов"""
    @abstractmethod
    async def start(self):
        """Загрузка конфига в клиента"""
        pass
    
    @abstractmethod
    async def stop(self):
        """Остановка клиента"""
        pass

    @abstractmethod
    async def save_objs(self, index: str, objs: ObjList):
        """Сохранения обектов в хранилище"""
        pass
    

class AbstractExtractor(ABC, BaseStorage[TClient]):
    """Абстрактный класс для выгрузки данных для синхронных клиентов"""
    @abstractmethod
    def start(self):
        """Загрузка конфига в клиента"""
        pass
    
    @abstractmethod
    async def stop(self):
        """Остановка клиента"""
        pass

    @abstractmethod
    def get_mapping(self) -> Map:
        """Получение mapping-а"""
        pass

    @abstractmethod
    def get_objs(self) -> ObjList:
        """Получние объектов из storage"""
        pass
    

class AbstractLoader(ABC, BaseStorage[TClient]):
    """Абстрактный класс для загрузки данных для синхронных клиентов"""
    @abstractmethod
    def start(self):
        """Загрузка конфига в клиента"""
        pass
    
    @abstractmethod
    async def stop(self):
        """Остановка клиента"""
        pass

    @abstractmethod
    def save_objs(self, index: str, objs: ObjList):
        """Сохранения обектов в хранилище"""
        pass
