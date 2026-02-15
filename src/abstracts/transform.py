from abc import ABC, abstractmethod

from src.schema.mapping import IndexInfo
from src.schema.obj import ObjList


class AbstractTransform(ABC):
    @abstractmethod
    def transform(
        self,
        index_config: IndexInfo,
        batch_data: ObjList
    ) -> ObjList:
        pass
