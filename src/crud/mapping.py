import json
from typing import Any

from abstracts.crud import AbstractCRUD
from abstracts.db import AbstractExtractor


class MappingCRUD(AbstractCRUD):
    def __init__(self, path: str, extractor: AbstractExtractor):
        super().__init__(extractor)
        self.path = path

    async def get_obj(self, index: str) -> dict | None:
        try:
            with open(self.path, mode='r', encoding='utf-8') as file:
                map: dict = json.load(file)
        except (FileNotFoundError, PermissionError, json.JSONDecodeError):
            return None
            
        map_index = map.get(index, None)
        
        return map_index

    async def save_obj(self, index: str, obj: Any) -> bool:
        return True