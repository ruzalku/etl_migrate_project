import json

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
    
    async def save_obj(self, obj: dict) -> bool:
        mapping = self.extractor.get_mapping()
        try:
            with open(self.path, mode='w', encoding='utf-8') as file:
                json.dump(obj, file)
        except PermissionError:
            return False
        
        return True
