import json

from .abstract import AbstractCRUD
from db.abstracts import AbstractStorage


class MappingCRUD(AbstractCRUD):
    def __init__(self, path: str):
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
        try:
            with open(self.path, mode='w', encoding='utf-8') as file:
                json.dump(obj, file)
        except PermissionError:
            return False
        
        return True
