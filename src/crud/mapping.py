import json
from typing import Any

from src.abstracts.crud import AbstractCRUD
from src.abstracts.db import AbstractExtractor


class MappingCRUD(AbstractCRUD):
    def __init__(self, path: str):
        self.path = path

    async def get_obj(self, index: str) -> dict | None:
        try:
            with open(self.path, mode='r', encoding='utf-8') as file:
                map_data: dict = json.load(file)

            return map_data if not index else map_data.get(index)

        except (FileNotFoundError, PermissionError, json.JSONDecodeError) as e:
            return None

    async def save_obj(self, index: str, obj: Any) -> bool:
        return True