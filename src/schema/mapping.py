"""Алиасы для mapping-ов"""
from typing import TypedDict, TypeAlias


class FieldInfo(TypedDict):
    options: dict

class IndexInfo(TypedDict):
    old_table_name: str
    fields: dict[str, FieldInfo]
    options: dict

Map: TypeAlias = dict[str, IndexInfo]
