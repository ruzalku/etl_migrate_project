"""Алиасы для mapping-ов"""
from typing import TypedDict, TypeAlias


class FieldInfo(TypedDict):
    options: dict
    old_column_name: str

class IndexInfo(TypedDict):
    old_table_name: str
    fields: dict[str, FieldInfo]

Map: TypeAlias = dict[str, IndexInfo]
