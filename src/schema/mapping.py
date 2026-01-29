"""Алиасы для mapping-ов"""
from typing import TypedDict, TypeAlias


class FieldInfo(TypedDict):
    data_type: str
    constraint_type: str | None
    new_column_name: str

class IndexInfo(TypedDict):
    new_table_name: str
    fields: dict[str, FieldInfo]

Map: TypeAlias = dict[str, IndexInfo]
