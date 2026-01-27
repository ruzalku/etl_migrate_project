"""Алиасы для mapping-ов"""
from typing import TypedDict, TypeAlias


class FieldInfo(TypedDict):
    data_type: str
    constraint_type: str | None
    new_column_name: str

IndexMapValue: TypeAlias = FieldInfo | str

IndexMap: TypeAlias = dict[str, IndexMapValue]

Map: TypeAlias = dict[str, IndexMapValue]
