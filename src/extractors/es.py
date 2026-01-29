from typing import Sequence, Any

from elasticsearch import AsyncElasticsearch
from elastic_transport import ObjectApiResponse

from abstracts.db import AbstractExtractor
from schema.mapping import Map, FieldInfo
from schema.obj import ObjList


class Storage(AbstractExtractor[AsyncElasticsearch]):
    async def get_mapping(self) -> Map:
        mappings_response = await self.client.indices.get_mapping()
        mappings: Map = self._from_es_to_mapping(mappings_response.raw)

        return mappings
    
    def _from_es_to_mapping(self, es_mapping: dict[str, Any]) -> Map:
        mapping: Map = {}
        for index, index_field in es_mapping.items():
            mapping[index] = {"new_table_name": index, "fields": {}}

            if not isinstance(index_field, dict):
                continue
            
            rows = index_field.get('mappings')

            if not isinstance(rows, dict):
                continue

            for row, value in rows.items():
                if not isinstance(value, dict):
                    continue
                mapping[index]['fields'][row] = FieldInfo(
                    data_type=value.get('type', ''),
                    constraint_type=None,
                    new_column_name=value.get('type', '')
                )
                
        return mapping
                
            
    
    async def get_objs(self) -> ObjList:
        return await super().get_objs()
    
    async def save_objs(self, objs: ObjList) -> None:
        return await super().save_objs(objs)
