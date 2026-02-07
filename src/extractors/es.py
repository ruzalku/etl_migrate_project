from typing import Sequence, Any, AsyncIterable

from elasticsearch import AsyncElasticsearch, helpers
from elastic_transport import ObjectApiResponse

from abstracts.db import AsyncAbstractExtractor
from schema.mapping import Map, FieldInfo
from schema.obj import ObjList


class Storage(AsyncAbstractExtractor[AsyncElasticsearch]):
    async def get_mapping(self) -> Map:
        if not self.client:
            return {}

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
    
    async def _from_response_to_data(self, scan_generator: AsyncIterable) -> ObjList:
        objs = []
        async for doc in scan_generator:
            obj = doc.get('_source', {})
            objs.append(obj)
        return objs
            

    async def get_objs(self, batch_size: int, index: str) -> ObjList:
        if not self.client:
            return []
        
        query = {
            'query': {
                'match_all': {}
            }
        }
        
        data_response = helpers.async_scan(
            client=self.client,
            index=index,
            query=query,
            size=batch_size
        )
        
        return await self._from_response_to_data(data_response)

    async def start(self):
        try:
            self.client = AsyncElasticsearch(**self.config)
        except (KeyError, TypeError):
            return
        
    async def stop(self):
        if self.client:
            await self.client.close()
