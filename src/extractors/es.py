from typing import Any, AsyncIterable, Optional

from elasticsearch import AsyncElasticsearch, helpers

from abstracts.db import AsyncAbstractExtractor
from schema.mapping import Map, FieldInfo
from schema.obj import ObjList
from schema.enums import Mode
from src.crud.json_state import JSONStateManager
from src.core.backoff import backoff


class Storage(AsyncAbstractExtractor[AsyncElasticsearch]):
    def __init__(
        self,
        state_manager: JSONStateManager | None = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.state_manager = state_manager

    @backoff()
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

    async def _from_response_to_data(
        self,
        scan_generator: AsyncIterable,
    ) -> tuple[ObjList, Any]:
        objs = []
        async for doc in scan_generator:
            obj = doc.get('_source', {})
            if self.mode == Mode.TIMESTAMP:
                last_state = obj.get(self.update_row)
            elif self.mode == Mode.LOGS:
                last_state = doc.get('_seq_no')
            objs.append(obj)
        return objs, last_state

    @backoff()
    async def get_objs(
        self,
        index: str,
        batch_size: int = 500,
        last_state: Optional[Any] = None
    ) -> ObjList:
        if not self.client:
            return []

        query = {"query": {"match_all": {}}}

        if self.cdc:
            if self.mode == Mode.TIMESTAMP:
                query = self._build_timestamp_query(last_state)
            elif self.mode == Mode.LOGS:
                query = self._build_logs_query(last_state)

        sort_field = self.update_row if self.update_row else "_doc"

        data_response = helpers.async_scan(
            client=self.client,
            index=index,
            query=query,
            size=batch_size,
            preserve_order=True,
            sort=[{sort_field: "asc"}],
            scroll='5m'
        )

        batch, last_state = await self._from_response_to_data(data_response)

        if self.state_manager:
            self.state_manager.set_state(key=f'es_{index}', value=last_state)

        return batch

    def _build_timestamp_query(self, last_state: Any) -> dict:
        """Режим 'timestamp': фильтрация по дате/времени обновления"""
        if not last_state:
            return {"query": {"match_all": {}}}

        return {
            "query": {
                "range": {
                    self.update_row: {"gt": last_state}
                }
            }
        }

    def _build_logs_query(self, last_state: Any) -> dict:
        """Режим 'logs':  по _seq_no и работает только с включенными метаданными"""
        if not last_state:
            return {"query": {"match_all": {}}}

        return {
            "query": {
                "range": {
                    "_seq_no": {"gt": last_state}
                }
            }
        }

    async def start(self):
        try:
            self.client = AsyncElasticsearch(**self.config)
        except (KeyError, TypeError):
            return

    async def stop(self):
        if self.client:
            await self.client.close()
