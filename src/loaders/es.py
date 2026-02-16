from elasticsearch import AsyncElasticsearch, helpers

from src.schema.obj import ObjList
from src.abstracts.db import AsyncAbstractLoader
from src.core.backoff import async_backoff as backoff


class Loader(AsyncAbstractLoader[AsyncElasticsearch]):
    @backoff()
    async def save_objs(self, index: str, objs: ObjList):
        if not self.client or not objs:
            return
        
        await helpers.async_bulk(
            client=self.client,
            actions=self.generate_data(objs, index)
        )
        
    def generate_data(self, data: ObjList, index: str):
        for item in data:
            yield {
                '_index': index,
                '_source': item
            }
    
    @backoff()
    async def start(self):
        self.client = AsyncElasticsearch(**self.config)
    
    @backoff()
    async def stop(self):
        if self.client:
            await self.client.close()
