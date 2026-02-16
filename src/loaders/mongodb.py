from pymongo import AsyncMongoClient

from src.schema.obj import ObjList
from src.abstracts.db import AsyncAbstractLoader
from src.core.backoff import async_backoff as backoff


class Loader(AsyncAbstractLoader[AsyncMongoClient]):
    def __init__(self, db_name: str, **kwargs):
        super().__init__(**kwargs)
        self.db_name = db_name
        
    @backoff()
    async def save_objs(self, index: str, objs: ObjList):
        if not self.client or not objs:
            return
        
        await self.client[self.db_name][index].insert_many(objs)
    
    @backoff()
    async def start(self):
        self.client = AsyncMongoClient(**self.config)
    
    @backoff()
    async def stop(self):
        if self.client:
            await self.client.aclose()
