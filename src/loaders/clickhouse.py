from clickhouse_connect import get_async_client
from clickhouse_connect.driver import AsyncClient

from src.schema.obj import ObjList
from src.abstracts.db import AsyncAbstractLoader
from src.core.backoff import backoff


class Loader(AsyncAbstractLoader[AsyncClient]):
    @backoff()
    async def save_objs(self, index: str, objs: ObjList):
        if not self.client or not objs:
            return
        await self.client.insert(
            table=index,
            data=[tuple(i.values()) for i in objs],
            column_names=tuple(objs[0].keys())
        )
    
    @backoff()
    async def start(self):
        self.client = await get_async_client(**self.config)
    
    @backoff()
    async def stop(self):
        if self.client:
            await self.client.close()
