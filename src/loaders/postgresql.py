from psycopg import AsyncConnection, sql

from src.schema.obj import ObjList
from src.abstracts.db import AsyncAbstractLoader
from src.core.backoff import backoff


class Loader(AsyncAbstractLoader[AsyncConnection]):
    def _get_columns(self, objs: ObjList) -> list[str]:
        if not objs:
            return []
        return sorted(objs[0].keys())

    @backoff()
    async def save_objs(self, index: str, objs: ObjList):
        if not objs or not self.client:
            return
        
        columns = self._get_columns(objs)
        if not columns:
            return
        
        table = sql.Identifier(index)
        col_idents = sql.SQL(', ').join(sql.Identifier(col) for col in columns)
        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(table, col_idents)
        
        async with self.client.cursor() as cur:
            async with cur.copy(copy_sql) as copy:
                for obj in objs:
                    row = tuple(obj.get(col) for col in columns)
                    await copy.write_row(row)
        await self.client.commit()
    
    @backoff()
    async def start(self):
        self.client = await AsyncConnection.connect(**self.config)
    
    @backoff()
    async def stop(self):
        if self.client:
            await self.client.close()
