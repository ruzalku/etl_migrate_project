from typing import Any, Optional
from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row, DictRow
from datetime import datetime

from src.schema.mapping import Map, FieldInfo
from src.schema.obj import ObjList
from src.abstracts.db import AsyncAbstractExtractor
from src.crud.json_state import JSONStateManager
from src.schema.enums import Mode
from src.schema.errors import UnsupportedMode
from src.core.backoff import backoff


class Storage(AsyncAbstractExtractor[AsyncConnection]):
    def __init__(self, state_manager: JSONStateManager, **kwargs):
        super().__init__(**kwargs)
        self.pk_col = 'ctid'
        self.state_manager = state_manager
        self.client: Optional[AsyncConnection] = None

    @backoff()
    async def start(self):
        dsn = (
            f"dbname={self.config.get('dbname', 'postgres')} "
            f"user={self.config.get('user', 'postgres')} "
            f"password={self.config.get('password', '')} "
            f"host={self.config.get('host', 'localhost')} "
            f"port={self.config.get('port', 5432)}"
        )
        self.client = await AsyncConnection.connect(
            dsn,
            row_factory=dict_row  # type: ignore
        )

    @backoff()
    async def stop(self):
        if self.client:
            await self.client.close()

    @backoff()
    async def get_objs(
        self,
        index: str,
        batch_size: int = 500,
        last_state: Optional[Any] = None
    ) -> ObjList:
        if not self.client:
            return []

        query, params = self._create_cdc_query(index, last_state, batch_size)

        async with self.client.cursor() as cur:
            await cur.execute(query, params)
            data = await cur.fetchall()
            
            self._save_checkpoint(data=data, index=index)  #type: ignore
            return self._clean_data(data=data)  # type: ignore

    def _create_cdc_query(self, index: str, last_state: Any, batch_size: int):
        schema, table = index.split('.') if '.' in index else ('public', index)
        table_ident = sql.Identifier(schema, table)
        col = sql.Identifier(self.update_row)

        if not self.update_row:
            query = sql.SQL("SELECT * FROM {table} LIMIT %s").format(table=table_ident)
            return query, [batch_size]

        if self.mode != Mode.TIMESTAMP:
            raise UnsupportedMode(f'{self.mode}: Неподдерживаемый режим работы')

        if not last_state:
            query = sql.SQL("""
                SELECT *, ctid FROM {table} 
                ORDER BY {col}, ctid LIMIT %s
            """).format(table=table_ident, col=col)
            return query, [batch_size]

        ts, prev_ctid = last_state
        query = sql.SQL("""
            SELECT *, ctid FROM {table} 
            WHERE ({col} > %s) OR ({col} = %s AND ctid > %s)
            ORDER BY {col}, ctid LIMIT %s
        """).format(table=table_ident, col=col)

        return query, [ts, ts, prev_ctid, batch_size]
    
    def _save_checkpoint(self, data: list[DictRow], index: str):
        if self.state_manager and self.cdc and data and self.update_row:
            last_row = data[-1]

            cp = (
                last_row[self.update_row],
                last_row['ctid']
            )

            self.state_manager.set_state(f'pg_{index}', cp)
            
    def _clean_data(self, data: list[DictRow]):
        clean_data = []
        for row in data:
            row_copy = dict(row)
            row_copy.pop('ctid', None)
            clean_data.append(row_copy)
            
        return clean_data