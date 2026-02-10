from aiosqlite import Row, Connection, connect
from typing import Any, Optional

from src.schema.mapping import Map
from src.schema.obj import ObjList
from src.abstracts.db import AsyncAbstractExtractor
from src.crud.json_state import JSONStateManager
from src.schema.enums import Mode
from src.schema.errors import UnsupportedMode


class SQLiteStorage(AsyncAbstractExtractor[Connection]):
    def __init__(self, state_manager: JSONStateManager, **kwargs):
        super().__init__(**kwargs)
        self.state_manager = state_manager
        self.client: Optional[Connection] = None

    async def start(self):
        db_path = self.config.get('database', 'database.db')
        self.client = await connect(db_path)
        self.client.row_factory = Row

    async def stop(self):
        if self.client:
            await self.client.close()

    async def get_mapping(self) -> Map:
        if not self.client:
            return {}

        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"

        async with self.client.execute(query) as cursor:
            tables = await cursor.fetchall()

        result_map: Map = {}
        for table_row in tables:
            table_name = table_row['name']
            result_map[table_name] = {
                'new_table_name': table_name,
                'fields': {}
            }

            async with self.client.execute(f"PRAGMA table_info({table_name})") as cursor:
                columns = await cursor.fetchall()
                for col in columns:
                    result_map[table_name]['fields'][col['name']] = {
                        'data_type': col['type'],
                        'constraint_type': 'PK' if col['pk'] else None,
                        'new_column_name': col['name']
                    }

        return result_map

    async def get_objs(
        self,
        index: str,
        batch_size: int = 500,
        last_state: Optional[Any] = None
    ) -> ObjList:
        if not self.client:
            return []

        query, params = self._create_cdc_query(index, last_state, batch_size)

        async with self.client.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            data = [dict(row) for row in rows]

        if data and self.update_row:
            self.state_manager.set_state(f'sqlite_{index}', data[-1][self.update_row])

        return data  # type: ignore

    def _create_cdc_query(self, index: str, last_state: Any, batch_size: int):
        table_name = f'"{index}"'

        if not self.update_row:
            return f"SELECT * FROM {table_name} LIMIT ?", [batch_size]

        if self.mode == Mode.TIMESTAMP:
            if not last_state:
                query = f"SELECT * FROM {table_name} ORDER BY {self.update_row} LIMIT ?"
                return query, [batch_size]

            query = f"""
                SELECT * FROM {table_name}
                WHERE {self.update_row} > ?
                ORDER BY {self.update_row}
                LIMIT ?
            """
            return query, [last_state, batch_size]

        if self.mode == Mode.LOGS:
            raise UnsupportedMode(
                'LOGS: Неподдерживаемый режим работы'
            )

        raise UnsupportedMode(
            f'{self.mode}: Неподдерживаемый режим работы'
        )
