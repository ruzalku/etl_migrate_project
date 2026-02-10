from typing import Any, Optional
from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row

from src.schema.mapping import Map
from src.schema.obj import ObjList
from src.abstracts.db import AsyncAbstractExtractor
from src.crud.json_state import JSONStateManager
from src.schema.enums import Mode
from src.schema.errors import UnsupportedMode


class Storage(AsyncAbstractExtractor[AsyncConnection]):
    def __init__(self, state_manager: JSONStateManager, **kwargs):
        super().__init__(**kwargs)
        self.state_manager = state_manager
        self.client: Optional[AsyncConnection] = None

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

    async def stop(self):
        if self.client:
            await self.client.close()

    async def get_mapping(self) -> Map:
        if not self.client:
            return {}

        tables = await self._get_tables_by_owner()
        if not tables:
            return {}

        query = """
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE (table_schema, table_name) IN ({})
        """

        table_tuples = [tuple(t.split('.')) for t in tables]
        placeholders = sql.SQL(', ').join(
            [sql.Placeholder()] * len(table_tuples)
        )

        final_query = sql.SQL(query).format(placeholders)

        async with self.client.cursor() as cur:
            await cur.execute(final_query, table_tuples)
            response = await cur.fetchall()

        return self._from_respose_to_map(response)  # type: ignore

    async def _get_tables_by_owner(self) -> list[str]:
        query = """
            SELECT n.nspname, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_roles r ON c.relowner = r.oid
            WHERE c.relkind = 'r'
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND r.rolname = %s;
        """
        if not self.client:
            return []

        async with self.client.cursor() as cur:
            user = self.config.get('user', 'postgres')
            await cur.execute(query, (user,))
            rows = await cur.fetchall()

        return [f"{row['nspname']}.{row['relname']}" for row in rows]  # type: ignore

    def _from_respose_to_map(self, rows: list[dict]) -> Map:
        result_map: Map = {}
        for row in rows:
            full_name = f"{row['table_schema']}.{row['table_name']}"

            if full_name not in result_map:
                result_map[full_name] = {
                    'new_table_name': row['table_name'],
                    'fields': {}
                }

            result_map[full_name]['fields'][row['column_name']] = {
                'data_type': row['data_type'],
                'constraint_type': None,
                'new_column_name': row['column_name']
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

        async with self.client.cursor() as cur:
            await cur.execute(query, params)
            data = await cur.fetchall()
            if self.state_manager:
                self.state_manager.set_state(f'pg_{index}', data[-1][self.update_row])  #type: ignore
            return data  # type: ignore

    def _create_cdc_query(self, index: str, last_state: Any, batch_size: int):
        schema, table = index.split('.') if '.' in index else ('public', index)
        table_ident = sql.Identifier(schema, table)

        if not self.update_row:
            query = sql.SQL(
                "SELECT * FROM {table} LIMIT %s"
            ).format(table=table_ident)

            return query, [batch_size]

        if self.mode == Mode.TIMESTAMP:
            if not last_state:
                query = sql.SQL(
                    "SELECT * FROM {table} ORDER BY {col} LIMIT %s"
                ).format(
                    table=table_ident, col=sql.Identifier(self.update_row)
                )

                return query, [batch_size]

            query = sql.SQL(
                "SELECT * FROM {table} WHERE {col} > %s ORDER BY {col} LIMIT %s"
            ).format(
                table=table_ident, col=sql.Identifier(self.update_row)
            )

            return query, [last_state, batch_size]

        if self.mode == Mode.LOGS:
            raise UnsupportedMode(
                'LOGS: Неподдерживаемый режим работы'
            )

        raise UnsupportedMode(
            f'{self.mode}: Неподдерживаемый режим работы'
        )
