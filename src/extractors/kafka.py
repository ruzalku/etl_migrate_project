import asyncio
import logging
import json

from aiokafka import AIOKafkaConsumer  # type: ignore

from schema.mapping import Map
from src.abstracts.db import AsyncAbstractExtractor
from src.schema.errors import UnsupportedMode
from src.schema.enums import Mode
from src.crud.json_state import JSONStateManager
from src.core.backoff import backoff


logger = logging.getLogger(__name__)


class Storage(AsyncAbstractExtractor[AIOKafkaConsumer]):
    def __init__(
        self,
        state_manager: JSONStateManager | None = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.topic: str | None = None
        self._iterator = None
        self.state_manager = state_manager
        
    async def get_mapping(self) -> Map:
        """Для kafka не нужен mapping"""
        logger.info('Пожалуйста, впишите в файл ваши названия топиков')
        return {}

    async def start(self):
        self._check_mode()
        if self.client is None:
            self.client = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.config.get(
                    'bootstrap_servers', 'localhost:16457'
                ),
                group_id=f'group_{self.topic}',
                auto_offset_reset='earliest',
                enable_auto_commit=False
            )
            await self.client.start()
            self._iterator = self.client.__aiter__()

    @backoff()
    async def get_data(self, batch_size: int, index: str):
        self.topic = index
        batch = []
        try:
            while len(batch) < batch_size and self._iterator:
                try:
                    msg = await asyncio.wait_for(
                        self._iterator.__anext__(), timeout=5.0
                    )
                    batch.append(json.loads(msg.value))  # type: ignore
                except asyncio.TimeoutError:
                    logger.debug(
                        f"Timeout для {self.topic}, batch size: {len(batch)}"
                    )
                    break
                except StopAsyncIteration:
                    break
        except Exception as e:
            logger.error(f'Ошибка чтения {self.topic}: {e}')
        logger.info(f"Получено {len(batch)} сообщений из {self.topic}")
        return batch

    def _check_mode(self):
        if not self.mode:
            return

        if self.mode == Mode.TIMESTAMP:
            raise UnsupportedMode('LOGS: Неподдерживаемый режим работы')

        if self.mode == Mode.LOGS:
            return

        raise UnsupportedMode(f'{self.mode}: Неподдерживаемый режим работы')

    async def commit(self):
        if self.client is not None:
            await self.client.commit()

    async def stop(self):
        if self.client:
            await self.client.stop()
