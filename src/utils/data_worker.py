import asyncio
import logging

from src.schema.mapping import Map, IndexInfo
from src.crud.mapping import MappingCRUD
from src.abstracts.db import (
    AsyncAbstractExtractor, AsyncAbstractLoader, 
    AbstractExtractor, AbstractLoader
)
from src.abstracts.transform import AbstractTransform

class DataWorker:
    def __init__(
        self,
        index_name: str,
        index_info: IndexInfo,
        extractor: AsyncAbstractExtractor | AbstractExtractor,
        loader: AsyncAbstractLoader | AbstractLoader,
        transform: AbstractTransform,
        mapping_crud: MappingCRUD,
        logger: logging.Logger
    ):
        self.index_name = index_name
        self.index_info = index_info
        self.extractor = extractor
        self.loader = loader
        self.transform = transform
        self.mapping_crud = mapping_crud
        self.logger = logger
        
    async def process(self) -> bool:
        try:
            if asyncio.iscoroutinefunction(self.extractor.start):
                await self.extractor.start()
            else:
                self.extractor.start()
            
            if asyncio.iscoroutinefunction(self.loader.start):
                await self.loader.start()
            else:
                self.loader.start()
                
            if asyncio.iscoroutinefunction(self.extractor.get_objs):
                objs = await self.extractor.get_objs(self.index_name)
            else:
                objs = self.extractor.get_objs(self.index_name)
                
            print(objs)
                
            if not objs:
                self.logger.info(f"Нет данных для индекса {self.index_name}")
                return True
                
            transformed_objs = self.transform.transform(
                index_config=self.index_info,
                batch_data=objs  #type: ignore
            )
            print(f'{transformed_objs=}')
            
            if asyncio.iscoroutinefunction(self.loader.save_objs):
                await self.loader.save_objs(self.index_name, transformed_objs)
            else:
                self.loader.save_objs(self.index_name, transformed_objs)
                
            self.logger.info(f"Обработано {len(transformed_objs)} объектов для индекса {self.index_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка обработки индекса {self.index_name}: {e}")
            return False
        
        finally:
            try:
                if asyncio.iscoroutinefunction(self.extractor.stop):
                    await self.extractor.stop()
                else:
                    self.extractor.stop()  #type: ignore
                    
                if asyncio.iscoroutinefunction(self.loader.stop):
                    await self.loader.stop()
                else:
                    self.loader.stop()  #type: ignore
            except Exception as e:
                self.logger.error(f"Ошибка остановки клиентов для индекса {self.index_name}: {e}")
