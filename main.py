import asyncio
import importlib
import logging
from typing import Dict, List, Optional, Any, cast

from src.schema.mapping import Map, IndexInfo
from src.crud.mapping import MappingCRUD
from src.abstracts.db import (
    AsyncAbstractExtractor, AsyncAbstractLoader, 
    AbstractExtractor, AbstractLoader
)
from src.abstracts.transform import AbstractTransform
from src.crud.json_state import JSONStateManager
from src.schema.enums import Mode

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

class PipelineOrchestrator:
    def __init__(self, mapping_path: str, logger: logging.Logger):
        self.mapping_path = mapping_path
        self.mapping_crud = None
        self.logger = logger
        
    async def _load_mapping(self) -> Optional[Map]:
        if not self.mapping_crud:
            self.logger.error("MappingCRUD не инициализирован")
            return None

        try:
            map_dict = await self.mapping_crud.get_obj("")
            self.logger.info(f"Загружен mapping: {map_dict.keys() if map_dict else 'пустой'}")
        except Exception as e:
            self.logger.error(f"Ошибка загрузки mapping: {e}")
            return None

        if not map_dict:
            self.logger.error("Mapping пустой")
            return None

        return cast(Map, map_dict)
    
    def _create_instances(
        self,
        module_path: str,
        class_name: str,
        section_config: dict,
        **extra_kwargs
    ) -> Any:
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        
        db_config = section_config.get("config", section_config)
        class_kwargs = {k: v for k, v in section_config.items() if k != "config"}
        
        return cls(config=db_config, **class_kwargs, **extra_kwargs)
    
    async def run_pipeline(
        self,
        loader_module: str,
        extractor_module: str,
        transform_module: str,
        configs: Dict[str, dict],
        mapping_path: str
    ) -> List[bool]:
        self.mapping_crud = MappingCRUD(mapping_path)
        mapping: Map = await self._load_mapping()  #type: ignore
        if not mapping:
            self.logger.error("Не удалось загрузить mapping")
            return []

        tasks = []
        
        for index_name, index_info in mapping.items():
            extractor = self._create_instances(
                extractor_module, 
                "Storage",
                configs["extractor"],
                state_manager=JSONStateManager(f"state_{index_name}.json")
            )

            loader = self._create_instances(
                loader_module,
                "Loader", 
                configs["loader"]
            )

            transform = self._create_instances(
                transform_module,
                "DataTransformer",
                configs["transform"]
            )

            worker = DataWorker(
                index_name=index_name,
                index_info=index_info,
                extractor=extractor,
                loader=loader,
                transform=transform,
                mapping_crud=self.mapping_crud,
                logger=self.logger
            )
            tasks.append(worker.process())
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if isinstance(r, bool) and r)
        self.logger.info(f"Пайплайн завершен. Успех: {success_count}/{len(tasks)}")
        return [bool(r) if isinstance(r, bool) else False for r in results]

async def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    orchestrator = PipelineOrchestrator(mapping_path="mapping.json", logger=logger)
    
    configs = {
        "extractor": {
            "config": {
                "host": "localhost",
                "port": 5432,
                "dbname": "test_db", 
                "user": "postgres",
                "password": "postgres",
            },
            "update_row": "created_at",
            "pk_col": "ctid",
            "cdc": True,
            "cdc_mode": Mode.TIMESTAMP
        },
        "loader": {
            "config": {
                "host": "127.0.0.1",
                "port": 27017,
            },
            "db_name": "testdb"
        },
        "transform": {
            "config": {}
        }
    }
    
    await orchestrator.run_pipeline(
        loader_module="src.loaders.mongodb",
        extractor_module="src.extractors.postgresql", 
        transform_module="src.transaformers.default",
        configs=configs,
        mapping_path="mapping.json"
    )

if __name__ == '__main__':
    asyncio.run(main())
