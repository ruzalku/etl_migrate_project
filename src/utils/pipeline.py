import asyncio
import importlib
import logging
from typing import Dict, List, Optional, Any, cast

from src.schema.mapping import Map
from src.crud.mapping import MappingCRUD
from src.crud.json_state import JSONStateManager
from src.utils.data_worker import DataWorker


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
