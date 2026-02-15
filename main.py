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
from src.utils.pipeline import PipelineOrchestrator


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
