import asyncio
import logging

from src.schema.enums import Mode
from src.utils.pipeline import PipelineOrchestrator
from src.core.settings import settings


async def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    orchestrator = PipelineOrchestrator(mapping_path="mapping.json", logger=logger)
    
    configs = {
        "extractor": {
            "config": settings.extractor_config,
            "update_row": settings.update_row,
            "pk_col": "ctid",
            "cdc": True,
            "cdc_mode": Mode.TIMESTAMP
        },
        "loader": {
            "config": settings.loader_config,
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
