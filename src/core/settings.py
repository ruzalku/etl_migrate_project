from pydantic_settings import BaseSettings, SettingsConfigDict
import json


class Settings(BaseSettings):
    raw_extractor_config: str
    raw_loader_config: str
    update_row: str | None = None

    extractor_config: dict = {}
    loader_config: dict = {}

    model_config = SettingsConfigDict(env_file='.env')

settings = Settings()  # type: ignore

settings.extractor_config = json.loads(
    settings.raw_extractor_config
)

settings.loader_config = json.loads(
    settings.raw_loader_config
)
