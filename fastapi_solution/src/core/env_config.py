from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(extra='ignore', env_file='.env')

    redis_host: str
    redis_port: int


class ElasticsearchSettings(BaseSettings):
    model_config = SettingsConfigDict(extra='allow', env_file='.env')

    es_host: str
    es_port: int


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra='allow', env_file='.env')

    project_name: str
