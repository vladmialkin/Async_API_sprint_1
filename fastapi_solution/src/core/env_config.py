from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(extra='ignore', env_file='.env')

    REDIS_HOST: str
    REDIS_PORT: int


class ElasticsearchSettings(BaseSettings):
    model_config = SettingsConfigDict(extra='allow', env_file='.env')

    ELASTIC_HOST: str
    ELASTIC_PORT: int


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra='allow', env_file='.env')

    project_name: str
