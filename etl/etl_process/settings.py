from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(extra='ignore', env_file='.env')

    db_name: str
    db_user: str
    db_password: str
    db_host: str
    db_port: int


class ElasticsearchSettings(BaseSettings):
    model_config = SettingsConfigDict(extra='allow', env_file='.env')

    elastic_host: str
    elastic_port: int
