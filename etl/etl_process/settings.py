import os

from pydantic_settings import BaseSettings, SettingsConfigDict


def detect_env_file(file_name: str, path: str) -> str:
    for root, dirs, files in os.walk(path):
        if file_name in files:
            return os.path.join(root, file_name)


env_path = detect_env_file('.env', '..')


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(extra='ignore', env_file=env_path)

    db_name: str
    db_user: str
    db_password: str
    db_host: str
    db_port: int


class ElasticsearchSettings(BaseSettings):
    model_config = SettingsConfigDict(extra='allow', env_file=env_path)

    elastic_host: str
    elastic_port: int
