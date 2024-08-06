import os
from logging import config as logging_config

from .env_config import ElasticsearchSettings, RedisSettings, Settings
from .logger import LOGGING

logging_config.dictConfig(LOGGING)

settings = Settings()
PROJECT_NAME = settings.project_name

redis_settings = RedisSettings()
REDIS_HOST = redis_settings.REDIS_HOST
REDIS_PORT = redis_settings.REDIS_PORT

es_settings = ElasticsearchSettings()
ELASTIC_HOST = es_settings.ELASTIC_HOST
ELASTIC_PORT = es_settings.ELASTIC_PORT

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
