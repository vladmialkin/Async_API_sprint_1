import os
from logging import config as logging_config

from .logger import LOGGING
from .env_config import Settings, RedisSettings, ElasticsearchSettings
logging_config.dictConfig(LOGGING)

settings = Settings()
PROJECT_NAME = settings.project_name

redis_settings = RedisSettings()
REDIS_HOST = redis_settings.redis_host
REDIS_PORT = redis_settings.redis_port

es_settings = ElasticsearchSettings()
ELASTIC_HOST = es_settings.es_host
ELASTIC_PORT = es_settings.es_port

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
