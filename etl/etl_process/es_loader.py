import os
import json
import logging
from typing import Optional

from .backoff import backoff
from .settings import ElasticsearchSettings

import elasticsearch
import elastic_transport
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class ElasticsearchLoader:
    """Загрузка данных в подготовленном формате в Elasticsearch."""
    def __init__(self):
        self.host = None
        self.port = None
        self.connection = None
        self.file_name = 'index.json'
        self.index_name = 'movies'
        self.logger = logging.getLogger('es')

        self.init_env()
        self.set_connection()
        self.create_index()

    @staticmethod
    def get_file_path(file_name: str, path: str) -> str:
        for root, dirs, files in os.walk(path):
            if file_name in files:
                return os.path.join(root, file_name)

    def init_env(self):
        settings = ElasticsearchSettings()

        self.host = settings.elastic_host
        self.port = settings.elastic_port

    @backoff()
    def make_es_connection(self):
        self.logger.info('Подключение к Elasticsearch...')

        try:
            connection = Elasticsearch(f'http://{self.host}:{self.port}')
            if not connection.ping():
                connection = None
            else:
                self.logger.info('Соединение с Elasticsearch установлено')
        except elastic_transport.ConnectionError as err:
            connection = None
            self.logger.exception(f"Ошибка подключения к Elasticsearch!!!\n{err}")

        return connection

    def set_connection(self):
        self.connection = self.make_es_connection()

    def get_index_schema(self, file) -> Optional[dict]:
        file_path = self.get_file_path(file, '.')

        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError as err:
            self.logger.exception(err)

    def create_index(self):
        self.logger.info(f'Creating an index {self.index_name}')

        mappings = self.get_index_schema(self.file_name)
        mappings_persons = self.get_index_schema('index_genres.json')

        if mappings is None:
            self.logger.warning('Index will be created without settings and mappings!')

        try:
            response = self.connection.indices.create(index=self.index_name, body=mappings)
            response = self.connection.indices.create(index='persons', body=mappings_persons)
            if response.body['acknowledged']:
                self.logger.info(f'Index {self.index_name} created')
        except elasticsearch.BadRequestError as err:
            self.logger.exception(err)

    def generate_data(self, data: list):
        for movie in data:
            yield {
                "_index": self.index_name,
                "_id": movie.id,
                "id": movie.id,
                "imdb_rating": movie.imdb_rating,
                "creation_date": movie.creation_date,
                "file_path": movie.file_path,
                "genres": movie.genres,
                "title": movie.title,
                "description": movie.description,
                "directors_names": movie.directors_names,
                "actors_names": movie.actors_names,
                "writers_names": movie.writers_names,
                "directors": movie.directors,
                "actors": movie.actors,
                "writers": movie.writers
            }

    def generate_persons(self, data: list):
        for person in data:
            yield {
                "_index": 'persons',
                "_id": person.id,
                "id": person.id,
                "full_name": person.full_name,
                "films": person.films
            }

    def index_documents(self, index_documents):
        self.logger.info('Indexing documents...')
        success, errors = None, None

        try:
            success, errors = bulk(self.connection, self.generate_data(index_documents))
        except elastic_transport.SerializationError as err:
            self.logger.exception(err)
        except elasticsearch.helpers.BulkIndexError as err:
            self.logger.exception(err)

        return success, errors

    def index_persons(self, index_documents):
        self.logger.info('Indexing documents...')
        success, errors = None, None

        try:
            success, errors = bulk(self.connection, self.generate_persons(index_documents))
        except elastic_transport.SerializationError as err:
            self.logger.exception(err)
        except elasticsearch.helpers.BulkIndexError as err:
            self.logger.exception(err)

        return success, errors
